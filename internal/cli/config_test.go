/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli"
)

// configHome points the configuration file at a temporary directory and returns
// the path it will live at.
//
// t.Setenv rather than a path parameter, because the location *is* part of the
// contract: ${XDG_CONFIG_HOME:-~/.config}/kuberecord/config.yaml is documented, and
// a test that injected a path would leave the documented location asserted by
// nothing.
func configHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", home)
	return filepath.Join(home, "kuberecord", "config.yaml")
}

// writeConfigFile puts a hand-written configuration where the CLI will find it.
func writeConfigFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("creating the configuration directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing the configuration file: %v", err)
	}
}

// TestDefaultConfigPathFollowsXDG pins the documented location, in both of the two
// states an engineer's machine is in.
func TestDefaultConfigPathFollowsXDG(t *testing.T) {
	t.Run("XDG_CONFIG_HOME set", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "/somewhere/config")
		path, err := cli.DefaultConfigPath()
		if err != nil {
			t.Fatalf("DefaultConfigPath: %v", err)
		}
		if want := filepath.Join("/somewhere/config", "kuberecord", "config.yaml"); path != want {
			t.Errorf("DefaultConfigPath() = %q, want %q", path, want)
		}
	})

	t.Run("XDG_CONFIG_HOME unset falls back to ~/.config", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "")
		home, err := os.UserHomeDir()
		if err != nil {
			t.Skipf("no home directory on this machine: %v", err)
		}
		path, err := cli.DefaultConfigPath()
		if err != nil {
			t.Fatalf("DefaultConfigPath: %v", err)
		}
		if want := filepath.Join(home, ".config", "kuberecord", "config.yaml"); path != want {
			t.Errorf("DefaultConfigPath() = %q, want %q", path, want)
		}
	})
}

// TestAMissingConfigFileIsNotAFailure.
//
// It is the state of every first invocation and of every user whose cluster has a
// sink CR to discover. Reporting it would tell those users to create a file they
// do not need, which is the ceremony this whole resolution chain exists to avoid.
func TestAMissingConfigFileIsNotAFailure(t *testing.T) {
	path := configHome(t)

	cfg, err := cli.LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig of a missing file: %v", err)
	}
	if cfg == nil {
		t.Fatal("LoadConfig returned no configuration and no error")
	}
	if len(cfg.Profiles) != 0 || cfg.CurrentProfile != "" {
		t.Errorf("a missing file produced %+v, want an empty configuration", cfg)
	}
}

// TestSaveConfigWritesAPrivateFile.
//
// 0600 is not protecting a credential — the file cannot hold one. It is protecting
// the pointer to one: the variable name, the file path, the server and the user
// together describe how to obtain the password, and that is worth as little
// exposure as a file can be given for free.
func TestSaveConfigWritesAPrivateFile(t *testing.T) {
	path := configHome(t)

	cfg := &cli.Config{
		CurrentProfile: "prod",
		Profiles: map[string]cli.Profile{
			"prod": {
				Backend: cli.BackendClickHouse,
				ClickHouse: &cli.ClickHouseProfile{
					Addr: "ch.example:9000", Database: "kuberecord",
					Username: "kuberecord_ro", PasswordEnv: "KUBERECORD_CH_PASSWORD",
				},
			},
		},
		Contexts: map[string]string{"prod-eu": theCluster},
	}
	if err := cli.SaveConfig(path, cfg); err != nil {
		t.Fatalf("SaveConfig: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat the written file: %v", err)
	}
	if mode := info.Mode().Perm(); mode != cli.ConfigFileMode {
		t.Errorf("the configuration file is %v, want %v", mode, os.FileMode(cli.ConfigFileMode))
	}
	dirInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("stat the configuration directory: %v", err)
	}
	if mode := dirInfo.Mode().Perm(); mode != cli.ConfigDirMode {
		t.Errorf("the configuration directory is %v, want %v", mode, os.FileMode(cli.ConfigDirMode))
	}

	// Nothing but the file is left behind: an interrupted write must leave the old
	// configuration and no debris beside it.
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatalf("read the configuration directory: %v", err)
	}
	if len(entries) != 1 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Errorf("the configuration directory holds %v, want the file alone", names)
	}

	reloaded, err := cli.LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig of what SaveConfig wrote: %v", err)
	}
	if reloaded.APIVersion != cli.ConfigAPIVersion || reloaded.Kind != cli.ConfigKind {
		t.Errorf("the written file is %s/%s, want %s/%s",
			reloaded.APIVersion, reloaded.Kind, cli.ConfigAPIVersion, cli.ConfigKind)
	}
	if reloaded.Contexts["prod-eu"] != theCluster {
		t.Errorf("the context mapping did not survive the round trip: %v", reloaded.Contexts)
	}
	if profile := reloaded.Profiles["prod"]; profile.ClickHouse == nil ||
		profile.ClickHouse.PasswordEnv != "KUBERECORD_CH_PASSWORD" {
		t.Errorf("the profile did not survive the round trip: %+v", profile)
	}
}

// TestTheFileIsRefusedWhenItSaysSomethingImpossible.
//
// The inline-password case is the one the acceptance criteria name, and it is the
// reason the field is declared at all: without it the parser would reject the key
// as unknown, and "unknown field password" teaches nobody where a password is
// supposed to go. The others are here because a configuration silently ignored in
// part is how somebody ends up reading the wrong cluster's history and believing
// the tool agreed with them.
func TestTheFileIsRefusedWhenItSaysSomethingImpossible(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    []string
	}{
		{
			name: "an inline ClickHouse password",
			content: `profiles:
  prod:
    backend: clickhouse
    clickhouse:
      addr: ch.example:9000
      password: hunter2
`,
			want: []string{"passwordEnv", "passwordFile"},
		},
		{
			name: "both password references at once",
			content: `profiles:
  prod:
    backend: clickhouse
    clickhouse:
      addr: ch.example:9000
      passwordEnv: A
      passwordFile: /b
`,
			want: []string{"passwordEnv", "passwordFile"},
		},
		{
			name: "an inline S3 secret key",
			content: `profiles:
  archive:
    backend: s3
    s3:
      bucket: acme-audit
      secretAccessKey: wJalrXU
`,
			want: []string{"AWS credential chain", "secretAccessKey"},
		},
		{
			// A key JSON's own case-insensitive matching cannot rescue: `passwordEnv`
			// spelled in any case is still that field, and it is `passwordVar` — the
			// name a user might reasonably guess — that has to be refused rather than
			// silently doing nothing.
			name: "a field that does not exist",
			content: `profiles:
  prod:
    backend: clickhouse
    clickhouse:
      addr: ch.example:9000
      passwordVar: KUBERECORD_CH_PASSWORD
`,
			want: []string{"passwordVar"},
		},
		{
			name: "a stanza that does not match the backend",
			content: `profiles:
  prod:
    backend: clickhouse
    s3:
      bucket: acme-audit
`,
			want: []string{"clickhouse", "s3"},
		},
		{
			name: "an unknown backend",
			content: `profiles:
  prod:
    backend: postgres
`,
			want: []string{"postgres", "clickhouse, s3, local"},
		},
		{
			name: "a current profile that does not exist",
			content: `currentProfile: staging
profiles:
  prod:
    backend: local
    local:
      path: /archives
`,
			want: []string{"staging", "prod"},
		},
		{
			name:    "an apiVersion from another release",
			content: "apiVersion: cli.kuberecord.io/v1beta9\n",
			want:    []string{"v1beta9", cli.ConfigAPIVersion},
		},
		{
			name: "a prefix with a slash on it",
			content: `profiles:
  archive:
    backend: s3
    s3:
      bucket: acme-audit
      prefix: /kuberecord/
`,
			want: []string{"prefix", "slash"},
		},
		{
			name: "an endpoint with no scheme",
			content: `profiles:
  archive:
    backend: s3
    s3:
      bucket: acme-audit
      endpoint: minio.internal:9000
`,
			want: []string{"scheme", "http://minio.internal:9000"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := configHome(t)
			writeConfigFile(t, path, tc.content)

			_, err := cli.LoadConfig(path)
			if err == nil {
				t.Fatal("LoadConfig accepted a configuration it should have refused")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the failure does not mention %q, so it does not say what to do:\n%v",
						want, err)
				}
			}
		})
	}
}

// TestResolvePasswordReadsTheReferenceAndNotAValue.
//
// The unset-variable case is why the indirection is worth having at all. A profile
// naming a variable the shell never exported must say so here — not authenticate
// with an empty password and hand the user whatever the server says about that,
// three steps from the cause.
func TestResolvePasswordReadsTheReferenceAndNotAValue(t *testing.T) {
	t.Run("from the environment", func(t *testing.T) {
		t.Setenv("KUBERECORD_TEST_PASSWORD", "hunter2")
		profile := &cli.ClickHouseProfile{PasswordEnv: "KUBERECORD_TEST_PASSWORD"}

		password, err := profile.ResolvePassword()
		if err != nil {
			t.Fatalf("ResolvePassword: %v", err)
		}
		if password != "hunter2" {
			t.Errorf("ResolvePassword() = %q, want the variable's value", password)
		}
	})

	t.Run("an unset variable is a failure, not an empty password", func(t *testing.T) {
		profile := &cli.ClickHouseProfile{PasswordEnv: "KUBERECORD_TEST_UNSET_PASSWORD"}

		_, err := profile.ResolvePassword()
		if err == nil {
			t.Fatal("an unset variable resolved to a password")
		}
		if !strings.Contains(err.Error(), "KUBERECORD_TEST_UNSET_PASSWORD") {
			t.Errorf("the failure does not name the variable: %v", err)
		}
	})

	t.Run("a variable set to empty is an empty password", func(t *testing.T) {
		t.Setenv("KUBERECORD_TEST_EMPTY_PASSWORD", "")
		profile := &cli.ClickHouseProfile{PasswordEnv: "KUBERECORD_TEST_EMPTY_PASSWORD"}

		password, err := profile.ResolvePassword()
		if err != nil {
			t.Fatalf("ResolvePassword: %v", err)
		}
		if password != "" {
			t.Errorf("ResolvePassword() = %q, want the empty password that was configured", password)
		}
	})

	t.Run("from a file, with the newline echo leaves behind", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "password")
		if err := os.WriteFile(file, []byte("hunter2\n"), 0o600); err != nil {
			t.Fatalf("writing the password file: %v", err)
		}
		profile := &cli.ClickHouseProfile{PasswordFile: file}

		password, err := profile.ResolvePassword()
		if err != nil {
			t.Fatalf("ResolvePassword: %v", err)
		}
		if password != "hunter2" {
			t.Errorf("ResolvePassword() = %q, want the trailing newline trimmed", password)
		}
	})

	t.Run("a missing file is a failure", func(t *testing.T) {
		profile := &cli.ClickHouseProfile{PasswordFile: filepath.Join(t.TempDir(), "absent")}

		if _, err := profile.ResolvePassword(); err == nil {
			t.Fatal("a missing password file resolved to a password")
		}
	})

	t.Run("no reference is no password", func(t *testing.T) {
		password, err := (&cli.ClickHouseProfile{}).ResolvePassword()
		if err != nil {
			t.Fatalf("ResolvePassword with nothing configured: %v", err)
		}
		if password != "" {
			t.Errorf("ResolvePassword() = %q, want empty", password)
		}
	})
}
