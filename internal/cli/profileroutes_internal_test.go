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

package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
)

// The wiring half of Task 17.4, tested where the wiring is.
//
// The routes themselves and the classification are asserted in
// internal/cli/resolve, which is where they are decided. What only this package
// can see is that a failure raised inside resolution reaches the top of the CLI
// still carrying them — the block is rendered by RunContext through an interface
// rather than by a branch per error type, and an error that satisfied it at
// compile time and not at run time would go quiet rather than fail.

// failingProfileResolver builds a resolver whose active profile names an
// environment variable that is not set.
//
// It contacts nothing: a profile is step 3 and answers from the file alone, which
// is the whole reason profiles exist for engineers who cannot read the operator's
// Secrets.
func failingProfileResolver(t *testing.T) (*resolve.BackendResolver, genericiooptions.IOStreams) {
	t.Helper()

	// Set and then removed, so t.Setenv's cleanup restores whatever the machine
	// running this actually has. A developer with the variable exported would
	// otherwise resolve the profile and test nothing.
	t.Setenv(resolve.DefaultPasswordEnv, "")
	if err := os.Unsetenv(resolve.DefaultPasswordEnv); err != nil {
		t.Fatalf("unsetting %s: %v", resolve.DefaultPasswordEnv, err)
	}

	streams := genericiooptions.IOStreams{
		In: strings.NewReader(""), Out: &strings.Builder{}, ErrOut: &strings.Builder{},
	}
	root, flags := NewRootCommand(options.StandaloneName, streams)
	if err := root.ParseFlags([]string{
		"--kubeconfig", filepath.Join("testdata", "kubeconfig"),
		"--" + options.FlagClusterID, diagnoseCluster,
	}); err != nil {
		t.Fatalf("parsing flags: %v", err)
	}

	return &resolve.BackendResolver{
		Flags: flags, Streams: streams, InvokedAs: options.StandaloneName,
		ConfigPath: filepath.Join(t.TempDir(), "config.yaml"),
		Config: &resolve.Config{
			CurrentProfile: "prod",
			Profiles: map[string]resolve.Profile{
				"prod": {
					Backend: resolve.BackendClickHouse,
					ClickHouse: &resolve.ClickHouseProfile{
						Addr: "clickhouse.example:9000", Database: resolve.DefaultClickHouseDatabase,
						Username: "kuberecord_ro", PasswordEnv: resolve.DefaultPasswordEnv,
					},
				},
				"archive": {Backend: resolve.BackendLocal, Local: &resolve.LocalProfile{Path: "/archives"}},
			},
		},
	}, streams
}

// TestAFailingProfileArrivesAtTheTopWithItsRoutes.
func TestAFailingProfileArrivesAtTheTopWithItsRoutes(t *testing.T) {
	resolver, streams := failingProfileResolver(t)

	backend, err := resolver.Resolve(t.Context())
	if err == nil {
		if closeErr := backend.Close(); closeErr != nil {
			t.Errorf("closing the backend: %v", closeErr)
		}
		t.Fatal("a profile whose password reference does not resolve produced a backend")
	}

	root, flags := NewRootCommand(options.StandaloneName, streams)
	if parseErr := root.ParseFlags([]string{
		"--" + options.FlagColor, string(options.ColorNever),
	}); parseErr != nil {
		t.Fatalf("parsing flags: %v", parseErr)
	}

	advice := remediationAdvice(err, root, flags, streams)
	for _, want := range []string{
		"export " + resolve.DefaultPasswordEnv + "=…",
		"--" + options.FlagSink + " ClickHouseSink/<name>",
		"config use-profile archive",
		"config resolve",
	} {
		if !strings.Contains(advice, want) {
			t.Errorf("the advice does not carry %q:\n%s", want, advice)
		}
	}

	// The one-line failure is unchanged, and so is the code a wrapper script
	// reads: this is a configuration mistake, not something to retry.
	if !strings.Contains(err.Error(), resolve.DefaultPasswordEnv) {
		t.Errorf("the failure no longer names the variable: %v", err)
	}
	if code := exit.CodeFor(err); code != exit.RuntimeError {
		t.Errorf("a failing profile exits %d, want %d", code, exit.RuntimeError)
	}

	// Nothing reached stdout. The block is a diagnostic, and RunContext writes it
	// into the same stderr write as `error:` — on stdout it would corrupt a
	// `| jq` (Invariant 4).
	if produced := streams.Out.(*strings.Builder).String(); produced != "" {
		t.Errorf("a failing invocation wrote to stdout, which belongs to data: %q", produced)
	}
}

// TestTheProfileAdviceObeysTheColourMode, for the same reason the unreachable
// block's does: the decision belongs to --color and to whether stderr is a
// terminal, and this layer is the only one that knows both.
func TestTheProfileAdviceObeysTheColourMode(t *testing.T) {
	resolver, streams := failingProfileResolver(t)

	backend, err := resolver.Resolve(t.Context())
	if err == nil {
		if closeErr := backend.Close(); closeErr != nil {
			t.Errorf("closing the backend: %v", closeErr)
		}
		t.Fatal("a profile whose password reference does not resolve produced a backend")
	}

	for _, tc := range []struct {
		mode    options.ColorMode
		painted bool
	}{
		{options.ColorNever, false},
		{options.ColorAlways, true},
		{options.ColorAuto, false},
	} {
		t.Run(string(tc.mode), func(t *testing.T) {
			root, flags := NewRootCommand(options.StandaloneName, streams)
			if parseErr := root.ParseFlags([]string{
				"--" + options.FlagColor, string(tc.mode),
			}); parseErr != nil {
				t.Fatalf("parsing flags: %v", parseErr)
			}
			advice := remediationAdvice(err, root, flags, streams)
			if painted := strings.Contains(advice, "\x1b["); painted != tc.painted {
				t.Errorf("--%s=%s produced painted=%v, want %v",
					options.FlagColor, tc.mode, painted, tc.painted)
			}
		})
	}
}
