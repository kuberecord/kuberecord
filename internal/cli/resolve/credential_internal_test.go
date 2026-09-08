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

package resolve

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The two facts about a profile that a configuration file cannot state: the
// locator its fields add up to, and whether its credential reference resolves on
// the machine reading it.
//
// Both are here rather than in describe_internal_test.go because both are new
// surface with their own properties, and the second is the one worth guarding: it
// touches the environment and the filesystem, and it is the input to a column
// somebody will read as "can I query this right now".

// credentialEnv is the variable these cases name.
//
// A name of its own rather than DefaultPasswordEnv, because several of them need
// the variable to be *absent* and the default is one a developer running these
// tests may well have exported in their own shell. t.Setenv restores whatever was
// there, so setting it to a fixture value is safe; asserting on the absence of a
// name somebody else may own is not.
const credentialEnv = "KUBERECORD_TEST_CREDENTIAL_ENV"

// TestProfileTargetIsTheLocatorWithoutThePlainWords.
//
// `config get-profiles` prints BACKEND and TARGET in adjacent columns, so the
// prose half of a description would be the backend named twice on every row.
// What the column needs is the locator, with the same defaults applied that
// Describe applies — the address a query would open rather than the fields as
// typed.
func TestProfileTargetIsTheLocatorWithoutThePlainWords(t *testing.T) {
	tests := []struct {
		name    string
		profile Profile
		want    string
	}{
		{
			name: "clickhouse",
			profile: Profile{Backend: BackendClickHouse, ClickHouse: &ClickHouseProfile{
				Addr: "10.0.1.5:9000", Database: "audit",
			}},
			want: "10.0.1.5:9000/audit",
		},
		{
			name: "clickhouse with no database named",
			profile: Profile{Backend: BackendClickHouse, ClickHouse: &ClickHouseProfile{
				Addr: "10.0.1.5:9000",
			}},
			want: "10.0.1.5:9000/" + DefaultClickHouseDatabase,
		},
		{
			name: "s3 with a prefix",
			profile: Profile{Backend: BackendS3, S3: &S3Profile{
				Bucket: "acme-audit", Prefix: "kuberecord", Region: "eu-west-1",
			}},
			want: "s3://acme-audit/kuberecord",
		},
		{
			// The region is in Describe and not here on purpose: it is not part of
			// the locator, and a bucket name is global — a wrong region cannot
			// resolve to somebody else's bucket, it fails loudly.
			name:    "s3 with no prefix",
			profile: Profile{Backend: BackendS3, S3: &S3Profile{Bucket: "acme-audit"}},
			want:    "s3://acme-audit",
		},
		{
			name:    "local",
			profile: Profile{Backend: BackendLocal, Local: &LocalProfile{Path: "/archives/kuberecord"}},
			want:    "/archives/kuberecord",
		},
		{
			// Reachable only from a struct assembled in memory, since LoadConfig
			// refuses a profile whose stanza is missing. The fallback is Describe's,
			// because a crash inside a table about a misassembled profile would
			// replace the listing with a stack trace.
			name:    "a backend with no stanza falls back to its name",
			profile: Profile{Backend: BackendS3},
			want:    "s3",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.profile.Target(); got != test.want {
				t.Errorf("Target() = %q, want %q", got, test.want)
			}
			// The locator and the description are one implementation, so the
			// second must contain the first. Two renderings of one address is how
			// a reader ends up wondering whether they are two addresses.
			if description := test.profile.Describe(); !strings.Contains(description, test.want) {
				t.Errorf("Describe() = %q, which does not carry the locator %q",
					description, test.want)
			}
		})
	}
}

// TestProfileCredentialReportsTheReferenceAndWhetherItResolves.
//
// The whole of `config get-profiles`'s reason to exist is this method: a
// configuration file cannot say whether the variable it names is exported in the
// shell that is running or whether the file it names is on this disk, and those
// are the two states that turn into a resolution failure at the moment somebody
// runs a query.
func TestProfileCredentialReportsTheReferenceAndWhetherItResolves(t *testing.T) {
	present := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(present, []byte(plantedPassword), ConfigFileMode); err != nil {
		t.Fatalf("writing the password file: %v", err)
	}
	absent := filepath.Join(t.TempDir(), "no-such-password")

	tests := []struct {
		name    string
		profile Profile
		export  bool
		want    Credential
	}{
		{
			name: "an environment variable this shell exported",
			profile: Profile{Backend: BackendClickHouse, ClickHouse: &ClickHouseProfile{
				Addr: "10.0.1.5:9000", PasswordEnv: credentialEnv,
			}},
			export: true,
			want:   Credential{Source: CredentialEnv, Reference: credentialEnv, State: CredentialSet},
		},
		{
			name: "an environment variable it did not",
			profile: Profile{Backend: BackendClickHouse, ClickHouse: &ClickHouseProfile{
				Addr: "10.0.1.5:9000", PasswordEnv: credentialEnv,
			}},
			want: Credential{Source: CredentialEnv, Reference: credentialEnv, State: CredentialNotSet},
		},
		{
			name: "a password file that is there",
			profile: Profile{Backend: BackendClickHouse, ClickHouse: &ClickHouseProfile{
				Addr: "10.0.1.5:9000", PasswordFile: present,
			}},
			want: Credential{Source: CredentialFile, Reference: present, State: CredentialPresent},
		},
		{
			name: "a password file that is not",
			profile: Profile{Backend: BackendClickHouse, ClickHouse: &ClickHouseProfile{
				Addr: "10.0.1.5:9000", PasswordFile: absent,
			}},
			want: Credential{Source: CredentialFile, Reference: absent, State: CredentialMissing},
		},
		{
			// An ordinary state rather than a misconfiguration: a local evaluation
			// server usually has no password, which is what ResolvePassword returns
			// nothing and no error for.
			name: "a ClickHouse profile naming neither",
			profile: Profile{Backend: BackendClickHouse, ClickHouse: &ClickHouseProfile{
				Addr: "127.0.0.1:9000",
			}},
			want: Credential{Source: CredentialNone, State: CredentialNotChecked},
		},
		{
			name:    "an S3 archive reads the AWS credential chain",
			profile: Profile{Backend: BackendS3, S3: &S3Profile{Bucket: "acme-audit"}},
			want:    Credential{Source: CredentialAmbient, State: CredentialNotChecked},
		},
		{
			name:    "a local archive needs no credential at all",
			profile: Profile{Backend: BackendLocal, Local: &LocalProfile{Path: "/archives/kuberecord"}},
			want:    Credential{Source: CredentialNone, State: CredentialNotChecked},
		},
		{
			// The fallback the table needs rather than a crash, for the reason
			// Target has one.
			name:    "a backend with no stanza",
			profile: Profile{Backend: BackendClickHouse},
			want:    Credential{Source: CredentialNone, State: CredentialNotChecked},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.export {
				t.Setenv(credentialEnv, plantedPassword)
			} else {
				unsetCredentialEnv(t)
			}

			got := test.profile.Credential()
			if got != test.want {
				t.Errorf("Credential() = %+v, want %+v", got, test.want)
			}
			// The reference is a name or a path and never a value. Asserted on
			// every case rather than only on the two that hold one, because the
			// property is about the type and not about a branch of it.
			if strings.Contains(got.Reference, plantedPassword) ||
				strings.Contains(string(got.State), plantedPassword) {
				t.Errorf("Credential() carries the password itself: %+v", got)
			}
		})
	}
}

// TestAnUnreadablePasswordFileIsNotReportedAsAbsent.
//
// The two states send a reader to different fixes — one file has to be created
// and the other has to be made readable — and reporting the second as the first
// is a claim the check did not verify. It is also not a hypothetical: a password
// file is exactly the file somebody chmods to 0000 or leaves in a directory root
// owns, and the existing routes test plants that state deliberately.
func TestAnUnreadablePasswordFileIsNotReportedAsAbsent(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a mode-0000 file, so the failure this case needs cannot be produced")
	}
	unsetCredentialEnv(t)

	path := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(path, []byte(plantedPassword), ConfigFileMode); err != nil {
		t.Fatalf("writing the password file: %v", err)
	}
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatalf("making the password file unreadable: %v", err)
	}

	profile := Profile{Backend: BackendClickHouse, ClickHouse: &ClickHouseProfile{
		Addr: "10.0.1.5:9000", PasswordFile: path,
	}}
	got := profile.Credential()
	want := Credential{Source: CredentialFile, Reference: path, State: CredentialUnreadable}
	if got != want {
		t.Errorf("Credential() = %+v, want %+v", got, want)
	}
}

// TestCredentialAgreesWithWhatAQueryWillFind.
//
// The column's value is that it predicts the next query, so the two must be one
// decision rather than two that agree today. This drives both through the same
// stanza and fails if a state reporting a resolvable reference sits beside a
// ResolvePassword that refuses it — the drift a separate presence check would
// introduce the day one of them learned about a case the other did not.
func TestCredentialAgreesWithWhatAQueryWillFind(t *testing.T) {
	absent := filepath.Join(t.TempDir(), "no-such-password")
	present := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(present, []byte(plantedPassword), ConfigFileMode); err != nil {
		t.Fatalf("writing the password file: %v", err)
	}

	tests := []struct {
		name     string
		stanza   *ClickHouseProfile
		export   bool
		resolves bool
	}{
		{
			name:     "the variable is exported",
			stanza:   &ClickHouseProfile{Addr: "10.0.1.5:9000", PasswordEnv: credentialEnv},
			export:   true,
			resolves: true,
		},
		{
			name:   "the variable is not",
			stanza: &ClickHouseProfile{Addr: "10.0.1.5:9000", PasswordEnv: credentialEnv},
		},
		{
			name:     "the file is there",
			stanza:   &ClickHouseProfile{Addr: "10.0.1.5:9000", PasswordFile: present},
			resolves: true,
		},
		{
			name:   "the file is not",
			stanza: &ClickHouseProfile{Addr: "10.0.1.5:9000", PasswordFile: absent},
		},
		{
			name:     "there is no reference",
			stanza:   &ClickHouseProfile{Addr: "127.0.0.1:9000"},
			resolves: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.export {
				t.Setenv(credentialEnv, plantedPassword)
			} else {
				unsetCredentialEnv(t)
			}

			_, err := test.stanza.ResolvePassword()
			credential := Profile{Backend: BackendClickHouse, ClickHouse: test.stanza}.Credential()

			if (err == nil) != test.resolves {
				t.Fatalf("ResolvePassword resolved = %v, want %v (err: %v)", err == nil, test.resolves, err)
			}
			if reported := credentialResolves(credential.State); reported != test.resolves {
				t.Errorf("the column reports %q for a reference ResolvePassword resolved = %v",
					credential.State, test.resolves)
			}
		})
	}
}

// credentialResolves reads a state back as the boolean the agreement test
// compares.
//
// It lives in the test rather than beside the states themselves, deliberately:
// the production vocabulary is source-specific words because the two failures
// send a reader to two different places, and a boolean accessor on it would be
// the field somebody renders as `resolves: false` for an ambient credential
// nobody checked.
func credentialResolves(state CredentialState) bool {
	switch state {
	case CredentialSet, CredentialPresent, CredentialNotChecked:
		return true
	case CredentialNotSet, CredentialMissing, CredentialUnreadable:
		return false
	}
	return false
}

// unsetCredentialEnv guarantees this file's variable is absent for the duration
// of a test, and restored after it.
//
// The shape unsetPasswordEnv uses: t.Setenv is what registers the restoration,
// and the Unsetenv that follows is the state the case actually needs.
func unsetCredentialEnv(t *testing.T) {
	t.Helper()
	t.Setenv(credentialEnv, "")
	if err := os.Unsetenv(credentialEnv); err != nil {
		t.Fatalf("unsetting %s: %v", credentialEnv, err)
	}
}
