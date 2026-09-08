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
	"strings"
	"testing"
)

// TestProfileDescribeAppliesTheDefaultsAQueryWouldUse.
//
// The subject of a description is where a query would actually go, not what the
// stanza happens to spell. A profile with no database named reads `kuberecord`,
// and a description saying `at 10.0.1.5:9000/` would describe nothing that
// exists — which matters most in the one message this method was added for, where
// it names a profile that has just stopped existing and cannot be looked up.
func TestProfileDescribeAppliesTheDefaultsAQueryWouldUse(t *testing.T) {
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
			want: "ClickHouse at 10.0.1.5:9000/audit",
		},
		{
			name: "clickhouse with no database named",
			profile: Profile{Backend: BackendClickHouse, ClickHouse: &ClickHouseProfile{
				Addr: "10.0.1.5:9000",
			}},
			want: "ClickHouse at 10.0.1.5:9000/" + DefaultClickHouseDatabase,
		},
		{
			name: "s3 with a prefix",
			profile: Profile{Backend: BackendS3, S3: &S3Profile{
				Bucket: "acme-audit", Prefix: "kuberecord", Region: "eu-west-1",
			}},
			want: "s3://acme-audit/kuberecord, region eu-west-1",
		},
		{
			name:    "s3 with no region named",
			profile: Profile{Backend: BackendS3, S3: &S3Profile{Bucket: "acme-audit"}},
			want:    "s3://acme-audit, region " + DefaultS3Region,
		},
		{
			name:    "local",
			profile: Profile{Backend: BackendLocal, Local: &LocalProfile{Path: "/archives/kuberecord"}},
			want:    "local archive at /archives/kuberecord",
		},
		{
			// Reachable only from a struct assembled in memory, since LoadConfig
			// refuses such a profile. A nil dereference inside a message about a
			// misassembled profile would replace the complaint with a crash.
			name:    "a backend with no stanza is described by its name",
			profile: Profile{Backend: BackendClickHouse},
			want:    "clickhouse",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.profile.Describe(); got != test.want {
				t.Errorf("Describe() = %q, want %q", got, test.want)
			}
		})
	}
}

// TestTheResolutionNoticeAndProfileDescribeAreOneSpelling.
//
// `config delete-profile` says what it removed and the resolution notice says
// what it opened, and both are describing a profile stanza. Two spellings of that
// would drift, and the one nobody compiles is the one that drifts: a user who read
// `ClickHouse at host/db` in one message and something else in the other has to
// work out whether they are looking at the same thing.
//
// The notice adds the profile's *name* and the --sink-addr note, and those are
// this test's boundary: it asserts that the description is inside the notice, not
// that the two are equal.
func TestTheResolutionNoticeAndProfileDescribeAreOneSpelling(t *testing.T) {
	tests := []struct {
		name    string
		profile Profile
	}{
		{
			name: "clickhouse",
			profile: Profile{Backend: BackendClickHouse, ClickHouse: &ClickHouseProfile{
				Addr: "clickhouse.example:9000", Database: "kuberecord", Username: "kuberecord_ro",
			}},
		},
		{
			name: "s3",
			profile: Profile{Backend: BackendS3, S3: &S3Profile{
				Bucket: "acme-audit", Prefix: "kuberecord",
			}},
		},
		{
			name:    "local",
			profile: Profile{Backend: BackendLocal, Local: &LocalProfile{Path: "/archives/kuberecord"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chosen, err := targetFromProfile("prod", test.profile, "")
			if err != nil {
				t.Fatalf("targetFromProfile: %v", err)
			}
			want := "prod (" + test.profile.Describe() + ")"
			if chosen.description != want {
				t.Errorf("the resolution notice describes the profile as %q, want %q",
					chosen.description, want)
			}
		})
	}

	// And the one case where the two must differ: --sink-addr replaces the
	// endpoint of this invocation's backend (D25), so the notice describes the
	// address that was dialled while the stanza still names the address it
	// records. A describer that read the stanza would report the address the
	// override replaced.
	profile := Profile{Backend: BackendClickHouse, ClickHouse: &ClickHouseProfile{
		Addr: "clickhouse.example:9000", Database: "kuberecord",
	}}
	chosen, err := targetFromProfile("prod", profile, "127.0.0.1:9000")
	if err != nil {
		t.Fatalf("targetFromProfile with --sink-addr: %v", err)
	}
	if !strings.Contains(chosen.description, "ClickHouse at 127.0.0.1:9000/kuberecord") {
		t.Errorf("the notice does not describe the overridden address: %q", chosen.description)
	}
	if !strings.Contains(profile.Describe(), "clickhouse.example:9000") {
		t.Errorf("the stanza's own description changed with an override that does not touch it: %q",
			profile.Describe())
	}
}

// TestRequireProfileListsWhatTheFileDoesHold is the requireSecretKey shape, and
// the reason it is one function: three callers ask this question, and a mistyped
// profile name is almost always settled by seeing the list.
func TestRequireProfileListsWhatTheFileDoesHold(t *testing.T) {
	cfg := &Config{Profiles: map[string]Profile{
		"prod":    {Backend: BackendClickHouse, ClickHouse: &ClickHouseProfile{Addr: "host:9000"}},
		"archive": {Backend: BackendS3, S3: &S3Profile{Bucket: "acme-audit"}},
	}}

	if _, err := RequireProfile(cfg, "/config.yaml", "prod"); err != nil {
		t.Fatalf("RequireProfile of a profile that exists: %v", err)
	}

	_, err := RequireProfile(cfg, "/config.yaml", "prd")
	if err == nil {
		t.Fatal("RequireProfile accepted a name the file does not hold")
	}
	for _, want := range []string{`"prd"`, "/config.yaml", "defined: archive, prod"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not name %s: %v", want, err)
		}
	}

	// An empty file says so rather than offering an empty list, which would read
	// as a rendering failure rather than as the ordinary state of a first
	// invocation.
	_, err = RequireProfile(&Config{}, "/config.yaml", "prod")
	if err == nil || !strings.Contains(err.Error(), "defines no profiles") {
		t.Errorf("an empty file produced %v", err)
	}
}
