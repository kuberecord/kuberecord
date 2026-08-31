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

package buildinfo_test

import (
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli/buildinfo"
)

// TestResolve covers the ways a binary comes to exist, because each one answers
// the "which build is this?" question from a different source and the failure
// mode of getting it wrong is silent: a version command that prints an empty
// string looks like a broken terminal rather than an unstamped build.
func TestResolve(t *testing.T) {
	const (
		revision = "77514b6329259d2354f2047b08f82bac4a077a19"
		short    = "77514b632925"
		vcsTime  = "2026-08-31T18:50:32Z"
	)

	// A checkout build with no linker flags: `go build ./cmd/kubectl-kuberecord`.
	fromCheckout := &debug.BuildInfo{
		Main: debug.Module{Version: buildinfo.DevelVersion},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: revision},
			{Key: "vcs.time", Value: vcsTime},
			{Key: "vcs.modified", Value: "false"},
		},
	}

	tests := []struct {
		name                            string
		version, commit, date           string
		embedded                        *debug.BuildInfo
		ok                              bool
		wantVersion, wantCommit, wantAt string
	}{
		{
			name:    "a release build stamps every field",
			version: "v0.3.0", commit: "77514b632925", date: "2026-08-31T21:04:11Z",
			embedded: fromCheckout, ok: true,
			wantVersion: "v0.3.0", wantCommit: "77514b632925", wantAt: "2026-08-31T21:04:11Z",
		},
		{
			name:     "an unstamped checkout build falls back to the VCS stamps",
			embedded: fromCheckout, ok: true,
			wantVersion: buildinfo.DevelVersion, wantCommit: short, wantAt: vcsTime,
		},
		{
			// The reason the suffix exists: the commit is the only thing tying an
			// artifact to a source tree, and printing it bare for a build made
			// from a dirty tree makes an untraceable binary look traceable.
			name: "a dirty checkout says so",
			embedded: &debug.BuildInfo{
				Main: debug.Module{Version: buildinfo.DevelVersion},
				Settings: []debug.BuildSetting{
					{Key: "vcs.revision", Value: revision},
					{Key: "vcs.modified", Value: "true"},
				},
			},
			ok:          true,
			wantVersion: buildinfo.DevelVersion, wantCommit: short + "-dirty", wantAt: buildinfo.Unknown,
		},
		{
			// `go install …@v0.3.0`, a documented install path that cannot pass
			// linker flags: the module version is the only answer available, and
			// there are no VCS stamps at all.
			name: "go install reports the module version and no revision",
			embedded: &debug.BuildInfo{
				Main: debug.Module{Version: "v0.3.0"},
			},
			ok:          true,
			wantVersion: "v0.3.0", wantCommit: buildinfo.Unknown, wantAt: buildinfo.Unknown,
		},
		{
			// A stamp beats the embedded pseudo-version: `-X` says what the
			// release is, which is the string a user compares against a release
			// page.
			name:    "a stamped version wins over the embedded one",
			version: "v0.3.0",
			embedded: &debug.BuildInfo{
				Main: debug.Module{Version: "v0.2.2-0.20260831185032-77514b632925"},
				Settings: []debug.BuildSetting{
					{Key: "vcs.revision", Value: revision},
					{Key: "vcs.time", Value: vcsTime},
				},
			},
			ok:          true,
			wantVersion: "v0.3.0", wantCommit: short, wantAt: vcsTime,
		},
		{
			name:        "a binary the toolchain recorded nothing about says unknown",
			ok:          false,
			wantVersion: buildinfo.Unknown, wantCommit: buildinfo.Unknown,
			wantAt: buildinfo.Unknown,
		},
		{
			// ok true with a nil pointer is not a shape the runtime produces, and
			// that is the point: Resolve is total over its inputs rather than over
			// the ones one caller happens to pass.
			name:        "a nil build info is survivable",
			ok:          true,
			wantVersion: buildinfo.Unknown, wantCommit: buildinfo.Unknown,
			wantAt: buildinfo.Unknown,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := buildinfo.Resolve(tc.version, tc.commit, tc.date, tc.embedded, tc.ok)

			if got.Version != tc.wantVersion {
				t.Errorf("Version = %q, want %q", got.Version, tc.wantVersion)
			}
			if got.Commit != tc.wantCommit {
				t.Errorf("Commit = %q, want %q", got.Commit, tc.wantCommit)
			}
			if got.BuildDate != tc.wantAt {
				t.Errorf("BuildDate = %q, want %q", got.BuildDate, tc.wantAt)
			}
			// Read at runtime rather than stamped, on every path, so that a
			// mislabelled archive cannot make a binary lie about what it is.
			if want := runtime.GOOS + "/" + runtime.GOARCH; got.Platform != want {
				t.Errorf("Platform = %q, want %q", got.Platform, want)
			}
			if got.GoVersion != runtime.Version() {
				t.Errorf("GoVersion = %q, want %q", got.GoVersion, runtime.Version())
			}
		})
	}
}

// TestGetNeverReturnsAnEmptyField is the property the version command depends on:
// every field is either a fact or the word that says there is no fact.
//
// It runs against the real build info of the test binary, which is stamped by
// nothing, so it exercises the path a `go build` user gets rather than the one a
// release gets.
func TestGetNeverReturnsAnEmptyField(t *testing.T) {
	got := buildinfo.Get()

	for name, value := range map[string]string{
		"Version":   got.Version,
		"Commit":    got.Commit,
		"BuildDate": got.BuildDate,
		"GoVersion": got.GoVersion,
		"Platform":  got.Platform,
	} {
		if value == "" {
			t.Errorf("%s is empty; an unavailable fact must be spelled %q, because a blank "+
				"beside a label reads as a broken terminal rather than as an unstamped build",
				name, buildinfo.Unknown)
		}
	}
}
