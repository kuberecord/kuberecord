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

// Package buildinfo reports which build of the CLI is running.
//
// It exists as a package of its own for two reasons, and both are about the
// linker. The first is that `-X` names a variable by its full import path, so the
// target of a stamp is a public interface of the build: `make build-cli` writes
// github.com/kuberecord/kuberecord/internal/cli/buildinfo.version, and a variable
// that moved would silently stop being stamped — a build with no version in it
// rather than a build that fails. Keeping it in one small package makes that path
// a thing somebody has to change on purpose.
//
// The second is that this package must never fail to build or to run. It imports
// the standard library and nothing else, which deps_test.go asserts: a `version`
// command that could not answer because a backend package it did not need failed
// to initialise would be worse than useless — it is the command someone runs
// precisely when something else is already wrong.
//
// Nothing here is stamped when the binary is built any other way, which is not a
// corner case: `go install github.com/kuberecord/kuberecord/cmd/kubectl-kuberecord@v0.3.0`
// is a documented install path and cannot pass linker flags. The build info the
// toolchain embeds by itself answers for those builds — see Resolve.
package buildinfo

import (
	"runtime"
	"runtime/debug"
	"strings"
)

// The stamped fields. Empty in every build that did not pass `-X`, which is what
// Resolve's fallbacks are for.
//
// They are unexported so that the only way to read them is through Get, which
// applies those fallbacks. An exported variable would be read directly by the
// first caller in a hurry, and that caller would print an empty string for a
// `go install` build.
var (
	version string
	commit  string
	date    string
)

// Unknown is what a field reports when neither the linker nor the toolchain's own
// build info could say.
//
// A word rather than an empty string, because the output is read by a person
// asking "which build is this?" and a blank space next to `commit` answers a
// different question — it looks like the field does not exist rather than like
// the fact is unavailable.
const Unknown = "unknown"

// DevelVersion is what the toolchain records for a build from a working tree
// with no module version, and it is passed through rather than translated: it is
// the Go ecosystem's own spelling and a reader who has seen it elsewhere already
// knows what it means.
const DevelVersion = "(devel)"

// shortCommitLength is how much of a revision this prints.
//
// Twelve hex characters is what `git log --abbrev-commit` settles on for a
// repository this size and is unambiguous well past any history it will have. The
// full forty is available in the build info of the binary itself
// (`go version -m`), which is where somebody who needs it should look.
const shortCommitLength = 12

// dirtySuffix marks a build made from a working tree with uncommitted changes.
//
// It matters more here than in most places: the commit is the only thing tying an
// artifact to a source tree, and a commit printed without this suffix for a build
// that was not made from it is an untraceable binary claiming to be traceable.
const dirtySuffix = "-dirty"

// Info is one build's identity.
//
// Every field is a string, including the date. This is a report about a build
// rather than a value anything computes with, and a time.Time would invite a
// caller to format it a second way — which is how the human-readable output and
// the structured output come to disagree about the same instant.
type Info struct {
	// Version is the release this was built from, `v`-prefixed, or DevelVersion
	// or Unknown when nothing said.
	Version string

	// Commit is the abbreviated revision, with dirtySuffix when the tree it was
	// built from had uncommitted changes.
	Commit string

	// BuildDate is when the binary was linked, RFC 3339 in UTC.
	BuildDate string

	// GoVersion is the toolchain that compiled it, as `go version` spells it.
	GoVersion string

	// Platform is the GOOS/GOARCH this binary runs on. It is read at runtime
	// rather than stamped, so a mislabelled archive cannot make a binary lie
	// about what it is.
	Platform string
}

// Get reports the running binary's identity.
func Get() Info {
	embedded, ok := debug.ReadBuildInfo()
	return Resolve(version, commit, date, embedded, ok)
}

// Resolve is Get's whole decision, separated from the two sources it reads so
// that the fallbacks can be tested rather than assumed.
//
// The order is stamped-first, and the reason is that the two sources answer
// slightly different questions. `-X` says what the release *is* — the tag being
// published, which is the string a user compares against a release page — while
// the embedded info says what the toolchain observed: a pseudo-version for a
// build from a branch, and nothing at all for a binary built with
// `-buildvcs=false` or from an unpacked source archive. Where both exist the
// stamp is the more useful answer, and where neither does the field says so.
//
// embedded may be nil, and ok may be false, which is exactly what a caller gets
// from a binary the toolchain recorded nothing about.
func Resolve(stampedVersion, stampedCommit, stampedDate string, embedded *debug.BuildInfo, ok bool) Info {
	info := Info{
		Version:   stampedVersion,
		Commit:    stampedCommit,
		BuildDate: stampedDate,
		GoVersion: runtime.Version(),
		Platform:  runtime.GOOS + "/" + runtime.GOARCH,
	}
	if !ok || embedded == nil {
		return info.withDefaults()
	}

	if info.Version == "" {
		info.Version = embedded.Main.Version
	}
	if info.GoVersion == "" {
		// Unreachable through Get, since runtime.Version() always answers. It is
		// here so that Resolve is total over its inputs rather than over the ones
		// one caller happens to pass.
		info.GoVersion = embedded.GoVersion
	}

	// The VCS stamps the toolchain adds for a build from a checkout. They are
	// absent for `go install`, which resolves a module version instead, and for a
	// build with `-buildvcs=false`.
	settings := make(map[string]string, len(embedded.Settings))
	for _, setting := range embedded.Settings {
		settings[setting.Key] = setting.Value
	}
	if info.Commit == "" {
		info.Commit = shorten(settings["vcs.revision"])
		if info.Commit != "" && settings["vcs.modified"] == "true" {
			info.Commit += dirtySuffix
		}
	}
	if info.BuildDate == "" {
		info.BuildDate = settings["vcs.time"]
	}
	return info.withDefaults()
}

// withDefaults names what nothing could answer.
func (i Info) withDefaults() Info {
	for _, field := range []*string{&i.Version, &i.Commit, &i.BuildDate, &i.GoVersion, &i.Platform} {
		if strings.TrimSpace(*field) == "" {
			*field = Unknown
		}
	}
	return i
}

// shorten abbreviates a revision, leaving anything already short alone.
//
// A revision shorter than shortCommitLength is returned unchanged rather than
// padded or rejected: the field is a fact about a build, and a stamp that is not
// a git hash — a build system's own identifier, say — is still the most useful
// thing this can print.
func shorten(revision string) string {
	if len(revision) <= shortCommitLength {
		return revision
	}
	return revision[:shortCommitLength]
}
