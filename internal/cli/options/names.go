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

package options

// The two names this binary answers to, and the two ways it names itself back.
//
// Both are built from one package: krew installs the plugin binary, a direct
// download or `go install` gets the standalone one, and an engineer who has both
// on their PATH must not get two different help texts from one implementation.
//
// They live here, below the command tree, rather than beside the detection that
// reads argv. Backend resolution names the command in its remediation messages —
// "run `kuberecord config set-profile …`" — and a resolver that had to import the
// command package to spell its own program's name would put the whole cobra tree
// on the resolution path, which is the direction Task 11.8 exists to forbid. The
// detection itself, InvocationName, stays with command construction: it is a fact
// about this invocation, not about the program's identity.
const (
	// StandaloneName is the command as invoked directly.
	StandaloneName = "kuberecord"

	// PluginBinaryName is the file name kubectl requires of a plugin providing
	// the `kuberecord` subcommand. kubectl finds plugins by this convention
	// alone, so the name is fixed by kubectl rather than chosen here.
	PluginBinaryName = "kubectl-kuberecord"

	// PluginInvocation is how that plugin must describe itself: a user who ran
	// `kubectl kuberecord` and is shown `kuberecord timeline …` in an example
	// has been given a command that will not work unless they happen to also
	// have the standalone binary installed.
	PluginInvocation = "kubectl kuberecord"
)
