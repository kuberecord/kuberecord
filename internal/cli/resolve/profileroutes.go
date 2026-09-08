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
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
)

// The second failure that stops a command dead, and the second one whose whole
// remedy already existed somewhere the reader could not see.
//
// A profile is step 3 of the resolution chain. When what it references does not
// resolve — an environment variable the shell never exported, a password file
// that is not there — the chain stops. It stops for a reason: falling through to
// the cluster's own sink would read from somewhere the user did not choose and
// report success, which is the same "connects somewhere other than where it was
// told" property D24 forbids of an address. D35 settles it for a profile. A
// configured profile that cannot be resolved is fatal.
//
// What was missing is the sentence after that. Three routes get past this
// failure, all three work today, and the error named none of them:
//
//	export KUBERECORD_CLICKHOUSE_PASSWORD=…                resolve the reference
//	kuberecord timeline … --sink ClickHouseSink/default    bypass the profile
//	kuberecord config use-profile other                    switch away
//
// The third of those names `config get-profiles` beside it, which reports the
// same credential state for every profile in the file — so a reader deciding
// which one to switch to can see which of them would resolve. See switchRoute.
//
// The middle one is the one people reach for. --sink is step 2 and the profile is
// step 3, so naming a sink is reached first and the credential comes from the
// Secret that sink references — the profile is not consulted at all.
//
// # Why --sink-addr is not that route
//
// It is the flag a reader passes when they are one step from it, and it fails
// identically: --sink-addr replaces the endpoint of whichever step answered and
// never a credential (D36). targetFromProfile resolves the password before it
// applies the override, and its own comment says why — one field replaced and no
// other. So when the flag was given the message says so, rather than leaving a
// reader to conclude the flag is broken and stop there.
//
// # What this file must not become
//
// Nothing here reorders the chain, softens the profile step, or widens what
// --sink-addr replaces. Each of those would make this failure disappear by
// letting the tool read from somewhere it was not told to, and an audit reader
// whose answers carry an unstated "…from somewhere" is worse than one that
// stopped. The remedy is a message that names the bypass; it is never the bypass.
//
// # Why it is its own file
//
// The same reason diagnose.go is. The message is documentation that happens to be
// compiled, it will be edited by people improving wording rather than by people
// changing resolution, and a paragraph in the middle of resolve.go would be
// edited by neither.

// UnresolvableProfileError is a configured profile that could not be turned into
// a backend.
//
// Its one-line form is the cause's own, because the cause already names the
// profile and the reason — "profile \"prod\": the environment variable … is not
// set" is the sentence the chain step recorded and the sentence `error:` prints,
// and a wrapper that prefixed it again would say the word twice. It carries no
// exit code of its own either: the cause keeps the one the chain gave it, so a
// malformed stanza still exits 1 and nothing about classification moves.
//
// What it adds is Render, the block of routes the top of the CLI prints beneath
// that line. The split is the one UnreachableSinkError makes and it is made for
// the same two reasons: a multi-line Error would be spliced into the middle of
// every caller that wraps it with context, and an error string cannot know
// whether it is going to a terminal.
type UnresolvableProfileError struct {
	routes profileRoutes
	cause  error
}

// Error is the cause verbatim. See the type's own comment for why it adds nothing.
func (e *UnresolvableProfileError) Error() string { return e.cause.Error() }

// Unwrap keeps the original failure reachable, so that explaining a failure never
// costs a caller the ability to classify it — exit.CodeFor reads the cause's code
// straight through this.
func (e *UnresolvableProfileError) Unwrap() error { return e.cause }

// profileRoutes is everything the message needs, gathered where it is already
// known.
//
// It is built in resolveTarget, which has just read the configuration file and
// knows both how this profile came to be chosen and what else the file holds. By
// the time the failure reaches the top of the CLI none of that is in scope, which
// is the same reason diagnosis travels on a target rather than being refetched.
//
// It holds no credential, and the two password fields are the reason to say so
// out loud: they are the *name* of an environment variable and the *path* of a
// file, which is what a profile stores and what a message may print. The value
// behind either one is read a few lines away in ResolvePassword and deliberately
// does not travel here.
type profileRoutes struct {
	// name is the profile that failed, and backend is what it said it was — which
	// decides whether the bypass route can name a sink kind or has to ask for one.
	name    string
	backend BackendKind

	// namedByFlag distinguishes --profile prod from a stanza the file quietly made
	// active. They are different situations for the reader and they have different
	// ways out: one is a flag to change, the other is a pointer in a file.
	namedByFlag bool

	// active reports that this profile is the file's currentProfile, which is what
	// decides whether deleting it needs --force (see config delete-profile).
	active bool

	// configPath is the file all of this was read from, named when the reader has
	// to go and look at it.
	configPath string

	// others are the profiles that file defines apart from this one, sorted. Empty
	// is the case that needs its own sentence: there is nothing to switch to.
	others []string

	// commandName is how this process was invoked — `kuberecord` or
	// `kubectl kuberecord` — so the `config` commands name something typable.
	commandName string

	// sinkAddr is the endpoint override this invocation carried, empty for none.
	// Its presence, not its effect, is what the message reacts to: the flag was
	// applied to an address and the failure was about a credential.
	sinkAddr string

	// passwordEnv and passwordFile are the reference the stanza holds, at most one
	// of them. Neither set means the failure was not about a credential at all.
	passwordEnv  string
	passwordFile string
}

// profileRoutes gathers what a failed profile's message will need.
func (r *BackendResolver) profileRoutes(name string, profile Profile) profileRoutes {
	routes := profileRoutes{
		name:        name,
		backend:     profile.Backend,
		configPath:  r.ConfigPath,
		commandName: r.commandName(),
		sinkAddr:    r.sinkAddr(),
	}
	if r.Flags != nil {
		routes.namedByFlag = r.Flags.Profile != ""
	}
	if r.Config != nil {
		routes.active = r.Config.CurrentProfile == name
		others := make([]string, 0, len(r.Config.Profiles))
		for _, other := range slices.Sorted(maps.Keys(r.Config.Profiles)) {
			if other != name {
				others = append(others, other)
			}
		}
		routes.others = others
	}
	if profile.ClickHouse != nil {
		routes.passwordEnv = profile.ClickHouse.PasswordEnv
		routes.passwordFile = profile.ClickHouse.PasswordFile
	}
	return routes
}

// explainProfile attaches the routes past a profile that could not be resolved.
//
// Everything the profile step raises about the *stanza* gets the block: a
// credential reference that does not resolve, a backend name the file does not
// define. A usage error does not, and the distinction is one exit codes already
// draw — a usage error here is about what was typed on this command line rather
// than about what the file holds, and the message that raised it is the one that
// knows which flags conflicted with which. --sink-addr against an archive profile
// is the only instance today, it already names both routes past itself, and a
// second block underneath cobra's usage output would bury them.
//
// A failure added later that is about the stanza and reported as a usage error
// would fall through this and lose its routes, which is a reason to report it as
// what it is rather than a reason to enumerate cases here.
func (r *BackendResolver) explainProfile(name string, profile Profile, err error) error {
	if err == nil {
		return nil
	}
	if exit.CodeFor(err) == exit.UsageError {
		return err
	}
	return &UnresolvableProfileError{routes: r.profileRoutes(name, profile), cause: err}
}

// Render writes the three routes past this profile.
//
// commandPath is the command the user actually ran, as cobra spells it, and
// colorize is decided by the caller from --color, NO_COLOR and whether stderr is
// a terminal — both for the reasons UnreachableSinkError.Render takes them: this
// package does not know which command failed, and a function that consulted the
// environment itself would have golden files that changed with the shell they
// were generated in.
//
// The register is the closed vocabulary every other notice uses (D27): Warning
// prose, because a reader who skips this block is left believing a working route
// does not exist (D30), with Emphasis on the lines meant to be typed.
func (e *UnresolvableProfileError) Render(commandPath string, colorize bool) string {
	severity := render.NewSeverity(colorize)
	r := e.routes

	invocation := commandPath
	if invocation == "" {
		invocation = r.commandName
	}

	var out strings.Builder
	line := func(text string) { out.WriteString(text + "\n") }
	prose := func(text string) { line(severity.Warning(text)) }
	command := func(text string) { line(severity.Emphasis("    " + text)) }
	gap := func() { line("") }

	// The marker goes on the first line and on no other, as it does in the
	// unreachable-backend block: this is a paragraph rather than a notice, and
	// marking every line of it would put a column of "!" down the side of a page
	// somebody is reading.
	line(render.WarningMarker + " " + severity.Warning(
		fmt.Sprintf("profile %q is where this invocation reads from, and the chain stops here.", r.name)))
	gap()
	for _, text := range r.chosenBecause() {
		prose(text)
	}
	gap()
	prose("Falling through to the cluster's own sink would read from somewhere you did not choose")
	prose("and report success, so a profile that cannot be resolved is fatal rather than skipped.")
	prose("Three routes get past it, and all three work today.")
	gap()

	r.referenceRoute(prose, command, gap)
	gap()
	r.bypassRoute(prose, command, gap, invocation)
	gap()
	r.switchRoute(prose, command, gap, invocation)

	// Suppressed under `config resolve` itself, which is the one command that has
	// already printed the step-by-step report this line offers. Telling a reader
	// to run what they just ran spends the most load-bearing paragraph in the CLI
	// on a no-op.
	if !strings.HasSuffix(commandPath, resolveReportCommand) {
		gap()
		prose("To watch the chain make this decision, with this step's own reason beside it:")
		gap()
		command(r.commandName + " " + resolveReportCommand)
	}

	return out.String()
}

// resolveReportCommand is the inspecting command this block points at, without
// the program's own name in front of it.
//
// One constant for both uses on purpose: the block suppresses the pointer when
// the command that failed *is* that report, and the test for whether it is has to
// be the same string as the command being suggested. Two literals would let the
// suppression stop matching a renamed subcommand while the suggestion carried on
// naming it.
const resolveReportCommand = "config resolve"

// chosenBecause says why this profile was the one, in the two spellings
// profileDetail already distinguishes.
//
// The difference is the whole of the confusion `config resolve` was built to
// expose: a profile this invocation named, and one a file written months ago
// quietly made active. A reader who has forgotten the second is a reader for whom
// "switch away" is the route, and they cannot take it without knowing which case
// they are in.
//
// Both spellings name the file, and it gets a line of its own and no full stop —
// the rule this package applies to every token meant to be copied rather than
// read. A home directory and an XDG override produce paths of very different
// lengths, and a sentence that wrapped around one of them would be a sentence
// that wrapped differently on two machines.
func (r profileRoutes) chosenBecause() []string {
	opening := "It is the currentProfile in"
	if r.namedByFlag {
		opening = fmt.Sprintf("It was named by --%s on this command line, and the file defining it is",
			options.FlagProfile)
	}
	return []string{opening, r.configPath}
}

// referenceRoute is the first route: make what the profile points at resolve.
//
// Which sentence applies is decided by the reference the stanza holds rather than
// by the text of the failure, because the stanza is the structural fact and the
// failure is prose. A profile naming an environment variable can only have failed
// on that variable; one naming a file can only have failed on that file; one
// naming neither did not fail on a credential at all, and the route for it is to
// write the stanza again.
func (r profileRoutes) referenceRoute(prose, command func(string), gap func()) {
	switch {
	case r.passwordEnv != "":
		prose("Export the variable this profile names as where its password comes from:")
		gap()
		command("export " + r.passwordEnv + "=…")

	case r.passwordFile != "":
		prose("The profile reads its password from a file. This is the path it names, and nothing")
		prose("here creates one:")
		gap()
		// A line to itself, unwrapped, for the reason the unreachable-backend
		// message gives the address one: it is the string a reader may need to
		// compare character by character with what is on their disk.
		prose("    " + r.passwordFile)
		gap()
		prose("Create it, readable by you. Or write the stanza again naming an environment variable")
		prose("instead — `set-profile` replaces a profile whole rather than merging into it, and")
		prose("with no flags it asks:")
		gap()
		command(r.commandName + " config set-profile " + r.name)

	default:
		prose("Write the stanza again. `set-profile` replaces a profile whole rather than merging")
		prose("into it, and with no flags it asks:")
		gap()
		command(r.commandName + " config set-profile " + r.name)
	}
}

// bypassRoute is the second route, and the one the task exists for: --sink is
// reached before the profile is.
//
// The sink's name is a placeholder and stays one. Listing what the cluster holds
// would mean a round trip on a failure path — a diagnostic performing I/O to
// improve its own wording is the thing Invariant 5 and D24 refuse — so the
// message names the command that lists them and leaves the reader to run it.
func (r profileRoutes) bypassRoute(prose, command func(string), gap func(), invocation string) {
	prose(fmt.Sprintf("Or skip the profile for this one invocation. --%s is step 2 and a profile is step 3,",
		options.FlagSink))
	prose("so a named sink is reached first and its credential comes from the Secret it references.")
	prose(fmt.Sprintf("`kubectl get %s` names the ones this cluster holds:", r.sinkResources()))
	gap()
	command(fmt.Sprintf("%s … --%s %s%s", invocation, options.FlagSink, r.sinkPlaceholder(), r.addrSuffix()))

	if r.sinkAddr == "" {
		return
	}
	// Printed only when the flag was given, because that reader has already tried
	// it and been refused for a reason the flag's name does not suggest. Task 16.4
	// wrote the section that settles it; this is the moment somebody needs it.
	gap()
	prose(fmt.Sprintf("--%s did not get you there on its own: it replaces the endpoint of whichever",
		options.FlagSinkAddr))
	prose("step answered and never a credential, so this profile's password still came from the")
	prose("reference above. One field replaced, no other:")
	prose(docsSourceVersusSinkAddr)
}

// switchRoute is the third: stop this profile being the one that answers.
//
// The only-profile case is told so rather than offered a switch to nothing, for
// the reason errDeletingTheActiveProfile gives the same sentence: a remedy naming
// no command reads as though the reader should have known which name to
// substitute. The two commands it offers instead are both real — one writes
// another profile, the other removes this one and lets discovery answer again.
//
// # Why the listing is named here and not elsewhere in the block
//
// `config get-profiles` is this diagnosis in a table: its CREDENTIAL column
// reports, for every profile, the same env-set-or-unset and file-present-or-
// missing fact that this page of prose reports for one. So it is named at the
// moment the reader has been told to switch to a different profile, because
// "will that one work?" is the question they now have and the column is the
// answer.
//
// It is not named in the only-profile case. There is nothing to switch to, this
// block has already stated that profile's own credential state in the route
// above, and a table of one row the reader has just read about is a command that
// tells them what they know.
func (r profileRoutes) switchRoute(prose, command func(string), gap func(), invocation string) {
	if len(r.others) == 0 {
		prose("Or stop it answering at all. It is the only profile that file defines, so there is")
		prose("none to switch to: write another, or remove this one and let the chain fall through")
		prose("to discovering a sink from the cluster.")
		gap()
		command(r.commandName + " config set-profile <name>")
		command(r.commandName + " config delete-profile " + r.name + r.forceSuffix())
		return
	}

	prose(fmt.Sprintf("Or stop this one answering. The file also defines %s. Which of those has a",
		strings.Join(r.others, ", ")))
	prose("credential that resolves right now is the first command below; the second switches to it:")
	gap()
	command(r.commandName + " config get-profiles")
	if r.namedByFlag {
		command(fmt.Sprintf("%s … --%s %s", invocation, options.FlagProfile, r.others[0]))
		return
	}
	command(r.commandName + " config use-profile " + r.others[0])
}

// sinkPlaceholder is what the bypass route puts after --sink.
//
// A ClickHouse profile reads a ClickHouseSink, so half the placeholder is known
// and printing `<kind>` for it would ask the reader to supply something the
// message could have said. A profile whose backend is not one this build defines
// says nothing about which kind to name, and guessing there would be worse than
// the placeholder.
func (r profileRoutes) sinkPlaceholder() string {
	if r.backend == BackendClickHouse {
		return KindClickHouseSink + "/<name>"
	}
	return "<kind>/<name>"
}

// sinkResources is the `kubectl get` argument that lists the candidates, narrowed
// to the one kind when the profile's backend implies it.
func (r profileRoutes) sinkResources() string {
	if r.backend == BackendClickHouse {
		return "clickhousesinks"
	}
	return "clickhousesinks,s3sinks"
}

// addrSuffix carries a --sink-addr that was given through to the bypass route.
//
// The value is repeated rather than dropped because the reader passed it for a
// reason — a port they have already forwarded — and a suggested command that
// silently omitted it would send them back to the address they were avoiding.
func (r profileRoutes) addrSuffix() string {
	if r.sinkAddr == "" {
		return ""
	}
	return fmt.Sprintf(" --%s %s", options.FlagSinkAddr, r.sinkAddr)
}

// forceSuffix adds --force to the deletion when this profile is the active one,
// which is the case config delete-profile refuses without it.
func (r profileRoutes) forceSuffix() string {
	if !r.active {
		return ""
	}
	return " --" + options.FlagForce
}
