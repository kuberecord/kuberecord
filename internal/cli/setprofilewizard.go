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
	"bufio"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
)

// The prompting layer, and the word "layer" is the whole design (D33).
//
// Configuring a profile requires knowing flags a new user does not have. That is a
// discoverability problem, not a validation problem, and the two have to be kept
// apart: everything below asks questions and nothing below decides whether an
// answer is acceptable. Every value typed here is put into profileFields and
// judged by profileFields.validate, which is resolve.Profile.Validate — the same
// function the configuration file is read with and the same one the flags are
// checked by. A wizard with a validator of its own is a wizard that drifts into
// accepting what the file will later refuse, and the friendlier route would then
// be the one that writes a profile that does not load.
//
// # Why this is not the TUI v0.3.0 refused
//
// That decision rejected an interactive *data browser*: it does not screenshot
// into a thread, it cannot be piped, and it triples the surface a query has to be
// correct through. A setup wizard shares none of those. Nobody screenshots config
// setup, nobody pipes it, the surface is one subcommand, and what it produces is a
// file the flag path could equally have produced. `gh auth login` and `npm init`
// are the precedent: sequential prompts inside a tool that is otherwise entirely
// flag-driven.
//
// Sequential prompts on stdin is the whole mechanism. No framework, no alternate
// screen buffer, no raw mode, no terminal state to restore — so there is no state
// this file can leave behind if it stops halfway, which is what makes cancellation
// at any question cost nothing.
//
// # The first question is the one that matters
//
// It asks whether to read the settings from a sink the cluster already holds,
// because a wizard whose first question is "what is the address?" has not solved
// anything: not knowing the address is why the user is here. Answering yes reaches
// --from-sink without having had to know it exists.
//
// # There is no password prompt, and that is a property rather than an omission
//
// A profile never stores a password inline (see resolve.ClickHouseProfile.Password),
// so what is asked for is the *name* of an environment variable or the path of a
// file. There is nothing to suppress echo for, nothing secret in this process's
// memory, and nothing that could reach a scrollback buffer. The hardest part of a
// configuration wizard does not exist here because of a decision already made.

// errWizardCancelled ends the questions without writing anything.
//
// It is a sentinel rather than an exit code because cancelling is not a failure:
// the user was asked whether they wanted something and stopped, which run turns
// into one line and a zero exit. Nothing has been written by the time it can be
// returned — the write is the last thing that happens — so there is nothing to
// undo.
var errWizardCancelled = errors.New("cancelled at a prompt")

// setProfileWizard asks for one profile, one question at a time.
type setProfileWizard struct {
	// streams is where the questions go (stderr, with every other diagnostic) and
	// where the answers come from.
	streams genericiooptions.IOStreams

	// in reads answers a line at a time. It is buffered once, and must be: a new
	// reader per question would discard whatever the previous one had read past
	// the newline, which is how a wizard loses the answer to its own next
	// question.
	in *bufio.Reader

	// severity and colorize are this invocation's colour decision, resolved once
	// where the answer is known. severity paints the lines this file writes;
	// colorize is handed to resolve.SinkProfile.Explain, which renders its own.
	severity render.Severity
	colorize bool

	// invokedAs is how this process was invoked, so the equivalent command names
	// something the reader can actually type.
	invokedAs string

	// format is the structured document this invocation asked for, empty for the
	// human form. It is decided by the command before the first question, so a
	// rendering nobody can produce is refused before anybody is asked anything.
	format render.StructuredFormat

	// activate is --use, given on an invocation that named no field and therefore
	// reached the questions anyway.
	//
	// It answers the last question before it is asked rather than adding a flag
	// the prompting layer has to interpret: a reader who typed --use has said what
	// they want about the active pointer, and asking them again would be the
	// question whose answer is disregarded that askActivation exists to avoid.
	activate bool

	// newResolver builds the cluster access the first question needs.
	//
	// It is a function rather than a resolver so that a wizard whose first answer
	// is "no" never reads a kubeconfig at all, and so a test can hand in a
	// resolver over client-go's fakes.
	newResolver func() (*resolve.BackendResolver, error)
}

// runSetProfileWizard is `config set-profile` with no flags on a terminal.
//
// The terminal check is the first thing it does and it is a refusal, never a
// wait. A wizard that blocked in CI would be worse than no wizard: the pipeline
// hangs until something kills it, and the message explaining what was wanted
// never arrives. Exit 2 says the same thing a missing flag says, because that is
// what this is.
func runSetProfileWizard(
	cmd *cobra.Command, flags *options.GlobalFlags,
	streams genericiooptions.IOStreams, invokedAs, name string, format render.StructuredFormat,
	activate bool,
) error {
	if !options.IsTerminalIn(streams.In) {
		return errNoQuestionsToAsk(invokedAs)
	}

	colorize := options.ShouldColorize(flags.Color, streams.ErrOut)
	wizard := &setProfileWizard{
		streams:   streams,
		in:        bufio.NewReader(streams.In),
		severity:  render.NewSeverity(colorize),
		colorize:  colorize,
		invokedAs: invokedAs,
		format:    format,
		activate:  activate,
		newResolver: func() (*resolve.BackendResolver, error) {
			return resolve.NewBackendResolver(flags, streams, invokedAs)
		},
	}
	return wizard.run(cmd.Context(), name)
}

// errNoQuestionsToAsk is what a non-interactive invocation with no flags gets.
//
// It names both flag routes rather than one, because the two serve different
// people: a CI job writing a profile for a cluster it can see wants --from-sink,
// and one writing a profile for a backend no custom resource describes wants
// --backend and its fields. Naming only the first would send half of them to
// `--help` anyway.
func errNoQuestionsToAsk(invokedAs string) error {
	return exit.UsageErrorf("config set-profile was given no flags and standard input is not a "+
		"terminal, so there is nobody to ask: name the fields as flags instead — "+
		"`%[1]s config set-profile NAME --%[2]s <kind>/<name>` reads them out of a sink custom "+
		"resource, and `%[1]s config set-profile NAME --%[3]s <kind>` with that backend's flags "+
		"writes them by hand (`%[1]s config set-profile --help`)",
		commandNameOr(invokedAs), options.FlagFromSink, options.FlagBackend)
}

// run asks the questions and writes what they produced.
func (w *setProfileWizard) run(ctx context.Context, name string) error {
	err := w.gather(ctx, name)
	if errors.Is(err, errWizardCancelled) {
		// Said rather than left to silence, because a terminal that simply
		// returned to a prompt would leave a reader wondering whether the profile
		// was written. Zero, because nothing failed: this is the answer to a
		// question, not an error. A real SIGINT is a different thing and keeps the
		// interrupted exit code every other command gives it.
		return w.say("", "→ cancelled, and nothing was written")
	}
	return err
}

// gather runs the questions in order and performs the write.
func (w *setProfileWizard) gather(ctx context.Context, name string) error {
	if err := w.say("",
		"Writing a profile: where this command reads recorded history from.",
		"A profile never holds a password — it names an environment variable or a file.",
		"Ctrl-D at any question stops, and writes nothing.",
	); err != nil {
		return err
	}

	name, err := w.askName(ctx, name)
	if err != nil {
		return err
	}

	derived, err := w.askDiscovery(ctx)
	if err != nil {
		return err
	}
	if derived != nil {
		return w.writeDerived(ctx, name, derived)
	}
	return w.writeTyped(ctx, name)
}

// askName settles the profile's name, which may already have been an argument.
//
// The emptiness rule is requireProfileName, the same function the flag path
// applies, so the two routes cannot disagree about what an empty name means.
func (w *setProfileWizard) askName(ctx context.Context, name string) (string, error) {
	if name != "" {
		return name, nil
	}
	for {
		answer, err := w.prompt(ctx,
			"A name for this profile. It is how --profile and `config use-profile` address it.", "")
		if err != nil {
			return "", err
		}
		if nameErr := requireProfileName(answer); nameErr != nil {
			if sayErr := w.reject(nameErr); sayErr != nil {
				return "", sayErr
			}
			continue
		}
		return answer, nil
	}
}

// derivedProfile is what the discovery branch produced.
//
// The address is carried beside the profile rather than read back out of it,
// because the equivalent command has a rule of its own about when to print --addr
// and the profile itself cannot express it. See equivalentFromSink.
type derivedProfile struct {
	ref     resolve.SinkRef
	profile *resolve.SinkProfile
	addr    string

	// username is the ClickHouse user this profile reads as, carried only when it
	// is not the sink's own. Empty means the question was answered with the
	// offered default, which is what --from-sink writes with no --username at all.
	username string

	// passwordEnv and passwordFile are the answer to the password-source question,
	// carried for the same reason addr is: the equivalent command has to print the
	// flag that reproduces the stanza, and only one of the two is ever set.
	passwordEnv  string
	passwordFile string
}

// askDiscovery is the first question, and the reason this command is worth
// having.
//
// A nil profile with a nil error means "carry on and type it out". That is the
// answer to three different things — the user said no, the cluster holds no sinks,
// the listing could not be done — and the last two say so before falling through,
// because a wizard that quietly changed branch would have answered a question the
// user never heard the result of (Invariant 4). What it will not do is fall
// through after a sink was *chosen* and could not be read: that is a failure of
// something the user asked for by name, and it is reported as one.
//
// A Secret the sink names and this kubeconfig may not read is not that failure,
// and ProfileFromSink does not report it as one — see its file's "Why a Secret it
// cannot read is not a failure". The derivation is complete without it, so the
// only thing lost is a check, and askDerivedPassword turns that into one more
// question rather than into a discarded conversation.
func (w *setProfileWizard) askDiscovery(ctx context.Context) (*derivedProfile, error) {
	yes, err := w.askYesNo(ctx,
		"Read the settings from a sink custom resource in this cluster?", true)
	if err != nil || !yes {
		return nil, err
	}

	resolver, err := w.newResolver()
	if err != nil {
		return nil, w.declineDiscovery(err.Error())
	}
	refs, err := resolver.ListSinkRefs(ctx)
	if err != nil {
		// Already classified by discovery: a forbidden list names the permission
		// and the routes that do not need it, a missing CRD is not an error at
		// all. Repeating it here would be a second opinion about one failure.
		return nil, w.declineDiscovery(err.Error())
	}
	if len(refs) == 0 {
		return nil, w.declineDiscovery("this cluster holds no sink custom resources")
	}

	choices := make([]wizardChoice, 0, len(refs))
	for _, ref := range refs {
		choices = append(choices, wizardChoice{value: ref.String(), description: sinkKindGloss(ref.Kind)})
	}
	answer, err := w.menu(ctx, "Which sink should this profile read from?", choices, true)
	if err != nil {
		return nil, err
	}
	ref := refs[indexOfChoice(choices, answer)]

	// The same call --from-sink makes, with the same overrides type. Nothing about
	// how a sink becomes a profile changes according to whether a flag or a
	// question chose it.
	profile, err := resolver.ProfileFromSink(ctx, ref, resolve.ProfileOverrides{})
	if err != nil {
		return nil, err
	}
	return w.askDerivedAddr(ctx, resolver, ref, profile)
}

// askDerivedAddr confirms the endpoint a derived ClickHouse profile will record.
//
// The address is the one field that must differ from the custom resource — a
// Service DNS name copied into a profile unchanged produces a profile that fails
// exactly as discovery did — so it is the one thing worth a question on a branch
// whose value is that nothing has to be known. The offered default is what
// --from-sink would have written unprompted, so pressing return is that flag
// exactly.
//
// An object store has no dialled endpoint, no user and no password, which is why
// ProfileFromSink refuses every override for one. There is nothing to ask.
func (w *setProfileWizard) askDerivedAddr(
	ctx context.Context, resolver *resolve.BackendResolver, ref resolve.SinkRef, profile *resolve.SinkProfile,
) (*derivedProfile, error) {
	stanza := profile.Profile.ClickHouse
	if stanza == nil {
		return &derivedProfile{ref: ref, profile: profile}, nil
	}

	// The recorded address gets a line to itself, unwrapped, for the reason the
	// unreachable-backend message gives it one: it is the string a reader may need
	// to compare character by character with what they have in a manifest.
	said := []string{"", fmt.Sprintf("%s records %s.", ref, profile.RecordedAddr)}
	if profile.AddrRewritten {
		said = append(said, "That name resolves inside the cluster and nowhere else.")
	}
	if err := w.say(said...); err != nil {
		return nil, err
	}

	field := profileFieldByName(options.FlagAddr)
	answer, err := w.askString(ctx, field, &profileFields{
		Backend: string(resolve.BackendClickHouse), Addr: stanza.Addr,
	}, stanza.Addr)
	if err != nil {
		return nil, err
	}

	derived := &derivedProfile{ref: ref, profile: profile}
	// --addr is printed when the profile records something other than what the
	// sink records, which is the condition under which the flag is load-bearing.
	// Deriving it from the *recorded* address rather than from whether the user
	// typed anything is what makes the printed command reproduce this profile
	// without depending on the cluster-internal classifier agreeing next time.
	if answer != profile.RecordedAddr {
		derived.addr = answer
	}
	if err := w.askDerivedCredential(ctx, derived); err != nil {
		return nil, err
	}

	// Re-derived rather than patched, so that the paragraph printed after the
	// write describes the profile that was written: SinkProfile.Explain says "as
	// --addr asked" from AddrOverridden, and a stanza edited behind its back would
	// have the flag's effect without the flag's explanation. Once, with every
	// override the questions produced, because two derivations would each discard
	// the other's.
	over := resolve.ProfileOverrides{
		Username:    derived.username,
		PasswordEnv: derived.passwordEnv, PasswordFile: derived.passwordFile,
	}
	if answer != stanza.Addr {
		over.Addr = answer
	}
	if over == (resolve.ProfileOverrides{}) {
		return derived, nil
	}
	derived.profile, err = resolver.ProfileFromSink(ctx, ref, over)
	if err != nil {
		return nil, err
	}
	return derived, nil
}

// askDerivedCredential asks which ClickHouse user this profile reads as, and
// where that user's password comes from.
//
// # Why it is one function and two adjacent questions
//
// A ClickHouse username and a password are one credential pair (D37). This branch
// used to ask neither: it took the user from the custom resource and the variable
// from resolve.DefaultPasswordEnv, and then printed advice telling the reader to
// export a *read-only* user's password into that variable — beside a stanza
// naming the sink's own writer. Following both is an authentication failure, and
// the moment the CLI is about to recommend a different principal is the moment it
// has to ask which principal, rather than recommend one and record another.
//
// So the two questions are adjacent and the second names the answer to the first.
// Nothing is required by them: the offered default is the sink's own user, the
// variable offered after it is what the derivation would have written for that
// user, and pressing return through both writes exactly the stanza this branch
// wrote before the questions existed.
//
// # The Secret nobody could read is a preamble, not a gate
//
// It used to be the condition on the whole function. That was Task 17.1's shape,
// and it is the read-only engineer's path: the operator's aggregated ClusterRole
// reads Secrets in its own namespace and most people have less than that (D7), so
// the derivation is deliberately complete without it (resolve/fromsink.go's "Why a
// Secret it cannot read is not a failure"). What it explains is why the questions
// below are not simply answered from the cluster, which is worth a sentence
// whenever it is true and is not a reason to ask nothing when it is false.
//
// CredentialUnreadable is the fact it reads, and it is empty for a Secret that was
// read and found to hold no password key — that says nothing about this reader's
// permissions. It is a broken sink, which the operator reports on its own status
// and Explain names the present keys for.
//
// The notice is plain prose at full weight rather than a tier. It is not a Warning:
// nothing here misleads, and the sentence exists precisely to stop the reader
// concluding that something broke. It is not Provenance either, since it varies
// between invocations and has to be read once rather than kept available. And the
// emphasis in this block belongs to the equivalent command at the end (D27, D30).
func (w *setProfileWizard) askDerivedCredential(ctx context.Context, derived *derivedProfile) error {
	if unreadable := derived.profile.CredentialUnreadable; unreadable != "" {
		if err := w.say("",
			fmt.Sprintf("Read the connection settings from %s.", derived.ref),
			fmt.Sprintf("Cannot read its Secret (%s) — that is fine: a profile stores where", unreadable),
			"your password lives, not the operator's.",
		); err != nil {
			return err
		}
	}

	// A profileFields carrying what the sink already answered, so both questions
	// are refused and accepted by resolve.Profile.Validate rather than by a rule
	// this branch invented (D33).
	stanza := derived.profile.Profile.ClickHouse
	sinkUsername := stanza.Username
	fields := &profileFields{
		Backend:  string(resolve.BackendClickHouse),
		Addr:     stanza.Addr,
		Database: stanza.Database,
		Username: sinkUsername,
		TLS:      stanza.TLS,
	}

	// The consequence, above the question rather than inside it, for the reason
	// askDerivedAddr puts the recorded address above --addr's: it is a fact about
	// this cluster's sink, and the question itself is the one sentence describing
	// --username that `--help` and the typed path also read.
	//
	// It names the user and what that user can do, and not the database beside it.
	// Explain says "Database X and user Y are the sink's own" after the write, and
	// printing the same sentence twice in one transcript would make the second
	// reading look like a second fact.
	if err := w.say("",
		fmt.Sprintf("%s authenticates as %s, which can write to the", derived.ref, sinkUsername),
		"audit trail.",
	); err != nil {
		return err
	}
	username, err := w.askString(ctx, profileFieldByName(options.FlagUsername), fields, sinkUsername)
	if err != nil {
		return err
	}
	if username != sinkUsername {
		derived.username = username
	}

	if err := w.askPassword(ctx, fields, resolve.ReaderPasswordEnv(username, sinkUsername), false); err != nil {
		return err
	}
	derived.passwordEnv, derived.passwordFile = fields.PasswordEnv, fields.PasswordFile
	return nil
}

// declineDiscovery says why the first question had nothing to offer, and moves on.
func (w *setProfileWizard) declineDiscovery(reason string) error {
	return w.say("",
		w.severity.Warning(render.WarningMarker+" "+reason),
		"  Carrying on with the settings typed out by hand.")
}

// writeDerived writes a profile the cluster described, and prints the flags that
// would have written it.
func (w *setProfileWizard) writeDerived(ctx context.Context, name string, derived *derivedProfile) error {
	activate, err := w.askActivation(ctx, name)
	if err != nil {
		return err
	}
	activated, err := writeProfile(profileWrite{
		name:        name,
		profile:     derived.profile.Profile,
		explanation: derived.profile.Explain(w.colorize),
		activate:    activate,
		nextStep:    true,
		invokedAs:   w.invokedAs,
		severity:    w.severity,
		format:      w.format,
	}, w.streams)
	if err != nil {
		return err
	}
	return w.sayEquivalent(w.equivalentFromSink(name, derived), activated)
}

// equivalentFromSink is the flag command for the discovery branch.
//
// Every override the questions produced is printed, and only those: the flags
// below are exactly what askDerivedAddr handed to ProfileFromSink, so the command
// re-run reaches the same derivation with the same inputs and writes the same
// stanza. They are printed in profileFieldFlags order, which is `--help`'s and
// docs/CLI.md's, so a line lifted out of a transcript reads like a line somebody
// wrote.
//
// --username appears when the profile reads as somebody other than the sink's
// user, which is the condition under which the flag is load-bearing: derived from
// the *value* rather than from whether the question was answered, exactly as
// --addr is, so that pressing return prints nothing and typing the offered default
// back does not print a flag that changes nothing.
//
// The password reference is printed whenever there is one, which on this branch is
// always. That is deliberately not the value rule the two flags above follow: what
// it pins is the variable resolve.ReaderPasswordEnv chose, and a line that omitted
// it would reproduce this profile only for as long as that function keeps
// answering the same way. The same reasoning --addr's rule is written with — a
// printed command must not depend on a classifier agreeing next time.
func (w *setProfileWizard) equivalentFromSink(name string, derived *derivedProfile) []string {
	parts := []string{
		commandNameOr(w.invokedAs), "config", "set-profile", shellArg(name),
		"--" + options.FlagFromSink, derived.ref.String(),
	}
	if derived.addr != "" {
		parts = append(parts, "--"+options.FlagAddr, shellArg(derived.addr))
	}
	if derived.username != "" {
		parts = append(parts, "--"+options.FlagUsername, shellArg(derived.username))
	}
	switch {
	case derived.passwordEnv != "":
		parts = append(parts, "--"+options.FlagPasswordEnv, shellArg(derived.passwordEnv))
	case derived.passwordFile != "":
		parts = append(parts, "--"+options.FlagPasswordFile, shellArg(derived.passwordFile))
	}
	return parts
}

// writeTyped asks for a backend and its fields, then writes what they describe.
func (w *setProfileWizard) writeTyped(ctx context.Context, name string) error {
	fields, err := w.askFields(ctx)
	if err != nil {
		return err
	}
	profile, err := fields.stanza()
	if err != nil {
		return err
	}
	// After the fields and before the write, which is what makes it the last
	// question on both branches: it is the only one whose subject is the file
	// rather than the profile, and a question about what to do with a stanza has
	// to come after the stanza exists.
	activate, err := w.askActivation(ctx, name)
	if err != nil {
		return err
	}
	activated, err := writeProfile(profileWrite{
		name: name, profile: profile, activate: activate, nextStep: true,
		invokedAs: w.invokedAs, severity: w.severity, format: w.format,
	}, w.streams)
	if err != nil {
		return err
	}
	return w.sayEquivalent(fields.equivalent(w.invokedAs, name), activated)
}

// askActivation is the last question: whether this profile should be the one that
// answers from here on.
//
// It defaults to no, and that default is the decision rather than a preference
// (D38). The active profile is a side effect on every later command in the shell,
// so a wizard that switched by default would redirect `timeline`, `diff` and `get`
// to a store somebody wrote in order to inspect it — D24's objection at the config
// layer, and not something `kubectl config set-context` does either. One keystroke
// is the whole cost of saying yes.
//
// Two states are not asked about at all, and activationIsADecision is where the
// rule lives so that the answer and the write cannot disagree about it: a first
// profile in an empty file is activated regardless, and a profile that already
// answers cannot be made to answer more. Asking either would be offering a choice
// that is disregarded whichever way it is given (D31).
//
// --use answers it before it is asked. It is not re-asked and not confirmed:
// writeProfile reports the activation, so what the flag did is on the screen
// without a question having been spent on it.
func (w *setProfileWizard) askActivation(ctx context.Context, name string) (bool, error) {
	if w.activate {
		return true, nil
	}

	// Read here rather than carried in from the command, because the file may have
	// been written by something else during the conversation and because this is
	// the same read the write is about to do. A failure is returned rather than
	// swallowed: the write would fail on it a moment later, and a question asked
	// against a file that cannot be read is a question asked for nothing.
	path, err := resolve.DefaultConfigPath()
	if err != nil {
		return false, exit.RuntimeErrorf("%w", err)
	}
	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		return false, exit.RuntimeErrorf("%w", err)
	}
	if !activationIsADecision(cfg, name) {
		return false, nil
	}
	return w.askYesNo(ctx, "Make this the active profile?", false)
}

// askFields asks for a backend and then for that backend's fields, in table
// order.
//
// Table order is the order `--help` lists the flags in, and it puts each backend's
// required field first. That is what makes validating after every answer sound:
// the profile built so far was accepted with every earlier field in it, so the
// first complaint the validator has can only be about the field just typed. A
// reordering that moved --addr below --database would break the attribution
// silently, which is why the table says so where it is defined.
func (w *setProfileWizard) askFields(ctx context.Context) (profileFields, error) {
	var fields profileFields

	backend, err := w.askBackend(ctx, &fields)
	if err != nil {
		return fields, err
	}

	for _, field := range profileFieldFlags {
		// The two password references are the one place a question does not map
		// to a field. They are mutually exclusive and the validator refuses both,
		// so asking for each in turn would walk a user into a state the tool then
		// takes back; asking where the password comes from makes that state
		// unreachable instead. It happens here, at --password-env's position in
		// the table, so the question keeps its place in the order.
		if field.name == options.FlagPasswordEnv && backend == resolve.BackendClickHouse {
			// resolve.DefaultPasswordEnv rather than a per-user variable, because
			// there is no sink's user here to read as somebody other than: a
			// hand-written stanza names one principal and nothing is being
			// replaced. It is also the variable docs/CLI.md's worked example
			// exports beside this exact command.
			if err := w.askPassword(ctx, &fields, resolve.DefaultPasswordEnv, true); err != nil {
				return fields, err
			}
			continue
		}
		if !field.asks(backend) {
			continue
		}
		if err := w.askField(ctx, field, &fields); err != nil {
			return fields, err
		}
	}
	return fields, nil
}

// askBackend chooses which stanza this profile carries.
//
// It is the one field validated by stanza() rather than by validate(): a backend
// on its own is never a complete profile — a ClickHouse one still needs an address
// — so the full validator would refuse every correct answer. stanza() is the half
// that has an opinion about the backend itself, and it is the same half the flag
// path is refused by, message included.
func (w *setProfileWizard) askBackend(ctx context.Context, fields *profileFields) (resolve.BackendKind, error) {
	field := profileFieldByName(options.FlagBackend)

	choices := make([]wizardChoice, 0, len(resolve.BackendKinds))
	for _, kind := range resolve.BackendKinds {
		choices = append(choices, wizardChoice{value: string(kind), description: backendDescriptions[kind]})
	}

	for {
		// Not strict: an unrecognised word is handed to stanza(), so that
		// `postgres` is refused by the sentence `--backend postgres` is refused
		// by, rather than by a menu telling the reader to pick a number.
		answer, err := w.menu(ctx, field.usage, choices, false)
		if err != nil {
			return "", err
		}
		candidate := *fields
		candidate.Backend = answer
		if _, stanzaErr := candidate.stanza(); stanzaErr != nil {
			if sayErr := w.reject(stanzaErr); sayErr != nil {
				return "", sayErr
			}
			continue
		}
		*fields = candidate
		return resolve.BackendKind(answer), nil
	}
}

// askPassword asks where a ClickHouse profile's password comes from.
//
// None of the answers is the password. The value asked for afterwards is the
// *name* of an environment variable or the path of a file, so nothing secret is
// typed, echoed or held — see this file's opening comment.
//
// The question names the user whose password it is asking about, because a
// username and a password are one credential pair and this is the half that says
// so out loud (D37). Both routes reach it with the user already answered —
// --username sits immediately above --password-env in profileFieldFlags, and the
// derived branch asks the two together — so the name is available on both and the
// question is one question rather than two spellings of one. It falls back to "the
// ClickHouse password" for a profile that names no user at all, which is a server
// whose default user is the one being authenticated as.
//
// defaultEnv is what pressing return at the variable name writes. It is passed in
// rather than read from a constant so that the value offered is the one the
// derivation would have produced for the user just chosen (resolve.ReaderPasswordEnv),
// which is what makes accepting every default reproduce --from-sink exactly.
//
// offerNone adds "nowhere — a server with no password", and the derived branch
// does not ask for it. resolve.ProfileOverrides has no way to express *no
// reference at all*: empty overrides are what --from-sink is given when nobody
// names a password, and the derivation answers them with an environment variable
// on purpose (see resolve.clickHouseProfile). So an answer of "none" there would
// produce that variable anyway — a choice offered, taken, and silently
// disregarded, which is the exact shape D31 is about. A ClickHouseSink's
// credentialsSecretRef is a required field, so the sink whose settings are being
// copied authenticates with a password; the answer is not one this branch has to
// have.
func (w *setProfileWizard) askPassword(
	ctx context.Context, fields *profileFields, defaultEnv string, offerNone bool,
) error {
	choices := []wizardChoice{
		{value: "environment", description: "an environment variable, named next"},
		{value: "file", description: "a file, named next"},
	}
	if offerNone {
		choices = append(choices,
			wizardChoice{value: "none", description: "nowhere — a server with no password"})
	}
	whose := "the ClickHouse"
	if fields.Username != "" {
		whose = fields.Username + "'s"
	}
	answer, err := w.menu(ctx, fmt.Sprintf("Where does %s password come from?", whose), choices, true)
	if err != nil {
		return err
	}

	switch answer {
	case "environment":
		// Offered as the default because the user has just said they want one. It
		// is a suggestion the equivalent command prints in full, so the flag line
		// reproduces the profile without relying on it.
		_, err = w.askString(ctx, profileFieldByName(options.FlagPasswordEnv), fields, defaultEnv)
	case "file":
		_, err = w.askString(ctx, profileFieldByName(options.FlagPasswordFile), fields, "")
	}
	return err
}

// askField asks for one field of the chosen backend.
func (w *setProfileWizard) askField(ctx context.Context, field profileField, fields *profileFields) error {
	if field.boolean != nil {
		// Not validated, and there is nothing to validate: no clause of
		// resolve.Profile.Validate has an opinion about a bool, and both of these
		// default to false exactly as their flags do when they are not passed.
		answer, err := w.askYesNo(ctx, field.usage, false)
		if err != nil {
			return err
		}
		*field.boolean(fields) = answer
		return nil
	}
	_, err := w.askString(ctx, field, fields, "")
	return err
}

// askString asks for one string field, re-asking while the validator refuses it.
//
// The candidate is a copy of the answers so far with this field replaced, so what
// is judged is a whole profile and the judge is resolve.Profile.Validate. Nothing
// in this function knows what any field means.
func (w *setProfileWizard) askString(
	ctx context.Context, field profileField, fields *profileFields, def string,
) (string, error) {
	for {
		answer, err := w.prompt(ctx, field.usage, def)
		if err != nil {
			return "", err
		}
		candidate := *fields
		*field.str(&candidate) = answer
		if validateErr := candidate.validate(); validateErr != nil {
			if sayErr := w.reject(validateErr); sayErr != nil {
				return "", sayErr
			}
			continue
		}
		*fields = candidate
		return answer, nil
	}
}

// askYesNo asks a question whose answers are yes and no.
func (w *setProfileWizard) askYesNo(ctx context.Context, question string, def bool) (bool, error) {
	shown := "y/N"
	if def {
		shown = "Y/n"
	}
	for {
		answer, err := w.prompt(ctx, question+" ["+shown+"]", "")
		if err != nil {
			return false, err
		}
		switch strings.ToLower(answer) {
		case "":
			return def, nil
		case "y", "yes":
			return true, nil
		case "n", "no":
			return false, nil
		}
		if sayErr := w.say("", w.severity.Warning(
			render.WarningMarker+" answer yes or no, or press return for the default")); sayErr != nil {
			return false, sayErr
		}
	}
}

// wizardChoice is one entry of a menu: the value an answer stands for, and what
// it means.
type wizardChoice struct {
	value       string
	description string
}

// menu asks a question with a listed set of answers.
//
// A number or the value itself is accepted, because the value is what the
// equivalent command prints and somebody who has read it in a menu should not
// have to learn a second spelling for it.
//
// strict decides what an unrecognised answer means, and the two cases are
// genuinely different. A menu whose set this layer owns — which sink, where a
// password comes from — has nothing underneath it with an opinion, so it re-asks
// here. --backend has a validator, and handing the answer back unrecognised is
// what lets the reader be refused by the sentence the flag would have refused
// them with.
func (w *setProfileWizard) menu(
	ctx context.Context, question string, choices []wizardChoice, strict bool,
) (string, error) {
	lines := make([]string, 0, len(choices)+1)
	lines = append(lines, question)
	for i, choice := range choices {
		line := fmt.Sprintf("  %d) %s", i+1, choice.value)
		if choice.description != "" {
			line += " — " + choice.description
		}
		lines = append(lines, line)
	}

	for {
		if err := w.say(append([]string{""}, lines...)...); err != nil {
			return "", err
		}
		answer, err := w.promptBare(ctx, choices[0].value)
		if err != nil {
			return "", err
		}

		if number, convErr := strconv.Atoi(answer); convErr == nil {
			if number < 1 || number > len(choices) {
				if sayErr := w.reject(fmt.Errorf("there is no choice %d: the numbers are 1 to %d",
					number, len(choices))); sayErr != nil {
					return "", sayErr
				}
				continue
			}
			return choices[number-1].value, nil
		}
		if index := indexOfChoice(choices, answer); index >= 0 {
			return choices[index].value, nil
		}
		if !strict {
			return answer, nil
		}
		if sayErr := w.reject(fmt.Errorf("%q is not one of the choices: answer with a number from 1 "+
			"to %d, or with the value itself", answer, len(choices))); sayErr != nil {
			return "", sayErr
		}
	}
}

// indexOfChoice finds a menu entry by value, matched the way a sink kind is
// matched: without regard to case, because these are values a person is retyping
// from a line above.
func indexOfChoice(choices []wizardChoice, value string) int {
	for i, choice := range choices {
		if strings.EqualFold(choice.value, value) {
			return i
		}
	}
	return -1
}

// sinkKindGloss says what reading through a sink of this kind gets you.
func sinkKindGloss(kind string) string {
	switch kind {
	case resolve.KindClickHouseSink:
		return backendDescriptions[resolve.BackendClickHouse]
	case resolve.KindS3Sink:
		return backendDescriptions[resolve.BackendS3]
	}
	return ""
}

// prompt puts one question on stderr and reads its answer.
func (w *setProfileWizard) prompt(ctx context.Context, question, def string) (string, error) {
	if err := w.say("", question); err != nil {
		return "", err
	}
	return w.promptBare(ctx, def)
}

// promptBare reads an answer with no question above it, for the menus that have
// already printed one.
//
// An empty answer is the default, which is how a default is offered at all: there
// is no line editing here and nothing pre-filled on the input line, so the only
// way to accept a suggestion is to press return.
//
// EOF with nothing typed is a cancellation. It is Ctrl-D, or a stdin that has gone
// away, and both mean there is nobody left to ask — so the questions stop rather
// than looping on an input that will never produce another line. Cancellation
// after a signal is the same, checked after the read because a blocked read does
// not return on SIGINT: the interrupt is noticed at the next answer, and nothing
// has been written by then either.
func (w *setProfileWizard) promptBare(ctx context.Context, def string) (string, error) {
	marker := "> "
	if def != "" {
		marker = fmt.Sprintf("> [%s] ", def)
	}
	if err := options.WriteAll(w.streams.ErrOut, marker); err != nil {
		return "", err
	}

	line, err := w.in.ReadString('\n')
	if err != nil && line == "" {
		return "", errWizardCancelled
	}
	if ctx.Err() != nil {
		return "", errWizardCancelled
	}
	if answer := strings.TrimSpace(line); answer != "" {
		return answer, nil
	}
	return def, nil
}

// reject prints why an answer was refused, in the validator's own words.
//
// The Warning tier, because this is a line the reader must not skim past: it is
// the whole reason they are being asked the same question twice. The marker is
// what carries that under NO_COLOR and in a redirected stream.
func (w *setProfileWizard) reject(err error) error {
	return w.say("", w.severity.Warning(render.WarningMarker+" "+err.Error()))
}

// say writes lines to stderr, which is where every question and every notice
// belongs.
//
// Nothing this file writes goes to stdout. `config set-profile` produces a file
// rather than a document, and a wizard that put its questions on stdout would
// make the one stream a caller might capture into a conversation with itself.
func (w *setProfileWizard) say(lines ...string) error {
	for _, line := range lines {
		if err := options.WriteLine(w.streams.ErrOut, line); err != nil {
			return err
		}
	}
	return nil
}

// sayEquivalent prints the flag command that would have produced this profile
// without the questions.
//
// It is what makes the feature worth its cost. The questions are for somebody who
// does not know the flags; this is what they are holding afterwards — the line to
// paste into a bug report, the line to lift into a CI job, and the reason a second
// profile does not need a second conversation. A wizard that never showed its
// equivalent would train people to need it forever.
//
// The command is Emphasis, and it is the only emphasised line in the block, for
// the reason the tier is defined with: it is the one line a reader skimming past
// the write confirmation has to come away with.
//
// --use is appended when the write activated the profile, so the line reproduces
// the whole outcome and not only the stanza. It is derived from what happened
// rather than from what was answered, exactly as --addr and --username are: a
// profile activated because it was the only one in the file is a profile the
// printed command has to activate on a machine whose file is not empty, or the
// line would be a claim that stopped being true the moment somebody wrote a second
// profile.
func (w *setProfileWizard) sayEquivalent(parts []string, activated bool) error {
	if activated {
		parts = append(parts, "--"+options.FlagUse)
	}
	return w.say("", "The same thing without the questions:",
		w.severity.Emphasis("  "+strings.Join(parts, " ")))
}

// profileFieldByName returns the table entry for a flag, and panics for one that
// is not there.
//
// It panics for the reason mustCompleteFlag does: the argument is a compiled-in
// constant naming a row of a compiled-in table, so a miss is a programming error
// with no user-facing repair and no dependence on input, an environment or a
// cluster. Every test in this package builds the command tree and several drive
// the wizard, so the first `go test` after such a mistake is where it surfaces.
func profileFieldByName(name string) profileField {
	for _, field := range profileFieldFlags {
		if field.name == name {
			return field
		}
	}
	panic(fmt.Sprintf("no profile field named %q in profileFieldFlags", name))
}
