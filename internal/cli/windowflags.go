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
	"github.com/spf13/pflag"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
)

// The window a command is given, as flags.
//
// The parsing these names feed — the two grammars, and what an absent bound
// means — belongs to the options package, which every layer below the commands
// can reach. What stays here is only the registration: which flag set the four
// names go into, and how a command collapses them to one spelling before it
// reads them. That is per-command wiring, and it is the one thing a package
// below the command tree has no business knowing about (Task 11.8).
// windowFlags is the pair of bounds a command's window is given with, under both
// of the names each of them accepts.
//
// It is one type shared by `timeline`, `diff` and `scopes` rather than four flag
// registrations repeated in each of them. The vocabulary of a window is the part
// of this CLI a user carries between commands, and three copies of it would be
// three places for one spelling, one alias or one conflict rule to drift — which
// the user discovers as `--from` working on `timeline` and not on `scopes`.
//
// # Why there are two names
//
// --since/--until is the kubectl-adjacent spelling and stays the primary one: it
// is what a person types, and both ends read as "ago".
//
// --from/--to is the read plane's own spelling, and it is not invented here. It is
// what query.TimelineQuery calls these bounds, and — the half that reaches users —
// what the D19 structured envelope calls them: every coverage interval in `-o json`
// carries `from` and `to`. Somebody who has just written a `jq` expression over
// those fields and reaches for the matching flag should find it rather than a
// usage error, and a released CLI cannot add the spelling later without the
// asymmetry becoming permanent.
type windowFlags struct {
	// since and until hold the resolved bounds after resolve has run. Before that
	// they hold only what was given under those two names.
	since string
	until string

	// from and to hold what was given under the aliases, and are read exactly once
	// — by resolve, which collapses them onto the pair above so that no code past
	// the flag layer has to know there are two spellings.
	from string
	to   string
}

// addFlags registers all four names.
//
// The two descriptions are the caller's because the bound selects a different
// thing in each command — changes in `timeline` and `diff`, watch periods in
// `scopes` — and a shared string would have to be vague enough to cover both,
// which is how help text stops being read.
func (w *windowFlags) addFlags(flags *pflag.FlagSet, sinceHelp, untilHelp string) {
	flags.StringVar(&w.since, options.FlagSince, w.since, sinceHelp)
	flags.StringVar(&w.until, options.FlagUntil, w.until, untilHelp)
	flags.StringVar(&w.from, options.FlagFrom, w.from,
		"Alias for --"+options.FlagSince+", spelled as the structured output and the query contract spell it.")
	flags.StringVar(&w.to, options.FlagTo, w.to,
		"Alias for --"+options.FlagUntil+", spelled as the structured output and the query contract spell it.")
}

// resolve collapses the aliases onto --since/--until, and refuses one bound given
// twice with two different values.
//
// The conflict is a usage error rather than a last-one-wins, because the two
// readings of `--since 3d --from 6h` are a window of three days and a window of
// six, and a tool that silently picks one has answered a question the user did not
// ask — with a table that looks exactly like the one they wanted. Exit 2 says "you
// typed something this program does not accept", which is the code a wrapper
// script must not retry.
//
// Giving both names the *same* value is accepted rather than refused. It is
// harmless, it is what a generated command line does when a template fills in both
// spellings, and rejecting it would be pedantry with a non-zero exit code.
//
// The flag set is consulted for which names were given rather than the values
// being tested for emptiness: `--since ""` is a malformed value that options.ParseInstant
// explains well, and an emptiness test would silently replace it with --from
// instead.
func (w *windowFlags) resolve(flags *pflag.FlagSet) error {
	since, err := oneSpelling(flags, options.FlagSince, w.since, options.FlagFrom, w.from)
	if err != nil {
		return err
	}
	until, err := oneSpelling(flags, options.FlagUntil, w.until, options.FlagTo, w.to)
	if err != nil {
		return err
	}
	w.since, w.until = since, until
	return nil
}

// oneSpelling picks the value of one bound out of the two names it may arrive
// under.
func oneSpelling(flags *pflag.FlagSet, primary, primaryValue, alias, aliasValue string) (string, error) {
	switch {
	case !flags.Changed(alias):
		return primaryValue, nil
	case !flags.Changed(primary):
		return aliasValue, nil
	case primaryValue != aliasValue:
		return "", exit.UsageErrorf(
			"--%s and --%s are two names for the same bound, and they were given different values "+
				"(%q and %q); pass one of them", primary, alias, primaryValue, aliasValue)
	}
	return primaryValue, nil
}
