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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
)

// The routes past a failing profile are tested from inside the package for the
// same reason the dial diagnostic is: the two things worth asserting are the
// classification, which is a private decision about which failures the block
// explains, and the block itself, which is assembled from a struct no caller
// sees.
//
// The guarding half is here too, and it is the half this task is most likely to
// lose. Two of these cases assert that the chain still behaves exactly as it did
// — the profile step is still fatal (D35) and --sink-addr still replaces one
// field (D36) — because the "obvious fix" for the failure this file explains is
// to make either of those untrue, and the message would then be explaining
// something that no longer happens.

// The fixture profile. The environment variable is DefaultPasswordEnv rather
// than a name invented here, because it is the variable every other route
// suggests and the one docs/CLI.md exports: a message telling a reader to export
// one name while the wizard writes another would be two suggestions.
const (
	routesName       = "prod"
	routesConfigPath = "/home/engineer/.config/kuberecord/config.yaml"
	routesAddr       = "clickhouse.example:9000"

	// routesPasswordFile is a path that does not exist and is written as a
	// literal so that the rendered block is the same on every machine. A run
	// where it *does* exist fails in profileFailure, saying so.
	routesPasswordFile = "/var/lib/kuberecord/clickhouse-password"

	// plantedPassword is the value that must never reach a rendered message. It
	// is distinctive so a substring search for it is searching for something.
	plantedPassword = "correct-horse-battery-staple"
)

// clickHouseStanza is the fixture profile with one field changed per case.
func clickHouseStanza(mutate func(*ClickHouseProfile)) Profile {
	stanza := &ClickHouseProfile{
		Addr: routesAddr, Database: DefaultClickHouseDatabase, Username: "kuberecord_ro",
	}
	if mutate != nil {
		mutate(stanza)
	}
	return Profile{Backend: BackendClickHouse, ClickHouse: stanza}
}

// routesConfig is a configuration file holding the fixture profile and the
// others named.
func routesConfig(profile Profile, others ...string) *Config {
	cfg := &Config{
		CurrentProfile: routesName,
		Profiles:       map[string]Profile{routesName: profile},
	}
	for _, other := range others {
		cfg.Profiles[other] = Profile{Backend: BackendLocal, Local: &LocalProfile{Path: "/archives/" + other}}
	}
	return cfg
}

// profileFailure resolves the fixture profile and returns the failure, which must
// be one carrying routes.
//
// It goes through targetFromProfile and explainProfile rather than constructing
// the error, so that a case describes a stanza and a command line and the test
// asserts what the resolution chain actually produces from them.
func profileFailure(t *testing.T, cfg *Config, flags *options.GlobalFlags) *UnresolvableProfileError {
	t.Helper()

	resolver := &BackendResolver{
		Flags:      flags,
		InvokedAs:  options.StandaloneName,
		Config:     cfg,
		ConfigPath: routesConfigPath,
	}
	profile := cfg.Profiles[routesName]
	_, err := targetFromProfile(routesName, profile, resolver.sinkAddr())
	if err == nil {
		t.Fatalf("profile %q resolved; this case has nothing to explain", routesName)
	}

	explained := resolver.explainProfile(routesName, profile, err)
	var carrying *UnresolvableProfileError
	if !errors.As(explained, &carrying) {
		t.Fatalf("the failure carries no routes past it: %v", explained)
	}
	return carrying
}

// unsetPasswordEnv guarantees the fixture's variable is absent for the duration
// of a test, and restored after it.
//
// t.Setenv is what registers the restoration; the unset that follows is the state
// the cases need. A profile naming a variable the shell exported anyway would
// resolve, and every case here would silently stop testing what it says it does
// — which is why this is a helper rather than two lines repeated.
func unsetPasswordEnv(t *testing.T) {
	t.Helper()
	t.Setenv(DefaultPasswordEnv, "")
	if err := os.Unsetenv(DefaultPasswordEnv); err != nil {
		t.Fatalf("unsetting %s: %v", DefaultPasswordEnv, err)
	}
}

// TestEveryProfileFailureNamesTheRoutesPastIt is the acceptance criterion's
// "every failure, not only the unset variable".
//
// The four are the whole of what the profile step can raise about a stanza, and
// each is checked for the route that is specific to it as well as for the two
// that are not. A case that named only the shared routes would pass with the
// specific one deleted, which is the route the reader in that case actually
// needs.
func TestEveryProfileFailureNamesTheRoutesPastIt(t *testing.T) {
	missingFile := filepath.Join(t.TempDir(), "absent")
	unreadableFile := t.TempDir() // A directory is unreadable as a file for root too.

	for _, tc := range []struct {
		name    string
		profile Profile
		setup   func(t *testing.T)
		want    []string
	}{
		{
			name:    "an environment variable the shell never exported",
			profile: clickHouseStanza(func(p *ClickHouseProfile) { p.PasswordEnv = DefaultPasswordEnv }),
			setup:   func(t *testing.T) { t.Helper(); unsetPasswordEnv(t) },
			want:    []string{"export " + DefaultPasswordEnv + "=…"},
		},
		{
			name:    "a password file that is not there",
			profile: clickHouseStanza(func(p *ClickHouseProfile) { p.PasswordFile = missingFile }),
			want:    []string{missingFile, "config set-profile " + routesName},
		},
		{
			name:    "a password file that cannot be read",
			profile: clickHouseStanza(func(p *ClickHouseProfile) { p.PasswordFile = unreadableFile }),
			want:    []string{unreadableFile, "config set-profile " + routesName},
		},
		{
			name:    "a backend this build does not define",
			profile: Profile{Backend: BackendKind("postgres")},
			want:    []string{"config set-profile " + routesName, "--" + options.FlagSink + " <kind>/<name>"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setup != nil {
				tc.setup(t)
			}
			failure := profileFailure(t, routesConfig(tc.profile, "archive"), &options.GlobalFlags{})
			rendered := failure.Render("kuberecord timeline", false)

			// The three routes, and the command that would have shown this step
			// failing before a query was ever run.
			want := append([]string{
				"--" + options.FlagSink,
				"config use-profile archive",
				"config resolve",
			}, tc.want...)
			for _, fragment := range want {
				if !strings.Contains(rendered, fragment) {
					t.Errorf("the routes do not name %q:\n%s", fragment, rendered)
				}
			}
		})
	}
}

// TestTheOneLineFailureIsUnchanged.
//
// The block is an addition to what a reader sees and not a replacement for it:
// the `error:` line, the chain step's recorded detail and every caller that wraps
// the failure with context all still read the sentence the cause produced. A
// wrapper that prefixed it would say "profile" twice in one line, and the exit
// code has to survive the wrapping or a wrapper script starts retrying a
// configuration mistake.
func TestTheOneLineFailureIsUnchanged(t *testing.T) {
	unsetPasswordEnv(t)
	profile := clickHouseStanza(func(p *ClickHouseProfile) { p.PasswordEnv = DefaultPasswordEnv })

	failure := profileFailure(t, routesConfig(profile), &options.GlobalFlags{})

	for _, want := range []string{routesName, DefaultPasswordEnv, "is not set"} {
		if !strings.Contains(failure.Error(), want) {
			t.Errorf("the one-line failure no longer names %q: %v", want, failure)
		}
	}
	if strings.Contains(failure.Error(), "\n") {
		t.Errorf("the one-line failure is more than one line:\n%v", failure)
	}
	if code := exit.CodeFor(failure); code != exit.RuntimeError {
		t.Errorf("exit code %d, want %d: explaining a failure must not reclassify it",
			code, exit.RuntimeError)
	}
}

// TestTheSinkAddrParagraphAppearsOnlyWhenTheFlagWasGiven.
//
// It is the sentence for a reader who has already tried the flag and been refused
// for a reason its name does not suggest. Printing it to somebody who never
// passed it would be explaining a flag they have not met, in a block that is
// already four paragraphs long.
func TestTheSinkAddrParagraphAppearsOnlyWhenTheFlagWasGiven(t *testing.T) {
	unsetPasswordEnv(t)
	profile := clickHouseStanza(func(p *ClickHouseProfile) { p.PasswordEnv = DefaultPasswordEnv })

	t.Run("given", func(t *testing.T) {
		failure := profileFailure(t, routesConfig(profile),
			&options.GlobalFlags{SinkAddr: forwardedAddr})
		rendered := failure.Render("kuberecord timeline", false)

		for _, want := range []string{
			// The bypass route carries the forwarded port through, because the
			// reader passed it for a reason and a suggestion without it would send
			// them back to the address they were avoiding.
			"--" + options.FlagSink + " " + KindClickHouseSink + "/<name> --" +
				options.FlagSinkAddr + " " + forwardedAddr,
			"never a credential",
			docsSourceVersusSinkAddr,
		} {
			if !strings.Contains(rendered, want) {
				t.Errorf("the block does not carry %q:\n%s", want, rendered)
			}
		}
	})

	t.Run("not given", func(t *testing.T) {
		failure := profileFailure(t, routesConfig(profile), &options.GlobalFlags{})
		rendered := failure.Render("kuberecord timeline", false)

		for _, unwanted := range []string{docsSourceVersusSinkAddr, forwardedAddr} {
			if strings.Contains(rendered, unwanted) {
				t.Errorf("the block explains --%s to somebody who did not pass it (%q):\n%s",
					options.FlagSinkAddr, unwanted, rendered)
			}
		}
	})
}

// TestTheSwitchRouteMatchesHowTheProfileWasChosen.
//
// A profile named by --profile is switched by changing the flag; one the file
// made active is switched by moving the pointer. Offering the wrong one is
// offering a command that leaves the failure exactly where it was — `use-profile`
// changes nothing while --profile is on the command line.
func TestTheSwitchRouteMatchesHowTheProfileWasChosen(t *testing.T) {
	unsetPasswordEnv(t)
	profile := clickHouseStanza(func(p *ClickHouseProfile) { p.PasswordEnv = DefaultPasswordEnv })
	cfg := routesConfig(profile, "archive", "staging")

	t.Run("the file made it active", func(t *testing.T) {
		rendered := profileFailure(t, cfg, &options.GlobalFlags{}).Render("kuberecord timeline", false)
		if !strings.Contains(rendered, "config use-profile archive") {
			t.Errorf("the switch route does not move the pointer:\n%s", rendered)
		}
		if !strings.Contains(rendered, "currentProfile in\n"+routesConfigPath) {
			t.Errorf("the block does not say why this profile was the one:\n%s", rendered)
		}
	})

	t.Run("--profile named it", func(t *testing.T) {
		rendered := profileFailure(t, cfg, &options.GlobalFlags{Profile: routesName}).
			Render("kuberecord timeline", false)
		if !strings.Contains(rendered, "--"+options.FlagProfile+" archive") {
			t.Errorf("the switch route does not change the flag:\n%s", rendered)
		}
		if strings.Contains(rendered, "config use-profile") {
			t.Errorf("the block offers a pointer move that --%s would override:\n%s",
				options.FlagProfile, rendered)
		}
	})

	// Both spellings name the rest of the file, so a reader whose replacement is
	// not the first name alphabetically can still see it.
	for _, flags := range []*options.GlobalFlags{{}, {Profile: routesName}} {
		rendered := profileFailure(t, cfg, flags).Render("kuberecord timeline", false)
		if !strings.Contains(rendered, "archive, staging") {
			t.Errorf("the block does not list the other profiles:\n%s", rendered)
		}
	}
}

// TestTheOnlyProfileIsToldSoRatherThanOfferedNothing.
//
// A remedy naming no command reads as though the reader should have known which
// name to substitute — the sentence errDeletingTheActiveProfile writes for the
// same state. The two commands offered instead are both real, and the deletion
// carries --force because the profile is the active one and delete-profile
// refuses that without it.
func TestTheOnlyProfileIsToldSoRatherThanOfferedNothing(t *testing.T) {
	unsetPasswordEnv(t)
	profile := clickHouseStanza(func(p *ClickHouseProfile) { p.PasswordEnv = DefaultPasswordEnv })

	rendered := profileFailure(t, routesConfig(profile), &options.GlobalFlags{}).
		Render("kuberecord timeline", false)

	for _, want := range []string{
		"only profile that file defines",
		routesConfigPath,
		"config set-profile <name>",
		"config delete-profile " + routesName + " --" + options.FlagForce,
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the block does not carry %q:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "config use-profile") {
		t.Errorf("the block offers a switch to a profile that does not exist:\n%s", rendered)
	}
}

// TestConfigResolveIsNotToldToRunItself.
//
// `config resolve` returns the chain's failure verbatim, so this block prints
// underneath a report that has already shown step 3 failing with its reason.
// Telling that reader to run the command they just ran spends the most
// load-bearing paragraph in the CLI on a no-op.
func TestConfigResolveIsNotToldToRunItself(t *testing.T) {
	unsetPasswordEnv(t)
	profile := clickHouseStanza(func(p *ClickHouseProfile) { p.PasswordEnv = DefaultPasswordEnv })
	failure := profileFailure(t, routesConfig(profile, "archive"), &options.GlobalFlags{})

	// The pointer is a command line of its own, and that is what must be gone.
	// The bypass route still spells the invocation the reader typed, so a bare
	// substring search for the words would find `config resolve … --sink …` and
	// report a self-pointer that is not there.
	const pointer = "\n    kuberecord config resolve\n"

	under := failure.Render("kuberecord config resolve", false)
	if strings.Contains(under, pointer) {
		t.Errorf("`config resolve` is told to run itself:\n%s", under)
	}
	// And the three routes are still all there: the suppression drops one
	// pointer, never a way out.
	for _, want := range []string{"export ", "--" + options.FlagSink, "config use-profile archive"} {
		if !strings.Contains(under, want) {
			t.Errorf("suppressing the self-pointer dropped %q:\n%s", want, under)
		}
	}

	elsewhere := failure.Render("kuberecord timeline", false)
	if !strings.Contains(elsewhere, pointer) {
		t.Errorf("no other command is pointed at the report:\n%s", elsewhere)
	}
}

// TestTheRoutesCarryNoCredential.
//
// Two plants, because there are two ways a value could reach this block. The
// first is a stanza that holds one: `clickhouse.password` is a field the
// configuration file refuses (ClickHouseProfile.validate), and it exists so that
// a user who fills it in is told why — which means a profile assembled in memory
// can carry it, and a renderer that described the stanza rather than its
// references would print it. The second is a file the profile points at, whose
// content this code reads a few lines away in ResolvePassword.
func TestTheRoutesCarryNoCredential(t *testing.T) {
	t.Run("a stanza holding one inline", func(t *testing.T) {
		unsetPasswordEnv(t)
		profile := clickHouseStanza(func(p *ClickHouseProfile) {
			p.PasswordEnv = DefaultPasswordEnv
			p.Password = plantedPassword
		})

		rendered := profileFailure(t, routesConfig(profile, "archive"), &options.GlobalFlags{}).
			Render("kuberecord timeline", false)
		if strings.Contains(rendered, plantedPassword) {
			t.Errorf("the block prints a password held in the stanza:\n%s", rendered)
		}
	})

	t.Run("a file holding one", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root reads a mode-0000 file, so the failure this case needs cannot be produced")
		}
		file := filepath.Join(t.TempDir(), "password")
		if err := os.WriteFile(file, []byte(plantedPassword), 0o600); err != nil {
			t.Fatalf("writing the password file: %v", err)
		}
		if err := os.Chmod(file, 0o000); err != nil {
			t.Fatalf("making the password file unreadable: %v", err)
		}
		profile := clickHouseStanza(func(p *ClickHouseProfile) { p.PasswordFile = file })

		failure := profileFailure(t, routesConfig(profile, "archive"), &options.GlobalFlags{})
		rendered := failure.Render("kuberecord timeline", false)
		if strings.Contains(rendered, plantedPassword) {
			t.Errorf("the block prints the content of the password file:\n%s", rendered)
		}
		// The one-line failure travels further than the block does — into logs,
		// into bug reports — so it is asserted too.
		if strings.Contains(failure.Error(), plantedPassword) {
			t.Errorf("the one-line failure prints the content of the password file: %v", failure)
		}
		if !strings.Contains(rendered, file) {
			t.Errorf("the block does not name the path it could not read:\n%s", rendered)
		}
	})
}

// TestTheProfileBlockRendersInBothColourModes, against golden files.
//
// A golden file rather than assembled expectations because the thing under test
// is a page of prose: line breaks, blank lines and where the commands sit are the
// content, and a test asserting substrings would pass on a block whose paragraphs
// had run together.
func TestTheProfileBlockRendersInBothColourModes(t *testing.T) {
	unsetPasswordEnv(t)

	byEnv := clickHouseStanza(func(p *ClickHouseProfile) { p.PasswordEnv = DefaultPasswordEnv })
	byFile := clickHouseStanza(func(p *ClickHouseProfile) { p.PasswordFile = routesPasswordFile })

	for _, tc := range []struct {
		name    string
		profile Profile
		others  []string
		flags   *options.GlobalFlags
		colored bool
	}{
		{name: "env-unset", profile: byEnv, others: []string{"archive", "staging"},
			flags: &options.GlobalFlags{}},
		{name: "env-unset-color", profile: byEnv, others: []string{"archive", "staging"},
			flags: &options.GlobalFlags{}, colored: true},
		{name: "sink-addr", profile: byEnv, others: []string{"archive"},
			flags: &options.GlobalFlags{SinkAddr: forwardedAddr}},
		{name: "password-file", profile: byFile, others: []string{"archive"},
			flags: &options.GlobalFlags{Profile: routesName}},
		{name: "only-profile", profile: byEnv, flags: &options.GlobalFlags{}},
		{name: "malformed-stanza", profile: Profile{Backend: BackendKind("postgres")},
			others: []string{"archive"}, flags: &options.GlobalFlags{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure := profileFailure(t, routesConfig(tc.profile, tc.others...), tc.flags)
			assertProfileGolden(t, tc.name, failure.Render("kuberecord timeline", tc.colored))
		})
	}
}

// profileBlock renders the fixture failure, for the colour-invariance property
// that lives with the other renderings in diagnose_internal_test.go.
//
// The --sink-addr case is the one it renders, because it is the longest: it
// exercises every tier the block uses and the paragraph that only appears when
// the flag was given, so a colour sequence that leaked into one span cannot hide
// in the case nobody chose.
func profileBlock(t *testing.T, colorize bool) string {
	t.Helper()

	unsetPasswordEnv(t)
	profile := clickHouseStanza(func(p *ClickHouseProfile) { p.PasswordEnv = DefaultPasswordEnv })
	failure := profileFailure(t, routesConfig(profile, "archive"),
		&options.GlobalFlags{SinkAddr: forwardedAddr})
	return failure.Render("kuberecord timeline", colorize)
}

// assertProfileGolden compares a rendering against its checked-in file.
//
// It shares diagnose_internal_test.go's -update flag rather than declaring a
// second one: two flag.Bool("update") in one package panic before any test runs.
func assertProfileGolden(t *testing.T, name, got string) {
	t.Helper()

	path := filepath.Join("testdata", "profile", name+".golden")
	if *updateDiagnoseGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatalf("creating the golden directory: %v", err)
		}
		if err := os.WriteFile(path, []byte(got), 0o600); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}
		return
	}

	want, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("reading %s (run `go test ./internal/cli/resolve/ -update` to create it): %v", path, err)
	}
	if got != string(want) {
		t.Errorf("the rendering of %s changed.\n--- want ---\n%s\n--- got ---\n%s", name, want, got)
	}
}

//
// The two properties this task deliberately did not change
//

// TestAFailingProfileDoesNotFallThroughToDiscovery is D35 as an assertion.
//
// The obvious fix for the failure this file explains is to let the chain carry on
// to the next step when a profile cannot be resolved. It would make the symptom
// disappear and it is exactly what must not happen: the user configured a profile,
// and answering from the cluster's own sink instead would read from somewhere they
// did not choose and report success.
//
// The cluster here holds a perfectly good sink that discovery would find, and the
// assertion is both that resolution fails and that nothing was asked of the
// cluster at all — a fall-through that failed for some other reason would satisfy
// the first half alone.
func TestAFailingProfileDoesNotFallThroughToDiscovery(t *testing.T) {
	unsetPasswordEnv(t)

	resolver := fromSinkResolver(t, goodSecret(), fromSinkClickHouse(fixtureAddr))
	resolver.Flags = &options.GlobalFlags{}
	resolver.ConfigPath = routesConfigPath
	resolver.Config = routesConfig(clickHouseStanza(func(p *ClickHouseProfile) {
		p.PasswordEnv = DefaultPasswordEnv
	}))
	resolver.Config.OperatorNamespace = fixtureNamespace

	_, origin, err := resolver.resolveTarget(t.Context())
	if err == nil {
		t.Fatal("a profile whose credential does not resolve produced a backend")
	}
	if origin != OriginProfile {
		t.Errorf("the chain answered from %q, want %q: the profile step is fatal",
			origin, OriginProfile)
	}
	if actions := dynamicActions(t, resolver); actions != 0 {
		t.Errorf("the chain made %d request(s) to the cluster after the profile failed; "+
			"discovery must not be consulted", actions)
	}

	// And the discovery step is recorded as one the chain never got to, so the
	// report says the same thing the behaviour does.
	completeChain(&resolver.backendSteps, backendChainSteps())
	for _, step := range resolver.backendSteps {
		if step.Step == OriginDiscovered.Step() && step.Outcome != StepNotReached {
			t.Errorf("discovery is recorded as %q, want %q", step.Outcome, StepNotReached)
		}
	}
}

// dynamicActions counts what a resolver has asked the cluster.
//
// Zero is the assertion rather than a diagnostic: "the profile step is fatal" and
// "the profile step failed" are different claims, and only a request count can
// tell them apart when the next step would have succeeded.
func dynamicActions(t *testing.T, resolver *BackendResolver) int {
	t.Helper()
	fake, ok := resolver.Clients.Dynamic.(*dynamicfake.FakeDynamicClient)
	if !ok {
		t.Fatalf("this resolver's dynamic client is a %T, which records no actions", resolver.Clients.Dynamic)
	}
	return len(fake.Actions())
}

// TestSinkAddrStillReplacesNothingButTheAddressOfAProfile is D36 as an assertion.
//
// The other obvious fix: let --sink-addr reach past the credential, so that the
// user who passed it to reach a forwarded port gets an answer. It would make the
// flag mean one thing against a custom resource and another against a profile,
// and its name describes neither.
//
// Both halves are here because they are one property seen from two sides. The
// flag does not rescue a profile whose credential is unresolvable, and on a
// profile that does resolve it moves the address and nothing else.
func TestSinkAddrStillReplacesNothingButTheAddressOfAProfile(t *testing.T) {
	t.Run("it does not resolve the credential", func(t *testing.T) {
		unsetPasswordEnv(t)
		profile := clickHouseStanza(func(p *ClickHouseProfile) { p.PasswordEnv = DefaultPasswordEnv })

		if _, err := targetFromProfile(routesName, profile, forwardedAddr); err == nil {
			t.Fatalf("--%s resolved a profile whose password reference does not",
				options.FlagSinkAddr)
		}
	})

	t.Run("it moves the address and nothing else", func(t *testing.T) {
		t.Setenv(DefaultPasswordEnv, plantedPassword)
		profile := clickHouseStanza(func(p *ClickHouseProfile) { p.PasswordEnv = DefaultPasswordEnv })

		plain, err := targetFromProfile(routesName, profile, "")
		if err != nil {
			t.Fatalf("resolving the profile with no override: %v", err)
		}
		overridden, err := targetFromProfile(routesName, profile, forwardedAddr)
		if err != nil {
			t.Fatalf("resolving the profile with --%s: %v", options.FlagSinkAddr, err)
		}

		if plain.clickhouse.Addr != routesAddr || overridden.clickhouse.Addr != forwardedAddr {
			t.Fatalf("the address did not move from %q to %q: %q and %q",
				routesAddr, forwardedAddr, plain.clickhouse.Addr, overridden.clickhouse.Addr)
		}
		// Compared against the same resolution without the flag rather than
		// against a list of values, because the property is "one field differs".
		plain.clickhouse.Addr, overridden.clickhouse.Addr = "", ""
		if plain.clickhouse != overridden.clickhouse {
			t.Errorf("--%s changed more than the endpoint:\n without: %+v\n with:    %+v",
				options.FlagSinkAddr, plain.clickhouse, overridden.clickhouse)
		}
	})
}
