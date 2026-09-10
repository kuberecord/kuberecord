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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/kuberecord/kuberecord/api/v1alpha1"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
)

// The wizard is tested from inside the package for one structural reason: the
// terminal check refuses a non-interactive stdin, and a test's stdin is a buffer.
// So the gate is driven through the whole binary — where the exit code is the
// half that matters — and the questions are driven against the type directly.
//
// The first test below is the one that matters. Everything else here asserts that
// the wizard behaves; that one asserts it cannot *decide* anything, which is the
// property that keeps a friendlier route from becoming a second, laxer one.

// wizardHome points the configuration file at a temporary directory and returns
// its path.
func wizardHome(t *testing.T) string {
	t.Helper()

	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", home)
	return filepath.Join(home, resolve.ConfigDirName, resolve.ConfigFileName)
}

// scriptedWizard builds a wizard whose answers come from a string.
//
// The resolver is deliberately one that fails the test if it is called: every
// case using this helper answers "no" to the discovery question, and a wizard that
// reached for a cluster anyway would be doing something none of these cases asked
// for.
func scriptedWizard(t *testing.T, answers ...string) (*setProfileWizard, *strings.Builder) {
	t.Helper()
	return colouredWizard(t, false, answers...)
}

// colouredWizard is scriptedWizard with the colour decision made explicitly.
//
// The two share one body so that a coloured transcript and a plain one differ in
// nothing but that decision — the property the golden pair exists to show, and one
// a second constructor would quietly stop guaranteeing.
func colouredWizard(
	t *testing.T, colorize bool, answers ...string,
) (*setProfileWizard, *strings.Builder) {
	t.Helper()

	// No trailing newline for an empty script, so that "answer nothing" is an
	// immediate EOF rather than one blank line — which would be answered with the
	// default and would run the case after the one being tested.
	script := strings.Join(answers, "\n")
	if script != "" {
		script += "\n"
	}

	var out, errOut strings.Builder
	in := strings.NewReader(script)
	return &setProfileWizard{
		streams:   genericiooptions.IOStreams{In: in, Out: &out, ErrOut: &errOut},
		in:        bufio.NewReader(in),
		severity:  render.NewSeverity(colorize),
		colorize:  colorize,
		invokedAs: options.StandaloneName,
		newResolver: func() (*resolve.BackendResolver, error) {
			t.Errorf("the wizard built a resolver for a run that declined discovery")
			return nil, errors.New("no cluster in this case")
		},
	}, &errOut
}

// requiredFieldValues are the values that get each backend past its one mandatory
// field, so that a case about an *optional* field is not also a case about a
// missing required one.
var requiredFieldValues = map[resolve.BackendKind]map[string]string{
	resolve.BackendClickHouse: {options.FlagAddr: "clickhouse.example:9000"},
	resolve.BackendS3:         {options.FlagBucket: "acme-audit"},
	resolve.BackendLocal:      {options.FlagPath: "/archives/kuberecord"},
}

// driftCase is one value, put through both routes to a profile.
type driftCase struct {
	name    string
	backend resolve.BackendKind
	flag    string
	value   string

	// refusal is the substring both routes must refuse the value with. Empty
	// means both must accept it.
	refusal string
}

// driftCases covers every clause of the validator a field can reach, in both
// polarities: a value it refuses and a value it takes.
//
// Both polarities on purpose. A table of rejections alone would pass against a
// prompt path that refused everything, and a table of acceptances alone would
// pass against one that refused nothing.
var driftCases = []driftCase{
	{
		name:    "a ClickHouse profile with no address",
		backend: resolve.BackendClickHouse, flag: options.FlagAddr, value: "",
		refusal: "clickhouse.addr is required",
	},
	{
		name:    "a ClickHouse profile with one",
		backend: resolve.BackendClickHouse, flag: options.FlagAddr, value: "clickhouse.example:9000",
	},
	{
		name:    "an environment variable to read the password from",
		backend: resolve.BackendClickHouse, flag: options.FlagPasswordEnv,
		value: resolve.DefaultPasswordEnv,
	},
	{
		name:    "an S3 profile with no bucket",
		backend: resolve.BackendS3, flag: options.FlagBucket, value: "",
		refusal: "s3.bucket is required",
	},
	{
		name:    "a prefix with a leading slash",
		backend: resolve.BackendS3, flag: options.FlagPrefix, value: "/kuberecord",
		refusal: "has a leading or trailing slash",
	},
	{
		name:    "a prefix with a trailing slash",
		backend: resolve.BackendS3, flag: options.FlagPrefix, value: "kuberecord/",
		refusal: "has a leading or trailing slash",
	},
	{
		name:    "a prefix that is a path fragment",
		backend: resolve.BackendS3, flag: options.FlagPrefix, value: archivePrefix,
	},
	{
		name:    "an endpoint with no scheme",
		backend: resolve.BackendS3, flag: options.FlagEndpoint, value: "minio.internal:9000",
		refusal: "has no scheme",
	},
	{
		name:    "an endpoint with one",
		backend: resolve.BackendS3, flag: options.FlagEndpoint, value: "https://minio.internal:9000",
	},
	{
		name:    "a local profile with no directory",
		backend: resolve.BackendLocal, flag: options.FlagPath, value: "",
		refusal: "local.path is required",
	},
	{
		name:    "a local profile whose prefix is a path",
		backend: resolve.BackendLocal, flag: options.FlagPrefix, value: "/kuberecord",
		refusal: "has a leading or trailing slash",
	},
	{
		name:    "a backend no build of this defines",
		backend: resolve.BackendClickHouse, flag: options.FlagBackend, value: "postgres",
		refusal: "is not one of clickhouse, s3, local",
	},
}

// TestTheFlagPathAndThePromptPathAgree is the anti-drift guard, and the
// acceptance criterion this task turns on.
//
// One table, two routes, one assertion: a value the flags take is a value the
// questions take, a value the flags refuse is a value the questions refuse, and
// the sentence refusing it is the same sentence. That property does not come from
// this test — it comes from there being exactly one validator, reached through
// profileFields.validate from both sides — but nothing except a test keeps it
// true, because a prompt-side check would be easy to add and would look like an
// improvement while it was being written.
func TestTheFlagPathAndThePromptPathAgree(t *testing.T) {
	for _, tc := range driftCases {
		t.Run(tc.name, func(t *testing.T) {
			flagStderr, flagCode := tc.throughFlags(t)
			promptStderr, promptErr := tc.throughPrompts(t)

			flagRefused := flagCode != exit.Success
			promptRefused := strings.Contains(promptStderr, render.WarningMarker)

			switch {
			case tc.refusal == "" && flagRefused:
				t.Fatalf("the flag path refused a value the prompt path is expected to take:\n%s", flagStderr)
			case tc.refusal == "" && promptRefused:
				t.Fatalf("the prompt path refused a value the flag path took:\n%s", promptStderr)
			case tc.refusal != "" && !flagRefused:
				t.Fatalf("the flag path accepted %q, which must be refused with %q", tc.value, tc.refusal)
			case tc.refusal != "" && !promptRefused:
				t.Fatalf("the prompt path accepted %q, which the flag path refuses with %q:\n%s",
					tc.value, tc.refusal, promptStderr)
			}

			if promptErr != nil {
				t.Fatalf("the prompt path failed rather than re-asking: %v", promptErr)
			}
			if tc.refusal == "" {
				return
			}
			for route, stderr := range map[string]string{"flag": flagStderr, "prompt": promptStderr} {
				if !strings.Contains(stderr, tc.refusal) {
					t.Errorf("the %s path refused %q without saying %q:\n%s",
						route, tc.value, tc.refusal, stderr)
				}
			}
		})
	}
}

// throughFlags runs this case as `config set-profile NAME --backend … --flag value`.
func (tc driftCase) throughFlags(t *testing.T) (stderr string, code int) {
	t.Helper()
	wizardHome(t)

	args := []string{options.StandaloneName, "config", "set-profile", "drift"}
	backend := tc.backend
	if tc.flag == options.FlagBackend {
		backend = resolve.BackendKind(tc.value)
	}
	args = append(args, "--"+options.FlagBackend, string(backend))

	// The required field first, then the field under test — which overwrites it
	// when they are the same flag, so an empty --addr case really does arrive
	// empty.
	for flag, value := range requiredFieldValues[tc.backend] {
		if flag != tc.flag {
			args = append(args, "--"+flag, value)
		}
	}
	if tc.flag != options.FlagBackend {
		args = append(args, "--"+tc.flag, tc.value)
	}

	streams, _, errOut := wizardStreams()
	code = Run(args, streams)
	return errOut.String(), code
}

// throughPrompts runs this case as the questions, answering everything else with
// the value the flag path used.
//
// A refused answer is followed by an acceptable one, so the run finishes rather
// than falling off the end of the script — which is how the test can assert that
// the wizard *re-asked* rather than that it gave up.
func (tc driftCase) throughPrompts(t *testing.T) (stderr string, err error) {
	t.Helper()
	wizardHome(t)

	answers := []string{"n"} // no, do not read the settings from a sink

	backend := tc.backend
	if tc.flag == options.FlagBackend {
		answers = append(answers, tc.value)
	}
	answers = append(answers, string(backend))

	for _, field := range profileFieldFlags {
		if field.name == options.FlagPasswordEnv && backend == resolve.BackendClickHouse {
			if tc.flag == options.FlagPasswordEnv {
				answers = append(answers, "environment", tc.value)
				continue
			}
			answers = append(answers, "none")
			continue
		}
		if !field.asks(backend) {
			continue
		}
		good := requiredFieldValues[tc.backend][field.name]
		if field.name != tc.flag {
			answers = append(answers, good)
			continue
		}
		answers = append(answers, tc.value)
		if tc.refusal != "" {
			answers = append(answers, good)
		}
	}

	wizard, errOut := scriptedWizard(t, answers...)
	err = wizard.run(t.Context(), "drift")
	return errOut.String(), err
}

// wizardStreams is the three-buffer IOStreams these cases drive the binary with.
// In is a reader and therefore not a terminal, which is what sends a flagless
// invocation to the refusal rather than to a question nobody can answer.
func wizardStreams() (genericiooptions.IOStreams, *strings.Builder, *strings.Builder) {
	var out, errOut strings.Builder
	return genericiooptions.IOStreams{
		In: strings.NewReader(""), Out: &out, ErrOut: &errOut,
	}, &out, &errOut
}

// TestSetProfileWithoutFlagsOffATerminalIsARefusal is the CI half of the mode
// selection.
//
// A wizard that blocked here would hang a pipeline until something killed it, and
// the message saying what was actually wanted would never arrive. The exit code is
// the assertion that matters: 2 is what a caller's `set -e` and a reviewer's
// `$?` both read.
func TestSetProfileWithoutFlagsOffATerminalIsARefusal(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"with no name at all", []string{"config", "set-profile"}},
		{"with a name and nothing else", []string{"config", "set-profile", "local"}},
		{
			// A global flag is not this command's flag: --operator-namespace says
			// where in the cluster the first question would look, so an invocation
			// carrying one is precisely one that wants to be asked.
			name: "with a global flag, which says nothing about a profile",
			args: []string{
				"--" + options.FlagOperatorNamespace, options.DefaultOperatorNamespace,
				"config", "set-profile", "local",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := wizardHome(t)

			streams, out, errOut := wizardStreams()
			code := Run(append([]string{options.StandaloneName}, tc.args...), streams)

			if code != exit.UsageError {
				t.Errorf("exit code %d, want %d for a wizard with nobody to ask", code, exit.UsageError)
			}
			if out.String() != "" {
				t.Errorf("the refusal wrote to stdout, which belongs to data: %q", out)
			}
			for _, want := range []string{
				"not a terminal", "--" + options.FlagFromSink, "--" + options.FlagBackend,
			} {
				if !strings.Contains(errOut.String(), want) {
					t.Errorf("the refusal never names %q:\n%s", want, errOut)
				}
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Errorf("a refused set-profile created %s anyway", path)
			}
		})
	}
}

// TestSetProfileFlagsSuppressTheQuestions is the other half of mode selection: a
// user who said what they wanted is not asked about it.
func TestSetProfileFlagsSuppressTheQuestions(t *testing.T) {
	path := wizardHome(t)

	streams, _, errOut := wizardStreams()
	code := Run([]string{
		options.StandaloneName, "config", "set-profile", "laptop",
		"--" + options.FlagBackend, string(resolve.BackendLocal),
		"--" + options.FlagPath, "/archives/kuberecord",
	}, streams)

	if code != exit.Success {
		t.Fatalf("config set-profile exited %d: %s", code, errOut)
	}
	if strings.Contains(errOut.String(), "?") {
		t.Errorf("an invocation carrying flags was asked a question:\n%s", errOut)
	}
	if _, err := resolve.LoadConfig(path); err != nil {
		t.Fatalf("the flag path did not write a loadable configuration: %v", err)
	}
}

// TestTheWizardWritesAndTeachesTheFlagsItReplaces.
//
// The equivalent command is what makes this feature worth its cost, so it is
// asserted as a whole line rather than by fragments: it is the artifact somebody
// pastes into a bug report or lifts into a CI job, and a line that is *nearly*
// right is worse than none.
func TestTheWizardWritesAndTeachesTheFlagsItReplaces(t *testing.T) {
	path := wizardHome(t)

	wizard, errOut := scriptedWizard(t,
		"n",                           // do not read the settings from a sink
		"2",                           // the second backend: s3
		"acme-audit",                  // bucket
		"",                            // region: empty, exactly as omitting --region
		"https://minio.internal:9000", // endpoint
		"y",                           // force path style
		archivePrefix,                 // prefix
	)
	if err := wizard.run(t.Context(), "archive"); err != nil {
		t.Fatalf("the wizard failed: %v\n%s", err, errOut)
	}

	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("the wizard did not write a loadable configuration: %v", err)
	}
	profile, ok := cfg.Profiles["archive"]
	if !ok {
		t.Fatalf("the file holds no profile named archive: %+v", cfg)
	}
	if profile.Backend != resolve.BackendS3 || profile.S3 == nil {
		t.Fatalf("the profile is not an S3 one: %+v", profile)
	}
	if profile.S3.Bucket != "acme-audit" || profile.S3.Prefix != archivePrefix ||
		profile.S3.Endpoint != "https://minio.internal:9000" || !profile.S3.ForcePathStyle {
		t.Errorf("the profile does not carry what was answered: %+v", profile.S3)
	}
	// Empty is what omitting --region writes, and the reader resolves it to the
	// default. A wizard that filled it in would be a second field semantic.
	if profile.S3.Region != "" {
		t.Errorf("s3.region = %q, want the empty value omitting --region writes", profile.S3.Region)
	}

	want := "  kuberecord config set-profile archive --backend s3 --bucket acme-audit " +
		"--endpoint https://minio.internal:9000 --force-path-style --prefix kuberecord"
	if !strings.Contains(errOut.String(), want) {
		t.Errorf("the equivalent command is not\n%s\nin:\n%s", want, errOut)
	}
	if !strings.Contains(errOut.String(), "The same thing without the questions:") {
		t.Errorf("the equivalent command is printed without saying what it is:\n%s", errOut)
	}
}

// TestTheEquivalentCommandReproducesTheProfile closes the loop the previous test
// opens.
//
// Printing a command that does not produce what was just written would be worse
// than printing nothing: it is a line somebody will paste into a CI job and
// believe. So the printed command is parsed back out of stderr and run.
func TestTheEquivalentCommandReproducesTheProfile(t *testing.T) {
	first := wizardHome(t)

	wizard, errOut := scriptedWizard(t,
		"n", "clickhouse", "clickhouse.example:9000", resolve.DefaultClickHouseDatabase, "kuberecord_ro",
		"environment", resolve.DefaultPasswordEnv, "y",
	)
	if err := wizard.run(t.Context(), "prod"); err != nil {
		t.Fatalf("the wizard failed: %v\n%s", err, errOut)
	}
	written, err := resolve.LoadConfig(first)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}

	args := equivalentArgs(t, errOut.String())
	wizardHome(t) // a second, empty configuration file for the replay
	streams, _, replayErr := wizardStreams()
	if code := Run(args, streams); code != exit.Success {
		t.Fatalf("the printed command exited %d: %s", code, replayErr)
	}

	replayed, err := resolve.LoadConfig(os.Getenv("XDG_CONFIG_HOME") +
		"/" + resolve.ConfigDirName + "/" + resolve.ConfigFileName)
	if err != nil {
		t.Fatalf("resolve.LoadConfig of the replay: %v", err)
	}
	if *replayed.Profiles["prod"].ClickHouse != *written.Profiles["prod"].ClickHouse {
		t.Errorf("the printed command wrote\n%+v\nwhere the questions wrote\n%+v",
			replayed.Profiles["prod"].ClickHouse, written.Profiles["prod"].ClickHouse)
	}
}

// equivalentArgs pulls the printed command off stderr and splits it into argv.
//
// Splitting on spaces is enough because every value these cases use is one word;
// a value that needed quoting would arrive quoted and fail this parse loudly,
// which is the right outcome for a test that is asserting the line is runnable.
func equivalentArgs(t *testing.T, stderr string) []string {
	t.Helper()

	for line := range strings.SplitSeq(stderr, "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == options.StandaloneName {
			return fields
		}
	}
	t.Fatalf("stderr carries no equivalent command:\n%s", stderr)
	return nil
}

// TestCancellingAtAnyQuestionWritesNothing.
//
// Every prefix of a complete run is a place the user can stop, so every prefix is
// a case. Nothing is written until the last question is answered, which is what
// makes "writes nothing" a property of the order rather than of an undo.
func TestCancellingAtAnyQuestionWritesNothing(t *testing.T) {
	complete := []string{"n", "clickhouse", "clickhouse.example:9000", "", "", "none", "n"}

	for stop := range len(complete) {
		t.Run(strings.Join(complete[:stop], ","), func(t *testing.T) {
			path := wizardHome(t)

			wizard, errOut := scriptedWizard(t, complete[:stop]...)
			// The script ends where it ends, so the next read is an EOF: exactly
			// what Ctrl-D at that question produces.
			if err := wizard.run(t.Context(), "prod"); err != nil {
				t.Fatalf("cancelling was an error rather than an answer: %v", err)
			}
			if !strings.Contains(errOut.String(), "cancelled, and nothing was written") {
				t.Errorf("stopping was silent, so a reader cannot tell whether it wrote:\n%s", errOut)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Errorf("a cancelled wizard created %s anyway", path)
			}
		})
	}
}

// TestTheWizardAsksForAProfileNameWhenTheArgumentIsAbsent.
//
// `config set-profile` with nothing after it at all is the shortest thing a user
// can type, and it has to work: the name is a question like any other, refused
// while empty by the same function that refuses an empty argument on the flag
// path.
func TestTheWizardAsksForAProfileNameWhenTheArgumentIsAbsent(t *testing.T) {
	path := wizardHome(t)

	wizard, errOut := scriptedWizard(t,
		"",                     // an empty name — refused, and re-asked
		"laptop",               // the name
		"n",                    // do not read the settings from a sink
		"local",                // the backend
		"/archives/kuberecord", // path
		"",                     // prefix
	)
	if err := wizard.run(t.Context(), ""); err != nil {
		t.Fatalf("the wizard failed: %v\n%s", err, errOut)
	}

	if !strings.Contains(errOut.String(), render.WarningMarker+" the profile name is empty") {
		t.Errorf("an empty name was not refused in the flag path's own words:\n%s", errOut)
	}
	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}
	if _, ok := cfg.Profiles[profileLaptop]; !ok {
		t.Errorf("the file holds no profile named laptop: %+v", cfg)
	}
}

// TestAnInterruptedWizardStopsAtTheNextAnswer.
//
// A blocked read does not return when a signal arrives, so the interrupt is
// noticed at the next answer rather than during the wait — which is the same
// place a cold scan notices one, and is why the check is after the read rather
// than around it. What matters is the same either way: nothing is written.
func TestAnInterruptedWizardStopsAtTheNextAnswer(t *testing.T) {
	path := wizardHome(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	wizard, errOut := scriptedWizard(t, "n", "clickhouse", "clickhouse.example:9000", "", "", "none", "n")
	if err := wizard.run(ctx, "prod"); err != nil {
		t.Fatalf("an interrupted wizard failed rather than stopping: %v", err)
	}
	if !strings.Contains(errOut.String(), "cancelled, and nothing was written") {
		t.Errorf("the interrupt was silent:\n%s", errOut)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("an interrupted wizard created %s anyway", path)
	}
}

// TestEveryProfileFlagIsInTheTable is the non-vacuity guard under the whole
// design.
//
// The table is what the questions, the refusals beside --from-sink and the
// equivalent command are all generated from, so a flag registered beside it is a
// field with no question and no way to be refused — and both of those are silent.
func TestEveryProfileFlagIsInTheTable(t *testing.T) {
	root, _ := NewRootCommand(options.StandaloneName, genericiooptions.IOStreams{})
	command, _, err := root.Find([]string{"config", "set-profile"})
	if err != nil {
		t.Fatalf("locating config set-profile: %v", err)
	}

	named := make([]string, 0, len(profileFieldFlags))
	for _, field := range profileFieldFlags {
		named = append(named, field.name)
		if (field.str == nil) == (field.boolean == nil) {
			t.Errorf("--%s has neither one accessor nor exactly one", field.name)
		}
	}

	// The two flags that are not fields of a profile, listed rather than pattern
	// matched so that a third one has to be argued for here. --from-sink fills every
	// field in from a custom resource, and --use says what to do with the file's
	// active pointer once the stanza is written — so neither has a question of its
	// own in the table, and neither is a value --from-sink could disagree with.
	notAField := []string{options.FlagFromSink, options.FlagUse}

	command.LocalNonPersistentFlags().VisitAll(func(f *pflag.Flag) {
		if slices.Contains(notAField, f.Name) || slices.Contains(named, f.Name) {
			return
		}
		t.Errorf("--%s is registered on config set-profile but is not in profileFieldFlags, "+
			"so nothing asks for it and --%s cannot refuse it", f.Name, options.FlagFromSink)
	})
}

// TestTheWizardReachesFromSinkWithoutTheUserKnowingIt is the first question, and
// the reason this command is worth having.
//
// The user types "y" and "1". What they get is --from-sink: the address the
// cluster records, the rewrite that makes it reachable from a laptop, the
// port-forward that completes it, and a profile that holds no password. The last
// line they are shown is the flag command they did not know existed, so the second
// profile does not need a second conversation.
func TestTheWizardReachesFromSinkWithoutTheUserKnowingIt(t *testing.T) {
	resolver, _, path := fromSinkFixture(t)

	// An existing active profile, so the write does not activate this one and the
	// `use-profile` line is the thing to run next. A first profile in an empty
	// file becomes active on its own and says so, which is --from-sink's own
	// behaviour and is asserted where that behaviour lives.
	seedActiveProfile(t, path)

	// y — read it from a sink; 1 — the ClickHouseSink; then the offered address,
	// the offered user, the environment and the offered variable, all by pressing
	// return. Four defaults, and they write what --from-sink writes with no flags.
	//
	// The last answer is the last question: no, leave the seeded profile active.
	// It is asked at all because this file already has one — a write into an empty
	// file has nothing to decide and is not asked (activationIsADecision).
	wizard, errOut := scriptedWizard(t, "y", "1", "", "", "", "", "n")
	wizard.newResolver = func() (*resolve.BackendResolver, error) { return resolver, nil }

	if err := wizard.run(t.Context(), "local"); err != nil {
		t.Fatalf("the wizard failed: %v\n%s", err, errOut)
	}

	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("the wizard did not write a loadable configuration: %v", err)
	}
	stanza := cfg.Profiles["local"].ClickHouse
	if stanza == nil {
		t.Fatalf("the file holds no ClickHouse profile named local: %+v", cfg)
	}
	if stanza.Addr != "127.0.0.1:9000" {
		t.Errorf("clickhouse.addr = %q, want the forwarded port the recorded address was rewritten to",
			stanza.Addr)
	}
	if stanza.Database != resolve.DefaultClickHouseDatabase || stanza.Username != sinkUsername {
		t.Errorf("the profile does not carry the sink's own database and user: %+v", stanza)
	}
	if stanza.PasswordEnv != resolve.DefaultPasswordEnv || stanza.Password != "" {
		t.Errorf("the profile does not reference a password rather than hold one: %+v", stanza)
	}

	// The whole point of the derivation, and the whole point of the file mode:
	// the value in the Secret this fixture holds must reach neither.
	written, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading what was written: %v", err)
	}
	for where, content := range map[string]string{"stderr": errOut.String(), path: string(written)} {
		if strings.Contains(content, sinkPassword) {
			t.Errorf("the sink's password reached %s", where)
		}
	}

	for _, want := range []string{
		// --from-sink's own explanation, printed unchanged: this route derives
		// through the same call and therefore says the same things.
		"ClickHouseSink/default records " + internalAddr,
		"kubectl port-forward -n kuberecord-quickstart svc/clickhouse 9000:9000",
		// The next step, because the write did not activate this profile.
		"config use-profile local",
		// And the flags that would have done it without the questions.
		"The same thing without the questions:",
		"  kuberecord config set-profile local --from-sink ClickHouseSink/default " +
			"--addr 127.0.0.1:9000 --password-env " + resolve.DefaultPasswordEnv,
	} {
		if !strings.Contains(errOut.String(), want) {
			t.Errorf("stderr never says %q:\n%s", want, errOut)
		}
	}
}

// TestTheWizardRecordsAnAddressTheUserGaveInstead.
//
// The offered default is what --from-sink would have written; typing over it has
// to reach the same place --addr reaches, explanation included. A profile whose
// address differed from the custom resource it was derived from without saying so
// would break the honesty property --sink-addr also keeps.
func TestTheWizardRecordsAnAddressTheUserGaveInstead(t *testing.T) {
	resolver, _, path := fromSinkFixture(t)

	wizard, errOut := scriptedWizard(t, "y", "ClickHouseSink/default", "127.0.0.1:19000", "", "", "")
	wizard.newResolver = func() (*resolve.BackendResolver, error) { return resolver, nil }

	if err := wizard.run(t.Context(), "local"); err != nil {
		t.Fatalf("the wizard failed: %v\n%s", err, errOut)
	}

	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}
	if got := cfg.Profiles["local"].ClickHouse.Addr; got != "127.0.0.1:19000" {
		t.Errorf("clickhouse.addr = %q, want the address that was typed", got)
	}
	for _, want := range []string{
		"as --addr asked",
		"--from-sink ClickHouseSink/default --addr 127.0.0.1:19000 --password-env " +
			resolve.DefaultPasswordEnv,
	} {
		if !strings.Contains(errOut.String(), want) {
			t.Errorf("stderr never says %q:\n%s", want, errOut)
		}
	}
}

// TestDiscoveryThatFindsNothingSaysSoAndCarriesOn.
//
// Falling through to typing it out is right; falling through in silence is not.
// The user asked a question and has to hear its answer, or the questions that
// follow look like the wizard ignoring them (Invariant 4, D31).
func TestDiscoveryThatFindsNothingSaysSoAndCarriesOn(t *testing.T) {
	resolver, _, path := emptyClusterFixture(t)

	wizard, errOut := scriptedWizard(t,
		"y",                    // yes, read it from a sink — and there are none
		"local",                // so: the local backend
		"/archives/kuberecord", // path
		"",                     // prefix
	)
	wizard.newResolver = func() (*resolve.BackendResolver, error) { return resolver, nil }

	if err := wizard.run(t.Context(), "laptop"); err != nil {
		t.Fatalf("the wizard failed: %v\n%s", err, errOut)
	}

	for _, want := range []string{
		render.WarningMarker + " this cluster holds no sink custom resources",
		"Carrying on with the settings typed out by hand.",
		"--backend local --path /archives/kuberecord",
	} {
		if !strings.Contains(errOut.String(), want) {
			t.Errorf("stderr never says %q:\n%s", want, errOut)
		}
	}
	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}
	if cfg.Profiles["laptop"].Local == nil {
		t.Errorf("the fall-through did not write a local profile: %+v", cfg)
	}
}

// The last question, and the two states in which it is not asked.
//
// Its default is the decision (D38): the active profile answers every later
// command that names no source, so a wizard that switched by default would
// redirect `timeline`, `diff` and `get` to a store somebody wrote in order to
// inspect. One keystroke is the whole cost of the other answer, and whichever way
// it goes the outcome is reported and reproduced by the printed command.

// TestTheWizardsLastQuestionDefaultsToNo.
//
// Pressing return through it leaves the file's existing choice alone, and the
// `use-profile` line is then the thing to run next — which is the behaviour every
// wizard case had before the question existed.
func TestTheWizardsLastQuestionDefaultsToNo(t *testing.T) {
	path := wizardHome(t)
	seedActiveProfile(t, path)

	// No to the sink, the local backend, its directory, no prefix — and then the
	// last question, answered by pressing return.
	wizard, errOut := scriptedWizard(t, "n", "local", "/archives/kuberecord", "", "")
	if err := wizard.run(t.Context(), "laptop"); err != nil {
		t.Fatalf("the wizard failed: %v\n%s", err, errOut)
	}

	// The default is in the prompt itself, which is the half that survives having
	// no colour: [y/N] is where a reader learns which way return goes.
	if !strings.Contains(errOut.String(), "Make this the active profile? [y/N]") {
		t.Errorf("the last question is not asked, or does not default to no:\n%s", errOut)
	}

	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}
	if cfg.CurrentProfile != "already-chosen" {
		t.Errorf("currentProfile = %q, want the profile that was already chosen", cfg.CurrentProfile)
	}
	if _, ok := cfg.Profiles[profileLaptop]; !ok {
		t.Errorf("the profile was not written: %+v", cfg.Profiles)
	}
	for _, want := range []string{
		"config use-profile laptop",
		"kuberecord config set-profile laptop --backend local --path /archives/kuberecord",
	} {
		if !strings.Contains(errOut.String(), want) {
			t.Errorf("stderr never says %q:\n%s", want, errOut)
		}
	}
	// The flag reproduces the outcome, and this outcome did not include an
	// activation — so printing it would make the line write a profile the
	// questions did not.
	if strings.Contains(errOut.String(), "--"+options.FlagUse) {
		t.Errorf("the equivalent command activates a profile the questions left alone:\n%s", errOut)
	}
}

// TestTheWizardActivatesWhenTheAnswerIsYes is the other answer.
//
// Both halves of it matter. The pointer moves, and the printed command gains the
// flag that moves it — a line reproducing the stanza and not the activation would
// reproduce half of what its reader just watched happen.
func TestTheWizardActivatesWhenTheAnswerIsYes(t *testing.T) {
	path := wizardHome(t)
	seedActiveProfile(t, path)

	wizard, errOut := scriptedWizard(t, "n", "local", "/archives/kuberecord", "", "y")
	if err := wizard.run(t.Context(), "laptop"); err != nil {
		t.Fatalf("the wizard failed: %v\n%s", err, errOut)
	}

	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}
	if cfg.CurrentProfile != profileLaptop {
		t.Errorf("currentProfile = %q, want laptop", cfg.CurrentProfile)
	}
	for _, want := range []string{
		`→ made "laptop" the active profile, as asked`,
		"kuberecord config set-profile laptop --backend local --path /archives/kuberecord " +
			"--" + options.FlagUse,
	} {
		if !strings.Contains(errOut.String(), want) {
			t.Errorf("stderr never says %q:\n%s", want, errOut)
		}
	}
	// A flag nobody typed is not named. The line above reaches this transcript
	// from a "yes" at a question, and the write reports it in words that are true
	// on both routes.
	if strings.Contains(errOut.String(), "as --"+options.FlagUse+" asked") {
		t.Errorf("the write named a flag this run never carried:\n%s", errOut)
	}
	if strings.Contains(errOut.String(), "config use-profile laptop") {
		t.Errorf("the next step was printed for something already done:\n%s", errOut)
	}
}

// TestTheWizardDoesNotAskWhatItCannotHonour covers both states with no decision
// in them.
//
// A question offered, answered, and then disregarded is worse than no question
// (D31), and these are the two ways that could happen here: a first profile is
// activated whatever the answer, and a profile that already answers cannot be made
// to answer more — "no" would not deactivate it.
func TestTheWizardDoesNotAskWhatItCannotHonour(t *testing.T) {
	const question = "Make this the active profile?"

	t.Run("the only profile in an empty file", func(t *testing.T) {
		path := wizardHome(t)

		wizard, errOut := scriptedWizard(t, "n", "local", "/archives/kuberecord", "")
		if err := wizard.run(t.Context(), "laptop"); err != nil {
			t.Fatalf("the wizard failed: %v\n%s", err, errOut)
		}
		if strings.Contains(errOut.String(), question) {
			t.Errorf("a write with nothing to displace asked whether to displace it:\n%s", errOut)
		}
		if !strings.Contains(errOut.String(), `→ made "laptop" the active profile (it is the only one)`) {
			t.Errorf("the activation nobody asked for was not reported, or not explained:\n%s", errOut)
		}
		cfg, err := resolve.LoadConfig(path)
		if err != nil {
			t.Fatalf("resolve.LoadConfig: %v", err)
		}
		if cfg.CurrentProfile != profileLaptop {
			t.Errorf("currentProfile = %q, want laptop", cfg.CurrentProfile)
		}
	})

	t.Run("the profile that already answers", func(t *testing.T) {
		path := wizardHome(t)
		seedActiveProfile(t, path)

		wizard, errOut := scriptedWizard(t, "n", "local", "/archives/elsewhere", "")
		if err := wizard.run(t.Context(), "already-chosen"); err != nil {
			t.Fatalf("the wizard failed: %v\n%s", err, errOut)
		}
		if strings.Contains(errOut.String(), question) {
			t.Errorf("the wizard asked whether to activate the profile that already answers:\n%s", errOut)
		}
		// What it says instead: the stanza just replaced is the live one, which is
		// the fact the confirmation above cannot carry.
		if !strings.Contains(errOut.String(),
			`→ "already-chosen" is the active profile: this stanza is what the next command reads`) {
			t.Errorf("rewriting the live profile did not say that it is live:\n%s", errOut)
		}
		if strings.Contains(errOut.String(), "config use-profile already-chosen") {
			t.Errorf("the next step was printed for a profile that is already active:\n%s", errOut)
		}
	})
}

// TestUseAnswersTheLastQuestionBeforeItIsAsked.
//
// --use names no field, so an invocation carrying it alone still reaches the
// questions — and the one question it has already answered is not asked again. The
// activation is reported by the write, so nothing about it is silent.
func TestUseAnswersTheLastQuestionBeforeItIsAsked(t *testing.T) {
	path := wizardHome(t)
	seedActiveProfile(t, path)

	wizard, errOut := scriptedWizard(t, "n", "local", "/archives/kuberecord", "")
	wizard.activate = true
	if err := wizard.run(t.Context(), "laptop"); err != nil {
		t.Fatalf("the wizard failed: %v\n%s", err, errOut)
	}

	if strings.Contains(errOut.String(), "Make this the active profile?") {
		t.Errorf("--%s was given and the question was asked anyway:\n%s", options.FlagUse, errOut)
	}
	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}
	if cfg.CurrentProfile != profileLaptop {
		t.Errorf("currentProfile = %q, want laptop: --%s was given", cfg.CurrentProfile, options.FlagUse)
	}
	if !strings.Contains(errOut.String(), `→ made "laptop" the active profile, as asked`) {
		t.Errorf("the activation was not reported:\n%s", errOut)
	}
}

// TestSetProfileWithOnlyUseStillAsks is the mode-selection half of the same
// property, through the whole binary.
//
// --use says what to do with the active pointer and nothing about what to write,
// so an invocation carrying it alone has named no field. Counting it as "the user
// has said what they want" would send it to the flag path and refuse it for a
// missing --backend it never claimed to carry; what it gets instead is the
// refusal a flagless invocation off a terminal gets, which names both flag routes.
func TestSetProfileWithOnlyUseStillAsks(t *testing.T) {
	wizardHome(t)

	streams, _, errOut := wizardStreams()
	code := Run([]string{options.StandaloneName, "config", "set-profile", "laptop",
		"--" + options.FlagUse}, streams)
	if code != exit.UsageError {
		t.Fatalf("exited %d, want %d: --%s names no field, so this invocation is one that "+
			"wanted to be asked", code, exit.UsageError, options.FlagUse)
	}
	if !strings.Contains(errOut.String(), "standard input is not a terminal") {
		t.Errorf("the refusal is not the one a flagless invocation gets:\n%s", errOut)
	}
	if strings.Contains(errOut.String(), "--"+options.FlagBackend+" \"\"") {
		t.Errorf("--%s was treated as having said what to write:\n%s", options.FlagUse, errOut)
	}
}

// seedActiveProfile puts a profile in the file and makes it the active one.
func seedActiveProfile(t *testing.T, path string) {
	t.Helper()

	if err := resolve.SaveConfig(path, &resolve.Config{
		CurrentProfile: "already-chosen",
		Profiles: map[string]resolve.Profile{
			"already-chosen": {
				Backend: resolve.BackendLocal,
				Local:   &resolve.LocalProfile{Path: "/archives/elsewhere"},
			},
		},
	}); err != nil {
		t.Fatalf("seeding the configuration file: %v", err)
	}
}

// emptyClusterFixture is a cluster whose CRDs are installed and whose sinks are
// none.
//
// The distinction matters to the branch it drives: a cluster that can be listed
// and holds nothing is the case that must be said out loud and carried on from,
// rather than one where the listing itself failed.
func emptyClusterFixture(t *testing.T) (*resolve.BackendResolver, genericiooptions.IOStreams, string) {
	t.Helper()

	path := wizardHome(t)
	gvr := func(resource string) schema.GroupVersionResource {
		return schema.GroupVersionResource{
			Group: v1alpha1.GroupVersion.Group, Version: v1alpha1.GroupVersion.Version, Resource: resource,
		}
	}
	streams := genericiooptions.IOStreams{
		In: strings.NewReader(""), Out: &strings.Builder{}, ErrOut: &strings.Builder{},
	}
	return &resolve.BackendResolver{
		Streams:    streams,
		InvokedAs:  options.StandaloneName,
		Config:     &resolve.Config{},
		ConfigPath: path,
		Clients: &resolve.Clients{
			Dynamic: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
				runtime.NewScheme(), map[schema.GroupVersionResource]string{
					gvr("clickhousesinks"): resolve.KindClickHouseSink + "List",
					gvr("s3sinks"):         resolve.KindS3Sink + "List",
				}),
			Typed: k8sfake.NewClientset(),
		},
	}, streams, path
}

// TestTheWizardTranscriptReadsAsAConversation pins the whole rendering.
//
// It is the golden-file assertion for this command, held inline rather than under
// testdata because the transcript is short and because the thing most likely to
// break it is an edit to the file three directories up rather than a data change:
// a reviewer seeing the diff beside the questions is seeing it in the right place.
//
// The case includes a refused answer on purpose. The re-ask is the mechanism the
// whole design rests on, and it is the one part of the rendering a reader has to
// be able to recognise instantly — which under NO_COLOR and in a redirected
// stream means the marker, since there is no colour left to carry it.
func TestTheWizardTranscriptReadsAsAConversation(t *testing.T) {
	path := wizardHome(t)

	wizard, errOut := scriptedWizard(t,
		"n",                    // do not read the settings from a sink
		"3",                    // the third backend: local
		"/archives/kuberecord", // path
		"/oops",                // prefix — refused, and re-asked
		archivePrefix,          // prefix
	)
	if err := wizard.run(t.Context(), "laptop"); err != nil {
		t.Fatalf("the wizard failed: %v\n%s", err, errOut)
	}

	// The configuration file's location is the one part of this that differs
	// between two machines, and it is announced on purpose (a reader with two
	// dotfile checkouts needs to know which one was written).
	got := strings.ReplaceAll(errOut.String(), path, "<config>")
	if got != wantTranscript {
		t.Errorf("the wizard's rendering changed.\n--- want ---\n%s\n--- got ---\n%s", wantTranscript, got)
	}
}

// wantTranscript is that rendering.
//
// The answers do not appear in it, and that is not a bug in the fixture: this
// stream is only what the wizard wrote, while a terminal interleaves the user's
// own typing and the newline their return key produces. It is why two lines here
// run together where a real session shows a break.
const wantTranscript = `
Writing a profile: where this command reads recorded history from.
A profile never holds a password — it names an environment variable or a file.
Ctrl-D at any question stops, and writes nothing.

Read the settings from a sink custom resource in this cluster? [Y/n]
> 
Which backend this profile reads. One of: clickhouse, s3, local.
  1) clickhouse — the frozen v1 schema in a ClickHouse instance
  2) s3 — a jsonl-v1 archive in an S3-compatible bucket
  3) local — a jsonl-v1 archive in a local directory
> [clickhouse] 
Directory holding a local archive — the one containing format=jsonl-v1/.
> 
The archive's key prefix within the bucket or directory, with no leading or trailing slash.
> 
! local.prefix "/oops" has a leading or trailing slash: it is a path fragment within the directory, not a path

The archive's key prefix within the bucket or directory, with no leading or trailing slash.
> → wrote profile "laptop" in <config>
→ made "laptop" the active profile (it is the only one)

The same thing without the questions:
  kuberecord config set-profile laptop --backend local --path /archives/kuberecord --prefix kuberecord --use
`

// wizardGoldens is the testdata subdirectory the discovery branch's transcripts
// live in.
//
// The same directory the flag path's confirmations use, because they are two
// routes to one write and a reviewer changing the order of its lines should see
// every file that pins the order in one diff.
const wizardGoldens = "config-profile"

// TestTheWizardSaysWhatItWroteBeforeSayingThatItWrote is Task 18.5's first item,
// and the whole of it is the sequence.
//
// Field use produced this:
//
//	> [127.0.0.1:9000]
//	→ wrote profile "test-2" in …/config.yaml
//	ClickHouseSink/default records …svc:9000.
//	…
//	→ to make it the active profile: …
//	The same thing without the questions: …
//
// The first `→` is the one line in that block that looks like an ending, and
// everything a reader still has to do was underneath it: the port-forward the
// profile now expects, and which principal's password the variable it names has to
// hold. A reader who stopped there had been told the write succeeded and nothing
// about what would make it work.
//
// So the order is explanation, then the confirmation, then where the active
// pointer stands, then the equivalent command — and this file is the assertion,
// because the ordering is the deliverable rather than a wording preference. The
// positional check beside it is what says which property the golden carries: a
// reordering that a future edit made would otherwise be a golden diff a reviewer
// could accept without noticing what it meant.
//
// Both colour modes, and the plain file is where the sequence is legible. The
// coloured one is what pins that the port-forward line is still the block's one
// emphasised line and that nothing in the reordering spent a tier.
func TestTheWizardSaysWhatItWroteBeforeSayingThatItWrote(t *testing.T) {
	for mode, colorize := range map[string]bool{"": false, "-color": true} {
		t.Run("transcript"+mode, func(t *testing.T) {
			resolver, _, path := fromSinkFixture(t)
			// A profile already answers, so activation is a decision to be asked
			// about and "no" is an answer that stands. Written into an empty file
			// this profile would be activated regardless (D38's carve-out) and the
			// `use-profile` line — one of the four steps whose order is under test —
			// would correctly never be printed.
			seedActiveProfile(t, path)

			// y — read the settings from a sink; 1 — the ClickHouseSink; then the
			// offered address, the offered user, the environment and the offered
			// variable name, all by pressing return; and n, do not activate it,
			// which is the answer that leaves the `use-profile` line to be printed.
			wizard, errOut := colouredWizard(t, colorize, "y", "1", "", "", "", "", "n")
			wizard.newResolver = func() (*resolve.BackendResolver, error) { return resolver, nil }

			if err := wizard.run(t.Context(), "local"); err != nil {
				t.Fatalf("the wizard failed: %v\n%s", err, errOut)
			}

			got := strings.ReplaceAll(errOut.String(), path, "<config>")
			assertInternalGolden(t, wizardGoldens, "wizard-from-sink"+mode, got)

			// The property, stated as positions rather than left to the file: the
			// explanation of what was written precedes the confirmation that it was,
			// and the confirmation is the last thing said about the write.
			plain := sgrSequence.ReplaceAllString(got, "")
			explanation := strings.Index(plain, internalAddr+".")
			confirmation := strings.Index(plain, `→ wrote profile "local"`)
			nextStep := strings.Index(plain, "→ to make it the active profile")
			equivalent := strings.Index(plain, "The same thing without the questions")
			for _, step := range []struct {
				name string
				at   int
			}{
				{"the explanation", explanation},
				{"the write confirmation", confirmation},
				{"the activation route", nextStep},
				{"the equivalent command", equivalent},
			} {
				if step.at < 0 {
					t.Fatalf("%s is not in the transcript:\n%s", step.name, plain)
				}
			}
			if explanation >= confirmation || confirmation >= nextStep || nextStep >= equivalent {
				t.Errorf("the transcript reads out of order (explanation %d, confirmation %d, "+
					"activation %d, equivalent %d):\n%s",
					explanation, confirmation, nextStep, equivalent, plain)
			}
		})
	}
}

// The read-only engineer's path, which is the shape most people who need a
// profile actually have (D7): a kubeconfig that can list custom resources and not
// read the operator's Secrets.
//
// Everything below asserts one property from two directions — a Secret this
// kubeconfig may not read is not a discovery failure, and every other way
// ProfileFromSink can fail still is. The carve-out is one carve-out, and the tests
// that matter are the ones holding its edges.

// forbidSecretReads makes every Secret read on a fixture's cluster forbidden.
//
// Prepended rather than seeded as an absent Secret, because the two are different
// states and only this one is the read-only engineer's: the Secret exists, the
// operator reads it, and this kubeconfig may not.
func forbidSecretReads(t *testing.T, resolver *resolve.BackendResolver) {
	t.Helper()

	typed, ok := resolver.Clients.Typed.(*k8sfake.Clientset)
	if !ok {
		t.Fatalf("the fixture's typed client is %T, not one a reactor can be added to", resolver.Clients.Typed)
	}
	typed.PrependReactor("get", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(corev1.Resource("secrets"), sinkSecret, errors.New("no"))
	})
}

// TestTheWizardSurvivesASecretItCannotRead is Task 17.1, and the case is the
// common one.
//
// Four questions have been answered by the time the Secret is read, and the read
// confirms a value the profile was never going to store — the address, the
// database and the user are already in hand from the custom resource. So the
// answer to a forbidden read is one more question rather than a discarded
// conversation, and the paragraph above it says why the question is being asked at
// all (Invariant 4).
func TestTheWizardSurvivesASecretItCannotRead(t *testing.T) {
	resolver, _, path := fromSinkFixture(t)
	forbidSecretReads(t, resolver)
	seedActiveProfile(t, path)

	// y — read it from a sink; 1 — the ClickHouseSink; then the offered address,
	// the offered user, the environment, and the offered variable name, all by
	// pressing return; and no, do not make this the active profile.
	wizard, errOut := scriptedWizard(t, "y", "1", "", "", "", "", "n")
	wizard.newResolver = func() (*resolve.BackendResolver, error) { return resolver, nil }

	if err := wizard.run(t.Context(), "local"); err != nil {
		t.Fatalf("a Secret this kubeconfig may not read ended the wizard: %v\n%s", err, errOut)
	}

	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("the wizard did not write a loadable configuration: %v\n%s", err, errOut)
	}
	stanza := cfg.Profiles["local"].ClickHouse
	if stanza == nil {
		t.Fatalf("the file holds no ClickHouse profile named local: %+v", cfg)
	}
	// Complete and usable: every field the custom resource answered is still
	// there, and the one the Secret would have confirmed is the reader's own.
	want := resolve.ClickHouseProfile{
		Addr:        "127.0.0.1:9000",
		Database:    resolve.DefaultClickHouseDatabase,
		Username:    sinkUsername,
		PasswordEnv: resolve.DefaultPasswordEnv,
	}
	if *stanza != want {
		t.Errorf("the profile is %+v, want %+v", *stanza, want)
	}

	for _, said := range []string{
		"Read the connection settings from ClickHouseSink/default.",
		"Cannot read its Secret (forbidden) — that is fine: a profile stores where",
		"your password lives, not the operator's.",
		// Named for the user answered one question earlier, because the two are one
		// credential pair (D37).
		"Where does " + sinkUsername + "'s password come from?",
		// And the equivalent reproduces what was assembled, password source
		// included: without it the printed line would write a profile whose
		// password came from somewhere the reader did not choose.
		"  kuberecord config set-profile local --from-sink ClickHouseSink/default " +
			"--addr 127.0.0.1:9000 --password-env " + resolve.DefaultPasswordEnv,
	} {
		if !strings.Contains(errOut.String(), said) {
			t.Errorf("stderr never says %q:\n%s", said, errOut)
		}
	}
	// The third answer of the typed path's question is not offered here, because
	// resolve.ProfileOverrides cannot express it and the derivation would supply
	// the default variable anyway — a choice taken and disregarded (D31).
	if strings.Contains(errOut.String(), "a server with no password") {
		t.Errorf("the derived branch offered an answer it cannot honour:\n%s", errOut)
	}
}

// TestTheEquivalentReproducesAProfileDerivedWithoutItsSecret closes the same loop
// TestTheEquivalentCommandReproducesTheProfile closes, on the branch this task
// adds a flag to.
//
// The printed line gained --password-env from an answer rather than from a flag,
// which is exactly the way a printed command drifts from the profile it claims to
// reproduce. So it is parsed back out of stderr and put through the derivation the
// flag path performs.
func TestTheEquivalentReproducesAProfileDerivedWithoutItsSecret(t *testing.T) {
	resolver, _, path := fromSinkFixture(t)
	forbidSecretReads(t, resolver)

	wizard, errOut := scriptedWizard(t, "y", "1", "127.0.0.1:19000", "", "file", "/run/secrets/ch")
	wizard.newResolver = func() (*resolve.BackendResolver, error) { return resolver, nil }

	if err := wizard.run(t.Context(), "local"); err != nil {
		t.Fatalf("the wizard failed: %v\n%s", err, errOut)
	}
	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}

	replayed := replayFromSink(t, resolver, equivalentArgs(t, errOut.String()))
	if *replayed.Profile.ClickHouse != *cfg.Profiles["local"].ClickHouse {
		t.Errorf("the printed command derives\n%+v\nwhere the questions wrote\n%+v",
			replayed.Profile.ClickHouse, cfg.Profiles["local"].ClickHouse)
	}
}

// replayFromSink runs a printed `config set-profile --from-sink` line through the
// derivation the flag path runs.
//
// The flags are parsed from profileFieldFlags — the table the command itself
// registers from, which TestEveryProfileFlagIsInTheTable pins — and mapped through
// profileFields.overrides, the one function that says which of them --from-sink
// can carry. So a printed flag the command does not have, or one the flag path
// would not apply, fails here rather than in somebody's CI job.
//
// The whole binary is not used because it would build a resolver from a kubeconfig
// this test does not have; the fixture's resolver is the half that differs.
func replayFromSink(t *testing.T, resolver *resolve.BackendResolver, args []string) *resolve.SinkProfile {
	t.Helper()

	if len(args) < 4 || args[1] != "config" || args[2] != "set-profile" {
		t.Fatalf("the printed command is not a config set-profile invocation: %v", args)
	}

	var (
		fields   profileFields
		fromSink string
		use      bool
	)
	set := pflag.NewFlagSet("replay", pflag.ContinueOnError)
	set.StringVar(&fromSink, options.FlagFromSink, "", "")
	// Accepted and then unused, because this replay derives a stanza and --use
	// decides nothing about one — it moves the file's active pointer. Registering
	// it anyway is the point: the printed line has to parse against the flags the
	// command really carries, so a flag the wizard prints and the command does not
	// have fails here.
	set.BoolVar(&use, options.FlagUse, false, "")
	for _, field := range profileFieldFlags {
		switch {
		case field.str != nil:
			set.StringVar(field.str(&fields), field.name, "", "")
		case field.boolean != nil:
			set.BoolVar(field.boolean(&fields), field.name, false, "")
		}
	}
	if err := set.Parse(args[4:]); err != nil {
		t.Fatalf("the printed command does not parse: %v", err)
	}

	ref, err := resolve.ParseSinkRef(options.FlagFromSink, fromSink)
	if err != nil {
		t.Fatalf("the printed --%s does not parse: %v", options.FlagFromSink, err)
	}
	derived, err := resolver.ProfileFromSink(t.Context(), ref, fields.overrides())
	if err != nil {
		t.Fatalf("the printed command fails to derive: %v", err)
	}
	return derived
}

// TestEveryOtherDerivationFailureStillEndsTheWizard holds the other edge.
//
// The carve-out is the Secret read and nothing else. Each case below is a failure
// of the thing the user named in the menu, and a wizard that carried on past one
// would write a profile describing a sink it never read.
//
// The object-store guard — refusing every override for an S3Sink — is absent
// because it is unreachable from here rather than because it is exempt: the wizard
// asks an archive nothing, so it never hands ProfileFromSink an override to refuse.
// resolve's own table covers it.
func TestEveryOtherDerivationFailureStillEndsTheWizard(t *testing.T) {
	badSpec := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(),
		"kind":       resolve.KindClickHouseSink,
		"metadata":   map[string]any{"name": "default"},
		// An addr that is not a string: the CRD forbids it, and something that
		// reached the API server another way does not.
		"spec": map[string]any{"connection": map[string]any{"addr": int64(9000)}},
	}}

	for _, tc := range []struct {
		name  string
		react clienttesting.ReactionFunc
		want  string
	}{
		{
			name: "a sink the menu listed and this kubeconfig may not read",
			react: func(clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewForbidden(
					schema.GroupResource{Group: v1alpha1.GroupVersion.Group, Resource: "clickhousesinks"},
					"default", errors.New("no"))
			},
			want: "cannot read ClickHouseSink/default (forbidden)",
		},
		{
			name: "a sink that is gone by the time it is chosen",
			react: func(clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewNotFound(
					schema.GroupResource{Group: v1alpha1.GroupVersion.Group, Resource: "clickhousesinks"},
					"default")
			},
			want: "cannot read ClickHouseSink/default",
		},
		{
			name:  "a sink whose spec does not decode",
			react: func(clienttesting.Action) (bool, runtime.Object, error) { return true, badSpec, nil },
			want:  "decoding ClickHouseSink/default",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver, _, path := fromSinkFixture(t)
			dynamic, ok := resolver.Clients.Dynamic.(*dynamicfake.FakeDynamicClient)
			if !ok {
				t.Fatalf("the fixture's dynamic client is %T", resolver.Clients.Dynamic)
			}
			dynamic.PrependReactor("get", "clickhousesinks", tc.react)

			wizard, errOut := scriptedWizard(t, "y", "1", "", "", "", "")
			wizard.newResolver = func() (*resolve.BackendResolver, error) { return resolver, nil }

			err := wizard.run(t.Context(), "local")
			if err == nil {
				t.Fatalf("the wizard carried on past a sink it could not read:\n%s", errOut)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the wizard failed with %q, want it to name %q", err, tc.want)
			}
			if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
				t.Errorf("a failed derivation wrote %s anyway", path)
			}
		})
	}
}

// The credential pair, asked as one decision (Task 18.1, D37).
//
// The discovery branch used to ask nothing about credentials when the Secret was
// readable, which is the ordinary case. It took the user from the custom resource
// and the variable from resolve.DefaultPasswordEnv, and then printed advice
// telling the reader to export a *read-only* user's password into that variable —
// beside a stanza saying `username: kuberecord`. Doing both authenticates as the
// sink's writer with somebody else's password, so the advice could not be
// followed at all.
//
// The two tests below are the two answers to the question that fixes it. Between
// them they assert the property the AC is written around: no rendering of this
// block recommends a principal other than the one written.

// TestTheWizardKeepsTheSinksUserWhenNobodyNamesAnother.
//
// Pressing return through both questions has to write exactly what --from-sink
// wrote before either existed. This adds a question, not a requirement — and the
// stanza is the evidence.
func TestTheWizardKeepsTheSinksUserWhenNobodyNamesAnother(t *testing.T) {
	resolver, _, path := fromSinkFixture(t)

	// The address, the user, the environment and the variable, all defaults.
	wizard, errOut := scriptedWizard(t, "y", "1", "", "", "", "")
	wizard.newResolver = func() (*resolve.BackendResolver, error) { return resolver, nil }

	if err := wizard.run(t.Context(), "local"); err != nil {
		t.Fatalf("the wizard failed: %v\n%s", err, errOut)
	}
	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}
	want := resolve.ClickHouseProfile{
		Addr:        "127.0.0.1:9000",
		Database:    resolve.DefaultClickHouseDatabase,
		Username:    sinkUsername,
		PasswordEnv: resolve.DefaultPasswordEnv,
	}
	if stanza := cfg.Profiles["local"].ClickHouse; stanza == nil || *stanza != want {
		t.Errorf("the stanza is %+v, want the one --from-sink writes with no flags: %+v", stanza, want)
	}

	for _, said := range []string{
		// The consequence, above the question, so the answer is an informed one.
		"ClickHouseSink/default authenticates as " + sinkUsername + ", which can write to the\naudit trail.",
		// The pair, visible: the second question names the answer to the first.
		"Where does " + sinkUsername + "'s password come from?",
		// And the write's own explanation, which still has the recommendation to
		// make because this profile has not taken it.
		"That user is the sink's own writer, so this profile can write to the audit trail.",
		"Give --username a read-only user instead",
	} {
		if !strings.Contains(errOut.String(), said) {
			t.Errorf("stderr never says %q:\n%s", said, errOut)
		}
	}
}

// TestTheWizardRecordsTheReadOnlyUserItWasGiven is the other answer, and it holds
// the AC's negative.
//
// Naming a different user is taking the advice, so the advice stops being printed:
// repeating it above a stanza that has already followed it is the same defect in
// the other direction. What is left is the pair that was recorded.
func TestTheWizardRecordsTheReadOnlyUserItWasGiven(t *testing.T) {
	resolver, _, path := fromSinkFixture(t)
	seedActiveProfile(t, path)

	// The address by default, then a read-only user, then the environment and the
	// variable it was offered for that user, and no to the last question.
	wizard, errOut := scriptedWizard(t, "y", "1", "", readOnlyUser, "", "", "n")
	wizard.newResolver = func() (*resolve.BackendResolver, error) { return resolver, nil }

	if err := wizard.run(t.Context(), "local"); err != nil {
		t.Fatalf("the wizard failed: %v\n%s", err, errOut)
	}
	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}
	want := resolve.ClickHouseProfile{
		Addr:     "127.0.0.1:9000",
		Database: resolve.DefaultClickHouseDatabase,
		Username: readOnlyUser,
		// A variable of its own, so a second profile naming a second principal does
		// not overwrite this one's password in the same shell.
		PasswordEnv: readOnlyPasswordEnv,
	}
	if stanza := cfg.Profiles["local"].ClickHouse; stanza == nil || *stanza != want {
		t.Errorf("the stanza is %+v, want %+v", stanza, want)
	}

	for _, said := range []string{
		"Where does " + readOnlyUser + "'s password come from?",
		"> [" + readOnlyPasswordEnv + "]",
		readOnlyUser + "'s password comes from $" + readOnlyPasswordEnv,
		// The flag line reproduces both halves of the pair.
		"  kuberecord config set-profile local --from-sink ClickHouseSink/default " +
			"--addr 127.0.0.1:9000 --username " + readOnlyUser + " --password-env " + readOnlyPasswordEnv,
	} {
		if !strings.Contains(errOut.String(), said) {
			t.Errorf("stderr never says %q:\n%s", said, errOut)
		}
	}
	// The AC's negative, and the reason this test exists: the advice has been
	// taken, so the explanation printed after the write does not repeat it.
	//
	// The question's own preamble — "That user can write to the audit trail" — is
	// deliberately not what is asserted against. It names the sink's user *above*
	// the question, before any answer exists, and it is the sentence that makes
	// naming another user an informed choice rather than a guess. Only the block
	// printed after the write knows which principal was chosen.
	for _, gone := range []string{
		"That user is the sink's own writer, so this profile can write to the audit trail.",
		"Give --username a read-only user instead",
	} {
		if strings.Contains(errOut.String(), gone) {
			t.Errorf("the recommendation %q was repeated to somebody who had taken it:\n%s", gone, errOut)
		}
	}
}

// profileLaptop is the profile the questions write in the cases that read the
// file back.
//
// A constant because those cases compare the file's active pointer and its
// profiles map against it, and a name compared against a literal in five places
// is a name one of them can be quietly wrong about. The literal stays where it is
// an *answer* the wizard was scripted with or a fragment of a line it printed,
// which are strings the reader is meant to see spelled out.
const profileLaptop = "laptop"

// readOnlyUser is the principal the pair tests name, and readOnlyPasswordEnv the
// variable resolve.ReaderPasswordEnv derives for it.
//
// Spelled out rather than computed, because a test that built the expectation
// with the function under test would pass for any sanitiser at all.
const (
	readOnlyUser        = "kuberecord_ro"
	readOnlyPasswordEnv = resolve.DefaultPasswordEnv + "_KUBERECORD_RO"
)

// TestTheEquivalentReproducesAProfileReadingAsAnotherUser closes the loop the two
// tests above open, on the flag this task adds to the printed line.
//
// --username is the half of the pair the equivalent command gained, and a printed
// line carrying one half would write a profile that authenticates as nobody. So it
// is parsed back out of stderr and put through the derivation the flag path runs.
func TestTheEquivalentReproducesAProfileReadingAsAnotherUser(t *testing.T) {
	resolver, _, path := fromSinkFixture(t)

	wizard, errOut := scriptedWizard(t, "y", "1", "", readOnlyUser, "", "")
	wizard.newResolver = func() (*resolve.BackendResolver, error) { return resolver, nil }

	if err := wizard.run(t.Context(), "local"); err != nil {
		t.Fatalf("the wizard failed: %v\n%s", err, errOut)
	}
	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}

	replayed := replayFromSink(t, resolver, equivalentArgs(t, errOut.String()))
	if *replayed.Profile.ClickHouse != *cfg.Profiles["local"].ClickHouse {
		t.Errorf("the printed command derives\n%+v\nwhere the questions wrote\n%+v",
			replayed.Profile.ClickHouse, cfg.Profiles["local"].ClickHouse)
	}
}

// TestFromSinkWithAUsernameAgreesWithTheWizard is D33 asserted directly.
//
// The prompting layer is a layer over the flag path, so a field the questions can
// set is a field the flags can set, and the two have to produce the same stanza
// from the same answer. If --from-sink could not express which user a profile
// reads as, the wizard would have invented a field — and the equivalent command it
// prints would be a line nobody can run.
func TestFromSinkWithAUsernameAgreesWithTheWizard(t *testing.T) {
	resolver, _, path := fromSinkFixture(t)

	wizard, errOut := scriptedWizard(t, "y", "1", "", readOnlyUser, "", "")
	wizard.newResolver = func() (*resolve.BackendResolver, error) { return resolver, nil }
	if err := wizard.run(t.Context(), "local"); err != nil {
		t.Fatalf("the wizard failed: %v\n%s", err, errOut)
	}
	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}

	// The flag path's own call, with the overrides `--from-sink <ref> --addr
	// 127.0.0.1:9000 --username kuberecord_ro` produces and nothing said about the
	// password: the derivation supplies the same variable the question offered.
	fields := profileFields{Addr: "127.0.0.1:9000", Username: readOnlyUser}
	derived, err := resolver.ProfileFromSink(t.Context(),
		resolve.SinkRef{Kind: resolve.KindClickHouseSink, Name: "default"}, fields.overrides())
	if err != nil {
		t.Fatalf("--%s --%s: %v", options.FlagFromSink, options.FlagUsername, err)
	}
	if *derived.Profile.ClickHouse != *cfg.Profiles["local"].ClickHouse {
		t.Errorf("--%s --%s derives\n%+v\nwhere the questions wrote\n%+v",
			options.FlagFromSink, options.FlagUsername,
			derived.Profile.ClickHouse, cfg.Profiles["local"].ClickHouse)
	}
	// And it says the same thing about it, which is the half a stanza comparison
	// cannot see: the recommendation is gone on both routes or on neither.
	if strings.Contains(derived.Explain(false), "can write to the audit trail") {
		t.Errorf("the flag path warns about a credential the questions did not:\n%s", derived.Explain(false))
	}
}
