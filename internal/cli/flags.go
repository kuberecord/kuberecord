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
	goflag "flag"
	"fmt"
	"io"
	"strconv"

	"github.com/spf13/pflag"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/klog/v2"
)

// Flag names that other parts of the CLI, and its tests, refer to by name.
//
// D21 is the reason FlagClusterID is spelled out here rather than typed inline
// at its one registration site: --cluster is kubectl's kubeconfig cluster entry
// and belongs to ConfigFlags, while a kuberecord cluster identity is the
// cluster_id column of the frozen schema. They are different things that would
// read as the same word, the collision would be discovered by a user rather
// than by us, and it is expensive to change after release. Naming both constants
// lets a test assert they stay distinct.
const (
	// FlagClusterID selects which recorded cluster's history to read.
	FlagClusterID = "cluster-id"

	// FlagKubeconfigCluster is cli-runtime's, not ours. It is named here only so
	// the distinctness test can refer to it without spelling a literal that
	// might drift from what ConfigFlags registers.
	FlagKubeconfigCluster = "cluster"

	// FlagOutput selects the rendering.
	FlagOutput = "output"

	// FlagColor selects colour behaviour.
	FlagColor = "color"

	// FlagSink names a configured sink to read through, as kind/name.
	FlagSink = "sink"

	// FlagSource names a location to read directly, bypassing sink discovery.
	FlagSource = "source"

	// FlagProfile selects a stanza in the CLI's configuration file.
	FlagProfile = "profile"

	// FlagVerbosity is spelled "v" with a "-v" shorthand, exactly as kubectl
	// spells it. The long form reads oddly in help output and is kept anyway:
	// muscle memory for `-v 6` is worth more than the tidier `--verbose`, and a
	// plugin that diverges from its host on the one flag people use while
	// debugging is a plugin people debug twice.
	FlagVerbosity = "v"
)

// GlobalFlags is the flag surface every kuberecord command shares.
//
// It is one struct rather than package-level variables so that a test can build
// a root command, drive it, and inspect what was parsed without touching global
// state — and so that two roots can exist at once, which is what makes the
// stdout/stderr assertions in this package's tests cheap to write.
type GlobalFlags struct {
	// ConfigFlags is kubectl's own kubeconfig surface: --kubeconfig, --context,
	// --namespace/-n, --cluster, --user, --as and the rest, with kubectl-identical
	// semantics because it is kubectl's implementation rather than a
	// reimplementation of it. Anything a user knows about connecting kubectl to a
	// cluster therefore transfers unchanged.
	ConfigFlags *genericclioptions.ConfigFlags

	// ClusterID is the kuberecord cluster identity (D21) — the cluster_id column
	// of the frozen schema, not a kubeconfig entry. Empty means "resolve it",
	// which Task 11.2 gives a chain for; this task only establishes the flag.
	ClusterID string

	// Output is the rendering, validated at parse time.
	Output OutputFormat

	// Color is the colour mode, validated at parse time.
	Color ColorMode

	// Sink names a configured sink as kind/name.
	Sink string

	// Source names a location to read directly — a local directory or a bucket
	// URL — which is the zero-infrastructure path (D18): it removes both the
	// operator and any sink CR from the evaluation route.
	Source string

	// Profile selects a stanza in the CLI's configuration file.
	Profile string

	// Verbosity is the klog verbosity level. It is applied to klog rather than
	// interpreted locally so that `-v 6` shows the API requests client-go is
	// making, which is the reason anyone reaches for the flag.
	Verbosity int
}

// NewGlobalFlags returns the shared flag surface with its defaults in place.
//
// The kubeconfig flags are built with a persistent config so that repeated
// lookups within one invocation reuse a single loader — the same choice kubectl
// makes, and the reason --namespace resolution below is cheap to call more than
// once.
func NewGlobalFlags() *GlobalFlags {
	return &GlobalFlags{
		ConfigFlags: genericclioptions.NewConfigFlags(true),
		Output:      OutputTable,
		Color:       ColorAuto,
	}
}

// AddFlags registers the whole global surface on flags.
//
// Registration order matters only for help output, where cli-runtime's block
// comes first because it is the vocabulary a kubectl user already has.
func (g *GlobalFlags) AddFlags(flags *pflag.FlagSet) {
	g.ConfigFlags.AddFlags(flags)

	flags.StringVar(&g.ClusterID, FlagClusterID, g.ClusterID,
		"The kuberecord cluster identity whose history to read (the cluster_id column). "+
			"Distinct from --cluster, which selects a kubeconfig cluster entry.")
	flags.VarP(&g.Output, FlagOutput, "o",
		fmt.Sprintf("Output format. One of: %s.", joinValues(outputFormats)))
	flags.Var(&g.Color, FlagColor,
		fmt.Sprintf("When to colourise output. One of: %s. NO_COLOR is honoured under auto.",
			joinValues(colorModes)))
	flags.StringVar(&g.Sink, FlagSink, g.Sink,
		"Read through a configured sink, as kind/name (for example ClickHouseSink/default).")
	flags.StringVar(&g.Source, FlagSource, g.Source,
		"Read directly from a location, bypassing sink discovery: a local directory or an s3:// URL.")
	flags.StringVar(&g.Profile, FlagProfile, g.Profile,
		"Use this profile from the kuberecord configuration file.")
	flags.IntVarP(&g.Verbosity, FlagVerbosity, "v", g.Verbosity,
		"Number for the log level verbosity of diagnostics, which are written to stderr.")
}

// Namespace resolves the namespace this invocation acts in.
//
// It goes through ToRawKubeConfigLoader().Namespace() rather than reading the
// --namespace flag directly, because the flag is only one of the inputs: with no
// flag the answer is the current context's namespace, and with no context it is
// "default". Reading the flag would silently change what `-n`-less commands mean
// for every engineer whose kubeconfig has a namespace set, which is most of them.
//
// The second return value of the loader — whether the namespace was set by the
// flag — is deliberately dropped here and re-derived by callers that need it
// from the flag's own Changed bit, so that this function has one meaning.
func (g *GlobalFlags) Namespace() (string, error) {
	namespace, _, err := g.ConfigFlags.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return "", fmt.Errorf("resolve namespace from kubeconfig: %w", err)
	}
	return namespace, nil
}

// ApplyVerbosity pushes the parsed -v level into klog, so that the diagnostics
// client-go and cli-runtime emit obey it.
//
// klog's flags are registered onto a throwaway FlagSet rather than onto the
// command's, which is what keeps `--vmodule`, `--log-flush-frequency` and the
// rest of klog's surface out of `kuberecord --help` while still driving the same
// global logger they would. Setting the value through the FlagSet — rather than
// reaching for a package-level setter — is the only supported way to move klog's
// verbosity, and it is why this takes the long way round.
//
// klog writes to the process's standard error by default, which is where
// diagnostics belong and why nothing here redirects it.
//
// Level 0 is not special-cased. Skipping the work for it would save one FlagSet
// per invocation and would make the function unable to lower a verbosity it had
// already raised, which is the one thing a test of it needs to do.
func ApplyVerbosity(level int) error {
	flags := goflag.NewFlagSet("klog", goflag.ContinueOnError)
	flags.SetOutput(io.Discard)
	klog.InitFlags(flags)
	if err := flags.Set("v", strconv.Itoa(level)); err != nil {
		return fmt.Errorf("set klog verbosity to %d: %w", level, err)
	}
	return nil
}
