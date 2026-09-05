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

	// FlagSinkAddr replaces the endpoint the resolution chain arrived at, and
	// nothing else about it (D25).
	//
	// It is named here beside FlagSink because it modifies that step's result
	// rather than adding a step of its own, and because Task 13.1's
	// unreachable-backend message tells the reader to type it. A message and a
	// flag that spelled the name independently would be two spellings of one
	// contract, and the one nobody compiles is the one that drifts.
	FlagSinkAddr = "sink-addr"

	// FlagProfile selects a stanza in the CLI's configuration file.
	FlagProfile = "profile"

	// FlagOperatorNamespace names the namespace the operator runs in, which is
	// where a sink's credentials Secret lives by default (D7) and where the
	// Deployment carrying the cluster identity is found.
	//
	// It exists because the two ways of learning it without asking can both fail
	// in ordinary clusters: searching for the Deployment needs a cluster-wide list
	// that a locked-down cluster will not grant, and the built-in default is only
	// right for an install nobody moved. Without a flag, the user whose search is
	// forbidden *and* whose install is elsewhere has no way out but hand-editing a
	// configuration file.
	FlagOperatorNamespace = "operator-namespace"

	// The window's four names. --since/--until is the primary pair and --from/--to
	// aliases it; see windowFlags for why one bound answers to two names, and why
	// the aliases are the read plane's own spelling rather than a synonym somebody
	// liked. They are constants because three commands register them and the error
	// that reports a conflict between two of them names both.
	FlagSince = "since"
	FlagUntil = "until"
	FlagFrom  = "from"
	FlagTo    = "to"

	// FlagAssumeYes answers the confirmation a wide cold scan asks for.
	//
	// It exists for the same reason `rm -f` does, and it is spelled the way every
	// other tool spells it so that nobody has to look it up while a pipeline is
	// failing. A non-interactive invocation assumes it anyway (see coldscan.Options):
	// the flag is for the interactive user who already knows what they are asking
	// for, not for the script, which must never be able to hang on a prompt.
	FlagAssumeYes = "yes"

	// FlagMaxObjects is the cold scan's circuit breaker.
	//
	// It bounds the *work* rather than the answer, which is the half --limit
	// cannot do against a backend with no index: --limit 100 still reads every
	// object in the window before it can know which hundred are newest. Naming the
	// bound in objects rather than in seconds is deliberate — objects are what the
	// estimate is denominated in, so the number a user types is the number they
	// were shown before they typed it.
	FlagMaxObjects = "max-objects"

	// The per-field flags `config set-profile` carries, one per field of the
	// stanza its --backend selects, plus the one that fills them all in from a
	// sink custom resource.
	//
	// They are constants because --from-sink refuses each of them by name: the
	// custom resource states the database, the bucket and the rest, and a profile
	// that disagreed with it would read somewhere other than where the sink
	// writes. A refusal that spelled the flag independently of its registration
	// would be two spellings of one contract, and the one nobody compiles is the
	// one that drifts — the same reason FlagSinkAddr is named here.
	FlagFromSink       = "from-sink"
	FlagBackend        = "backend"
	FlagAddr           = "addr"
	FlagDatabase       = "database"
	FlagUsername       = "username"
	FlagPasswordEnv    = "password-env"
	FlagPasswordFile   = "password-file"
	FlagTLS            = "tls"
	FlagBucket         = "bucket"
	FlagRegion         = "region"
	FlagEndpoint       = "endpoint"
	FlagForcePathStyle = "force-path-style"
	FlagPrefix         = "prefix"
	FlagPath           = "path"

	// FlagCheck asks whether the resolved backend can actually be reached.
	//
	// Two commands take it — `config resolve`, which reports every step of both
	// resolution chains, and `version`, which reports the four facts those chains
	// produced — and it is one name because it is one question put through one
	// piece of machinery. A build that answered "reachable" under one spelling and
	// something else under another would be two opinions about one backend.
	//
	// It is opt-in on both, and that is the whole design. Inspecting a
	// configuration must not require a reachable backend, because the
	// configuration a user most wants to inspect is the one whose backend cannot
	// be reached (D26); and `version` is the command somebody types when nothing
	// works, so it has to stay instant and offline until it is asked otherwise.
	//
	// It is named here rather than typed at its registration sites because the
	// resolution report withholds the cluster-id chain's last step by name —
	// "--check asks it" — and a message that spelled the flag independently of its
	// registration would be the spelling that drifts.
	FlagCheck = "check"

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

	// SinkAddr replaces the endpoint of a resolved ClickHouse backend — a sink
	// custom resource's spec.connection.addr, or a profile's clickhouse.addr —
	// and replaces nothing else about it (D25). The database, the user, the
	// credentials, the TLS setting and the dial timeout still come from wherever
	// that address came from.
	//
	// It is the ephemeral half of what `config set-profile` writes down: a
	// forwarded port for one invocation, for a colleague's cluster or a CI job,
	// with no file on disk touched. The persistent case is still a profile.
	SinkAddr string

	// Profile selects a stanza in the CLI's configuration file.
	Profile string

	// OperatorNamespace is where discovery reads a sink's credentials Secret and
	// the operator's own Deployment. Empty means "work it out": the configuration
	// file, then a labelled search, then DefaultOperatorNamespace.
	OperatorNamespace string

	// Verbosity is the klog verbosity level. It is applied to klog rather than
	// interpreted locally so that `-v 6` shows the API requests client-go is
	// making, which is the reason anyone reaches for the flag.
	Verbosity int

	// AssumeYes answers the confirmation a wide scan against an unindexed backend
	// would otherwise ask for.
	AssumeYes bool

	// MaxObjects aborts a scan that fetches more than this many objects. Zero
	// means no breaker, which is the default because the confirmation prompt is
	// already the protection a person gets; this is the one a script gets.
	MaxObjects int64
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
		fmt.Sprintf("Output format. One of: %s.", JoinValues(outputFormats)))
	flags.Var(&g.Color, FlagColor,
		fmt.Sprintf("When to colourise output. One of: %s. NO_COLOR is honoured under auto.",
			JoinValues(colorModes)))
	flags.StringVar(&g.Sink, FlagSink, g.Sink,
		"Read through a configured sink, as kind/name (for example ClickHouseSink/default).")
	flags.StringVar(&g.Source, FlagSource, g.Source,
		"Read directly from a location, bypassing sink discovery: a local directory or an s3:// URL.")
	flags.StringVar(&g.SinkAddr, FlagSinkAddr, g.SinkAddr,
		"Dial this host:port instead of the address the resolved ClickHouse backend recorded, "+
			"which is what a forwarded port needs. Everything else — database, user, credentials, "+
			"TLS, dial timeout — still comes from that backend, and the notice on stderr says so.")
	flags.StringVar(&g.Profile, FlagProfile, g.Profile,
		"Use this profile from the kuberecord configuration file.")
	flags.StringVar(&g.OperatorNamespace, FlagOperatorNamespace, g.OperatorNamespace,
		"The namespace the kuberecord operator runs in, where a sink's credentials Secret is read. "+
			"Defaults to the namespace of the operator's Deployment, or "+DefaultOperatorNamespace+".")
	flags.IntVarP(&g.Verbosity, FlagVerbosity, "v", g.Verbosity,
		"Number for the log level verbosity of diagnostics, which are written to stderr.")
	flags.BoolVar(&g.AssumeYes, FlagAssumeYes, g.AssumeYes,
		"Answer yes to the confirmation a wide scan of an unindexed backend asks for. "+
			"Assumed when the output is not a terminal, so a script never waits on a prompt.")
	flags.Int64Var(&g.MaxObjects, FlagMaxObjects, g.MaxObjects,
		"Abort a scan that fetches more than this many stored objects, naming this flag. "+
			"Zero means no limit. It bounds the work, which --limit cannot do without an index.")
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

// DefaultOperatorNamespace is where the operator is installed by both the chart
// and the kustomize overlay, and therefore where its credentials Secrets live
// unless somebody moved them.
//
// It sits here rather than with the discovery that uses it because
// --operator-namespace documents it in its own usage string, and the flag layer
// is below resolution in the order Task 11.8 fixed. The alternative was to spell
// the literal twice — once as the default and once in the help text — which is
// the shape a default and its documentation drift apart in.
const DefaultOperatorNamespace = "kuberecord-system"
