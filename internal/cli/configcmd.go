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
	"encoding/json"
	"fmt"
	"io"
	"slices"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"sigs.k8s.io/yaml"
)

// `config` exists because discovery cannot serve everyone.
//
// A sink's password lives in a Secret in the operator's namespace, and the
// operator's own RBAC is the only grant that reaches it (D7). Most engineers
// cannot read it, and the right answer to that is not to widen anybody's
// permissions — it is a read-only ClickHouse user and a profile naming where the
// password comes from. Four of these subcommands are the smallest surface that
// makes writing such a profile a command rather than a documentation exercise.
//
// What they will not do is store a credential. See resolve.ClickHouseProfile.Password.
//
// The fifth, `resolve`, writes nothing at all. It belongs here because a profile
// is one step of the chain that decides where an answer comes from, and the
// question it answers — "which step won, and why not the others" — is the one a
// reader of this file has when the file turns out not to be the step that won.
// See resolvecmd.go.

// newConfigCommand builds the `config` subtree.
func newConfigCommand(flags *options.GlobalFlags, streams genericiooptions.IOStreams, invokedAs string) *cobra.Command {
	config := &cobra.Command{
		Use:   "config",
		Short: "Read and write the kuberecord configuration file",
		Long: fmt.Sprintf(`Read and write the kuberecord configuration file.

The file lives at ${XDG_CONFIG_HOME:-~/.config}/%s/%s and is written 0600.

It holds profiles — where to read history from — and a mapping from kubeconfig
context to kuberecord cluster identity. It never holds a password: a profile
names an environment variable or a file to read one from, and a password written
inline is refused with an explanation.

`+"`config resolve`"+` writes nothing: it reports which step of the resolution
chains this invocation would use, and why the earlier ones had nothing to say.`,
			resolve.ConfigDirName, resolve.ConfigFileName),
		Args: rejectUnknownSubcommand,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}

	config.AddCommand(
		newConfigViewCommand(flags, streams),
		newConfigSetProfileCommand(flags, streams, invokedAs),
		newConfigUseProfileCommand(streams),
		newConfigSetContextClusterIDCommand(flags, streams, invokedAs),
		newConfigResolveCommand(flags, streams, invokedAs),
	)
	return config
}

// newConfigViewCommand prints the configuration.
//
// It prints the file's *contents* rather than an effective configuration merged
// with the flags, because the question it answers is "what did I write down", and
// a view that folded in this invocation's flags would show a file that does not
// exist. Nothing is redacted, which is safe by construction: the file cannot hold
// a credential.
func newConfigViewCommand(flags *options.GlobalFlags, streams genericiooptions.IOStreams) *cobra.Command {
	return &cobra.Command{
		Use:   "view",
		Short: "Print the configuration file",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				return exit.UsageErrorf("config view takes no arguments, and was given %q", args[0])
			}

			path, err := resolve.DefaultConfigPath()
			if err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			cfg, err := resolve.LoadConfig(path)
			if err != nil {
				return exit.RuntimeErrorf("%w", err)
			}

			// The path goes to stderr so that `config view -o json | jq` receives
			// the document alone, and so that a reader who has two machines'
			// dotfiles in play can see which file they are looking at.
			if err := options.WriteLine(streams.ErrOut, "# "+path); err != nil {
				return err
			}
			return writeConfig(streams.Out, cfg, flags.Output)
		},
	}
}

// writeConfig renders a configuration in the requested format.
//
// Only the two document formats are accepted. A configuration file is not a result
// set: there is no useful table of it, and `jsonl` is a streaming format for a
// result larger than memory. Refusing the other four by name is better than
// rendering YAML regardless and leaving a user to wonder why `-o table` did
// nothing.
func writeConfig(out io.Writer, cfg *resolve.Config, format options.OutputFormat) error {
	// Stamped on the way out even for a file that predates them, so that what is
	// printed is what a write of the same document would produce.
	cfg.APIVersion = resolve.ConfigAPIVersion
	cfg.Kind = resolve.ConfigKind

	switch format {
	case options.OutputYAML, options.OutputTable, options.OutputWide:
		// A configuration file is YAML, and `table` is the global default rather
		// than a choice this command was given; rendering the document is the only
		// sensible reading of it.
		encoded, err := yaml.Marshal(cfg)
		if err != nil {
			return exit.RuntimeErrorf("encoding the configuration: %w", err)
		}
		return options.WriteAll(out, string(encoded))

	case options.OutputJSON:
		encoded, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			return exit.RuntimeErrorf("encoding the configuration: %w", err)
		}
		return options.WriteAll(out, string(encoded)+"\n")
	}
	return exit.UsageErrorf("config view renders %s or %s, not %s", options.OutputYAML, options.OutputJSON, format)
}

// newConfigSetProfileCommand writes one profile.
func newConfigSetProfileCommand(
	flags *options.GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
) *cobra.Command {
	var (
		fromSink       string
		backend        string
		addr           string
		database       string
		username       string
		passwordEnv    string
		passwordFile   string
		useTLS         bool
		bucket         string
		region         string
		endpoint       string
		forcePathStyle bool
		prefix         string
		path           string
	)

	command := &cobra.Command{
		Use:   "set-profile NAME",
		Short: "Create or replace a profile",
		Long: `Create or replace a profile in the kuberecord configuration file.

A profile says where to read recorded history from. It never holds a password:
for ClickHouse, name an environment variable with --password-env or a file with
--password-file. For S3 and MinIO there is nothing to name — credentials come
from the AWS credential chain, which every tool on the machine already reads.

--from-sink <kind>/<name> fills in the whole stanza from a sink custom resource
the cluster already holds, which is every field except the ones a reader outside
the cluster has to decide: the endpoint, the user and where the password comes
from.`,
		Example: `  # A read-only ClickHouse user, with the password in the environment.
  kuberecord config set-profile prod --backend clickhouse \
      --addr clickhouse.example:9000 --database kuberecord \
      --username kuberecord_ro --password-env KUBERECORD_CLICKHOUSE_PASSWORD

  # The same thing, read from the sink the operator is already streaming to.
  kuberecord config set-profile local --from-sink ClickHouseSink/default

  # An archive in MinIO.
  kuberecord config set-profile archive --backend s3 --bucket acme-audit \
      --prefix kuberecord --endpoint https://minio.internal:9000 --force-path-style

  # An archive synced to a laptop.
  kuberecord config set-profile laptop --backend local --path ~/archives/kuberecord`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return exit.UsageErrorf("config set-profile takes one argument, the profile name")
			}
			name := args[0]
			if name == "" {
				return exit.UsageErrorf("the profile name is empty")
			}

			if fromSink != "" {
				derived, err := deriveProfile(cmd, flags, streams, invokedAs, fromSink,
					resolve.ProfileOverrides{
						Addr: addr, Username: username, PasswordEnv: passwordEnv,
						PasswordFile: passwordFile, TLS: useTLS,
					})
				if err != nil {
					return err
				}
				return writeProfile(profileWrite{
					name:    name,
					profile: derived.Profile,
					// Colour is decided here rather than in resolve for the reason
					// the unreachable-backend block's is: --color, NO_COLOR and
					// whether stderr is a terminal are facts about this invocation,
					// and a renderer that consulted them itself would have golden
					// files that changed with the shell they were generated in.
					explanation: derived.Explain(options.ShouldColorize(flags.Color, streams.ErrOut)),
					nextStep:    true,
					invokedAs:   invokedAs,
				}, streams)
			}

			profile := resolve.Profile{Backend: resolve.BackendKind(backend)}
			switch profile.Backend {
			case resolve.BackendClickHouse:
				profile.ClickHouse = &resolve.ClickHouseProfile{
					Addr: addr, Database: database, Username: username,
					PasswordEnv: passwordEnv, PasswordFile: passwordFile, TLS: useTLS,
				}
			case resolve.BackendS3:
				profile.S3 = &resolve.S3Profile{
					Bucket: bucket, Prefix: prefix, Region: region,
					Endpoint: endpoint, ForcePathStyle: forcePathStyle,
				}
			case resolve.BackendLocal:
				profile.Local = &resolve.LocalProfile{Path: path, Prefix: prefix}
			default:
				// Named here because this is where a user who does not know
				// --from-sink exists actually lands: the flag that reads all of
				// these out of the cluster is worth one clause at the moment
				// somebody is typing them by hand.
				return exit.UsageErrorf("--%s %q is not one of %s; or give --%s <kind>/<name> to read "+
					"the whole stanza from a sink custom resource",
					options.FlagBackend, backend, options.JoinValues(resolve.BackendKinds),
					options.FlagFromSink)
			}

			return writeProfile(profileWrite{name: name, profile: profile}, streams)
		},
	}

	command.Flags().StringVar(&fromSink, options.FlagFromSink, "",
		"Fill the profile in from a sink custom resource, as kind/name (for example "+
			"ClickHouseSink/default). A cluster-internal address is rewritten to a forwarded "+
			"loopback port, and the notice on stderr says so.")
	command.Flags().StringVar(&backend, options.FlagBackend, "",
		fmt.Sprintf("Which backend this profile reads. One of: %s.", options.JoinValues(resolve.BackendKinds)))
	command.Flags().StringVar(&addr, options.FlagAddr, "",
		"ClickHouse native-protocol endpoint, as host:port.")
	command.Flags().StringVar(&database, options.FlagDatabase, "",
		"ClickHouse database holding the frozen v1 tables.")
	command.Flags().StringVar(&username, options.FlagUsername, "",
		"ClickHouse user. A read-only user is the recommended posture; see docs/CLI.md.")
	command.Flags().StringVar(&passwordEnv, options.FlagPasswordEnv, "",
		"Name of an environment variable holding the ClickHouse password.")
	command.Flags().StringVar(&passwordFile, options.FlagPasswordFile, "",
		"Path to a file holding the ClickHouse password.")
	command.Flags().BoolVar(&useTLS, options.FlagTLS, false,
		"Connect to ClickHouse over TLS, using the platform's trust store.")
	command.Flags().StringVar(&bucket, options.FlagBucket, "", "S3 bucket holding the archive.")
	command.Flags().StringVar(&region, options.FlagRegion, "",
		fmt.Sprintf("Bucket region. Defaults to %s, which MinIO ignores.", resolve.DefaultS3Region))
	command.Flags().StringVar(&endpoint, options.FlagEndpoint, "",
		"S3 API endpoint, with scheme, for MinIO and other S3-compatible stores.")
	command.Flags().BoolVar(&forcePathStyle, options.FlagForcePathStyle, false,
		"Address the bucket as <endpoint>/<bucket>/<key>, which most MinIO deployments need.")
	command.Flags().StringVar(&prefix, options.FlagPrefix, "",
		"The archive's key prefix within the bucket or directory, with no leading or trailing slash.")
	command.Flags().StringVar(&path, options.FlagPath, "",
		"Directory holding a local archive — the one containing format=jsonl-v1/.")

	return command
}

// profileWrite is one profile on its way into the configuration file.
//
// It is a struct because the two routes that reach the write — a stanza typed
// flag by flag, and one derived from a sink custom resource — differ only in what
// they have to say about it afterwards, and a second copy of the load/merge/save
// sequence would be a second place for the activation rule to be decided.
type profileWrite struct {
	// name is the key in the file's profiles map.
	name string

	// profile is the stanza to write.
	profile resolve.Profile

	// explanation is what --from-sink derived and why, already rendered. Empty for
	// a profile typed out by hand, which needs no explaining to the person who
	// just typed it.
	explanation string

	// nextStep asks for the `config use-profile` line, which is printed only when
	// the write did not itself make this profile the active one.
	nextStep bool

	// invokedAs is how this process was invoked, so that line names a command the
	// reader can type.
	invokedAs string
}

// writeProfile validates a profile, writes it, and says what it did.
func writeProfile(w profileWrite, streams genericiooptions.IOStreams) error {
	// Validated before anything is read from disk, so a mistyped command cannot
	// rewrite a file only to be rejected on the way back in.
	if err := w.profile.Validate(); err != nil {
		return exit.UsageErrorf("profile %q: %w", w.name, err)
	}

	path, err := resolve.DefaultConfigPath()
	if err != nil {
		return exit.RuntimeErrorf("%w", err)
	}
	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		return exit.RuntimeErrorf("%w", err)
	}
	if cfg.Profiles == nil {
		cfg.Profiles = map[string]resolve.Profile{}
	}
	_, replaced := cfg.Profiles[w.name]
	cfg.Profiles[w.name] = w.profile

	// The first profile in an empty file becomes the active one. Requiring a
	// second command to make the only profile usable is ceremony with no
	// decision in it — and it is announced, so nothing about which profile is
	// active is decided silently. An existing choice is never overridden: that
	// one is a decision, and `use-profile` is where it is made.
	activated := false
	if cfg.CurrentProfile == "" {
		cfg.CurrentProfile = w.name
		activated = true
	}

	if err := resolve.SaveConfig(path, cfg); err != nil {
		return exit.RuntimeErrorf("%w", err)
	}

	verb := "wrote"
	if replaced {
		verb = "replaced"
	}
	if err := options.WriteLine(streams.ErrOut, fmt.Sprintf("→ %s profile %q in %s", verb, w.name, path)); err != nil {
		return err
	}
	if activated {
		if err := options.WriteLine(streams.ErrOut,
			fmt.Sprintf("→ %q is now the active profile", w.name)); err != nil {
			return err
		}
	}
	if w.explanation != "" {
		if err := options.WriteAll(streams.ErrOut, "\n"+w.explanation); err != nil {
			return err
		}
	}
	if w.nextStep && !activated {
		return options.WriteLine(streams.ErrOut,
			fmt.Sprintf("\n→ to make it the active profile: `%s config use-profile %s`",
				commandNameOr(w.invokedAs), w.name))
	}
	return nil
}

// deriveProfile reads a sink custom resource and turns it into a profile stanza.
//
// It is the whole of --from-sink at this layer: the flag conflicts, which are a
// question about flags and therefore belong to the command, and then a call into
// the same discovery path a query resolves through. Nothing about how a sink is
// read changes according to why it was read.
func deriveProfile(
	cmd *cobra.Command, flags *options.GlobalFlags, streams genericiooptions.IOStreams,
	invokedAs, value string, over resolve.ProfileOverrides,
) (*resolve.SinkProfile, error) {
	ref, err := resolve.ParseSinkRef(options.FlagFromSink, value)
	if err != nil {
		return nil, err
	}
	backend, err := backendForSinkKind(ref.Kind)
	if err != nil {
		return nil, err
	}
	// Refused before the cluster is contacted, for the same reason a profile is
	// validated before the file is opened: learning that two flags disagree should
	// not cost an API round trip and a Secret read.
	if err := refuseFromSinkConflicts(cmd, ref, backend); err != nil {
		return nil, err
	}

	resolver, err := resolve.NewBackendResolver(flags, streams, invokedAs)
	if err != nil {
		return nil, err
	}
	return resolver.ProfileFromSink(cmd.Context(), ref, over)
}

// backendForSinkKind says which profile backend a sink kind writes.
func backendForSinkKind(kind string) (resolve.BackendKind, error) {
	switch kind {
	case resolve.KindClickHouseSink:
		return resolve.BackendClickHouse, nil
	case resolve.KindS3Sink:
		return resolve.BackendS3, nil
	}
	return "", exit.UsageErrorf("--%s names the kind %q, which writes no profile this build knows",
		options.FlagFromSink, kind)
}

// profileFieldFlags is every per-field flag this command carries, paired with the
// backend it configures.
//
// It exists so that --from-sink can refuse the ones it has no use for by name and
// with a reason, rather than accepting them and quietly writing something else —
// a flag that parsed and did nothing is a silent error whose symptom is a profile
// reading somewhere its author did not choose (Invariant 4).
var profileFieldFlags = []struct {
	name string
	// backend is the one this flag belongs to. --backend itself belongs to none,
	// since it is the flag that selects one.
	backend resolve.BackendKind
}{
	{options.FlagBackend, ""},
	{options.FlagAddr, resolve.BackendClickHouse},
	{options.FlagDatabase, resolve.BackendClickHouse},
	{options.FlagUsername, resolve.BackendClickHouse},
	{options.FlagPasswordEnv, resolve.BackendClickHouse},
	{options.FlagPasswordFile, resolve.BackendClickHouse},
	{options.FlagTLS, resolve.BackendClickHouse},
	{options.FlagBucket, resolve.BackendS3},
	{options.FlagRegion, resolve.BackendS3},
	{options.FlagEndpoint, resolve.BackendS3},
	{options.FlagForcePathStyle, resolve.BackendS3},
	{options.FlagPrefix, resolve.BackendS3},
	{options.FlagPath, resolve.BackendLocal},
}

// fromSinkOverrides are the flags --from-sink accepts beside itself, by backend.
//
// Exactly the settings a sink custom resource cannot state, or must not state for
// a reader: the endpoint, which is the field a forwarded port changes; the TLS
// setting, which spec.connection does not carry at all; and the user and
// credential, which should be a read-only ClickHouse user's rather than the
// operator's write credential. An S3Sink has none of these — its bucket, prefix,
// region and endpoint are all facts about where the archive is, and its
// credentials are not in this file at all.
var fromSinkOverrides = map[resolve.BackendKind][]string{
	resolve.BackendClickHouse: {
		options.FlagAddr, options.FlagUsername,
		options.FlagPasswordEnv, options.FlagPasswordFile, options.FlagTLS,
	},
}

// refuseFromSinkConflicts rejects a flag the named sink already answers.
func refuseFromSinkConflicts(cmd *cobra.Command, ref resolve.SinkRef, backend resolve.BackendKind) error {
	// --sink-addr is a per-invocation override of a *resolved* backend (D25), and
	// this command resolves nothing and dials nothing. Accepting it here would
	// parse a value and write a different one to disk.
	if cmd.Flags().Changed(options.FlagSinkAddr) {
		return exit.UsageErrorf("--%s overrides the endpoint of one invocation's backend, and this "+
			"command writes a file rather than reading one: give --%s to set the address the "+
			"profile records", options.FlagSinkAddr, options.FlagAddr)
	}

	allowed := fromSinkOverrides[backend]
	for _, field := range profileFieldFlags {
		if !cmd.Flags().Changed(field.name) || slices.Contains(allowed, field.name) {
			continue
		}
		return exit.UsageErrorf("--%s %s and --%s cannot be given together: %s",
			options.FlagFromSink, ref, field.name, fromSinkConflict(ref, backend, field.name, field.backend))
	}
	return nil
}

// fromSinkConflict is the "because" half of a refusal, in the reader's terms.
//
// Three reasons, and they send a person to three different places: the kind
// already decided the backend, the flag belongs to a backend this sink is not, or
// the custom resource states the field and a profile disagreeing with it would
// read somewhere other than where the sink writes.
func fromSinkConflict(ref resolve.SinkRef, backend resolve.BackendKind, flag string, owner resolve.BackendKind) string {
	switch {
	case flag == options.FlagBackend:
		return fmt.Sprintf("the kind in --%s decides the backend, and %s writes a %s profile",
			options.FlagFromSink, ref, backend)
	case owner != backend:
		return fmt.Sprintf("--%s configures the %s backend, and %s writes a %s profile",
			flag, owner, ref, backend)
	}
	return fmt.Sprintf("%s states it, and a profile that disagreed with it would read somewhere "+
		"other than where that sink writes", ref)
}

// newConfigUseProfileCommand selects the active profile.
func newConfigUseProfileCommand(streams genericiooptions.IOStreams) *cobra.Command {
	return &cobra.Command{
		Use:   "use-profile NAME",
		Short: "Make a profile the active one",
		Long: `Make a profile the active one.

The active profile is used when neither --source nor --sink is given, and it
takes precedence over discovering a sink from the cluster. Pass --profile to
override it for a single command.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return exit.UsageErrorf("config use-profile takes one argument, the profile name")
			}
			name := args[0]

			path, err := resolve.DefaultConfigPath()
			if err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			cfg, err := resolve.LoadConfig(path)
			if err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			if _, ok := cfg.Profiles[name]; !ok {
				return exit.UsageErrorf("no profile named %q in %s (%s)",
					name, path, resolve.DescribeProfileNames(cfg.Profiles))
			}

			cfg.CurrentProfile = name
			if err := resolve.SaveConfig(path, cfg); err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			return options.WriteLine(streams.ErrOut, fmt.Sprintf("→ %q is now the active profile", name))
		},
	}
}

// newConfigSetContextClusterIDCommand records which kuberecord cluster a
// kubeconfig context reads.
//
// It is the second step of the cluster-id chain, and the one that pays off for an
// engineer working across several clusters: said once per context, and thereafter
// `--context prod-eu` carries the identity with it. D21 is the reason it has to be
// said at all — a kubeconfig context names an API server, and a kuberecord cluster
// identity is a string somebody chose when installing the operator.
func newConfigSetContextClusterIDCommand(
	flags *options.GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
) *cobra.Command {
	return &cobra.Command{
		Use:   "set-context-cluster-id [CONTEXT] CLUSTER_ID",
		Short: "Record the kuberecord cluster identity a kubeconfig context reads",
		Long: `Record the kuberecord cluster identity a kubeconfig context reads.

With one argument the current kubeconfig context is used, or the one named by
--context. With two, the context is named explicitly, which is what an engineer
writing several mappings in a row wants.`,
		Example: `  kuberecord config set-context-cluster-id prod-eu-1
  kuberecord config set-context-cluster-id prod-eu prod-eu-1
  kuberecord --context prod-eu config set-context-cluster-id prod-eu-1`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var contextName, clusterID string
			switch len(args) {
			case 1:
				resolver := &resolve.BackendResolver{Flags: flags, Streams: streams, InvokedAs: invokedAs}
				contextName, clusterID = resolver.KubeContext(), args[0]
				if contextName == "" {
					return exit.UsageErrorf("no kubeconfig context is current, so there is nothing to map "+
						"%q to; name one: `%s config set-context-cluster-id <context> %s`",
						clusterID, commandNameOr(invokedAs), clusterID)
				}
			case 2:
				contextName, clusterID = args[0], args[1]
			default:
				return exit.UsageErrorf("config set-context-cluster-id takes [CONTEXT] CLUSTER_ID")
			}
			if clusterID == "" {
				return exit.UsageErrorf("the cluster identity is empty")
			}

			path, err := resolve.DefaultConfigPath()
			if err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			cfg, err := resolve.LoadConfig(path)
			if err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			if cfg.Contexts == nil {
				cfg.Contexts = map[string]string{}
			}
			cfg.Contexts[contextName] = clusterID

			if err := resolve.SaveConfig(path, cfg); err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			return options.WriteLine(streams.ErrOut,
				fmt.Sprintf("→ context %q reads cluster %q", contextName, clusterID))
		},
	}
}

// commandNameOr is the invocation name for a message, defaulting to the
// standalone spelling.
func commandNameOr(invokedAs string) string {
	if invokedAs == "" {
		return options.StandaloneName
	}
	return invokedAs
}
