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
// password comes from. These four subcommands are the smallest surface that makes
// writing such a profile a command rather than a documentation exercise.
//
// What they will not do is store a credential. See ClickHouseProfile.Password.

// newConfigCommand builds the `config` subtree.
func newConfigCommand(flags *GlobalFlags, streams genericiooptions.IOStreams, invokedAs string) *cobra.Command {
	config := &cobra.Command{
		Use:   "config",
		Short: "Read and write the kuberecord configuration file",
		Long: fmt.Sprintf(`Read and write the kuberecord configuration file.

The file lives at ${XDG_CONFIG_HOME:-~/.config}/%s/%s and is written 0600.

It holds profiles — where to read history from — and a mapping from kubeconfig
context to kuberecord cluster identity. It never holds a password: a profile
names an environment variable or a file to read one from, and a password written
inline is refused with an explanation.`, ConfigDirName, ConfigFileName),
		Args: rejectUnknownSubcommand,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}

	config.AddCommand(
		newConfigViewCommand(flags, streams),
		newConfigSetProfileCommand(streams),
		newConfigUseProfileCommand(streams),
		newConfigSetContextClusterIDCommand(flags, streams, invokedAs),
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
func newConfigViewCommand(flags *GlobalFlags, streams genericiooptions.IOStreams) *cobra.Command {
	return &cobra.Command{
		Use:   "view",
		Short: "Print the configuration file",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				return UsageErrorf("config view takes no arguments, and was given %q", args[0])
			}

			path, err := DefaultConfigPath()
			if err != nil {
				return RuntimeErrorf("%w", err)
			}
			cfg, err := LoadConfig(path)
			if err != nil {
				return RuntimeErrorf("%w", err)
			}

			// The path goes to stderr so that `config view -o json | jq` receives
			// the document alone, and so that a reader who has two machines'
			// dotfiles in play can see which file they are looking at.
			if err := writeLine(streams.ErrOut, "# "+path); err != nil {
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
func writeConfig(out io.Writer, cfg *Config, format OutputFormat) error {
	// Stamped on the way out even for a file that predates them, so that what is
	// printed is what a write of the same document would produce.
	cfg.APIVersion = ConfigAPIVersion
	cfg.Kind = ConfigKind

	switch format {
	case OutputYAML, OutputTable, OutputWide:
		// A configuration file is YAML, and `table` is the global default rather
		// than a choice this command was given; rendering the document is the only
		// sensible reading of it.
		encoded, err := yaml.Marshal(cfg)
		if err != nil {
			return RuntimeErrorf("encoding the configuration: %w", err)
		}
		return writeAll(out, string(encoded))

	case OutputJSON:
		encoded, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			return RuntimeErrorf("encoding the configuration: %w", err)
		}
		return writeAll(out, string(encoded)+"\n")
	}
	return UsageErrorf("config view renders %s or %s, not %s", OutputYAML, OutputJSON, format)
}

// newConfigSetProfileCommand writes one profile.
func newConfigSetProfileCommand(streams genericiooptions.IOStreams) *cobra.Command {
	var (
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
from the AWS credential chain, which every tool on the machine already reads.`,
		Example: `  # A read-only ClickHouse user, with the password in the environment.
  kuberecord config set-profile prod --backend clickhouse \
      --addr clickhouse.example:9000 --database kuberecord \
      --username kuberecord_ro --password-env KUBERECORD_CLICKHOUSE_PASSWORD

  # An archive in MinIO.
  kuberecord config set-profile archive --backend s3 --bucket acme-audit \
      --prefix kuberecord --endpoint https://minio.internal:9000 --force-path-style

  # An archive synced to a laptop.
  kuberecord config set-profile laptop --backend local --path ~/archives/kuberecord`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return UsageErrorf("config set-profile takes one argument, the profile name")
			}
			name := args[0]
			if name == "" {
				return UsageErrorf("the profile name is empty")
			}

			profile := Profile{Backend: BackendKind(backend)}
			switch profile.Backend {
			case BackendClickHouse:
				profile.ClickHouse = &ClickHouseProfile{
					Addr: addr, Database: database, Username: username,
					PasswordEnv: passwordEnv, PasswordFile: passwordFile, TLS: useTLS,
				}
			case BackendS3:
				profile.S3 = &S3Profile{
					Bucket: bucket, Prefix: prefix, Region: region,
					Endpoint: endpoint, ForcePathStyle: forcePathStyle,
				}
			case BackendLocal:
				profile.Local = &LocalProfile{Path: path, Prefix: prefix}
			default:
				return UsageErrorf("--backend %q is not one of %s", backend, joinValues(backendKinds))
			}

			// Validated before anything is read from disk, so a mistyped command
			// cannot rewrite a file only to be rejected on the way back in.
			if err := profile.validate(); err != nil {
				return UsageErrorf("profile %q: %w", name, err)
			}

			path, err := DefaultConfigPath()
			if err != nil {
				return RuntimeErrorf("%w", err)
			}
			cfg, err := LoadConfig(path)
			if err != nil {
				return RuntimeErrorf("%w", err)
			}
			if cfg.Profiles == nil {
				cfg.Profiles = map[string]Profile{}
			}
			_, replaced := cfg.Profiles[name]
			cfg.Profiles[name] = profile

			// The first profile in an empty file becomes the active one. Requiring a
			// second command to make the only profile usable is ceremony with no
			// decision in it — and it is announced, so nothing about which profile is
			// active is decided silently.
			activated := false
			if cfg.CurrentProfile == "" {
				cfg.CurrentProfile = name
				activated = true
			}

			if err := SaveConfig(path, cfg); err != nil {
				return RuntimeErrorf("%w", err)
			}

			verb := "wrote"
			if replaced {
				verb = "replaced"
			}
			if err := writeLine(streams.ErrOut, fmt.Sprintf("→ %s profile %q in %s", verb, name, path)); err != nil {
				return err
			}
			if activated {
				return writeLine(streams.ErrOut, fmt.Sprintf("→ %q is now the active profile", name))
			}
			return nil
		},
	}

	command.Flags().StringVar(&backend, "backend", "",
		fmt.Sprintf("Which backend this profile reads. One of: %s.", joinValues(backendKinds)))
	command.Flags().StringVar(&addr, "addr", "",
		"ClickHouse native-protocol endpoint, as host:port.")
	command.Flags().StringVar(&database, "database", "",
		"ClickHouse database holding the frozen v1 tables.")
	command.Flags().StringVar(&username, "username", "",
		"ClickHouse user. A read-only user is the recommended posture; see docs/CLI.md.")
	command.Flags().StringVar(&passwordEnv, "password-env", "",
		"Name of an environment variable holding the ClickHouse password.")
	command.Flags().StringVar(&passwordFile, "password-file", "",
		"Path to a file holding the ClickHouse password.")
	command.Flags().BoolVar(&useTLS, "tls", false,
		"Connect to ClickHouse over TLS, using the platform's trust store.")
	command.Flags().StringVar(&bucket, "bucket", "", "S3 bucket holding the archive.")
	command.Flags().StringVar(&region, "region", "",
		fmt.Sprintf("Bucket region. Defaults to %s, which MinIO ignores.", DefaultS3Region))
	command.Flags().StringVar(&endpoint, "endpoint", "",
		"S3 API endpoint, with scheme, for MinIO and other S3-compatible stores.")
	command.Flags().BoolVar(&forcePathStyle, "force-path-style", false,
		"Address the bucket as <endpoint>/<bucket>/<key>, which most MinIO deployments need.")
	command.Flags().StringVar(&prefix, "prefix", "",
		"The archive's key prefix within the bucket or directory, with no leading or trailing slash.")
	command.Flags().StringVar(&path, "path", "",
		"Directory holding a local archive — the one containing format=jsonl-v1/.")

	return command
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
				return UsageErrorf("config use-profile takes one argument, the profile name")
			}
			name := args[0]

			path, err := DefaultConfigPath()
			if err != nil {
				return RuntimeErrorf("%w", err)
			}
			cfg, err := LoadConfig(path)
			if err != nil {
				return RuntimeErrorf("%w", err)
			}
			if _, ok := cfg.Profiles[name]; !ok {
				return UsageErrorf("no profile named %q in %s (%s)",
					name, path, describeProfileNames(cfg.Profiles))
			}

			cfg.CurrentProfile = name
			if err := SaveConfig(path, cfg); err != nil {
				return RuntimeErrorf("%w", err)
			}
			return writeLine(streams.ErrOut, fmt.Sprintf("→ %q is now the active profile", name))
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
	flags *GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
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
				resolver := &BackendResolver{Flags: flags, Streams: streams, InvokedAs: invokedAs}
				contextName, clusterID = resolver.kubeContext(), args[0]
				if contextName == "" {
					return UsageErrorf("no kubeconfig context is current, so there is nothing to map "+
						"%q to; name one: `%s config set-context-cluster-id <context> %s`",
						clusterID, commandNameOr(invokedAs), clusterID)
				}
			case 2:
				contextName, clusterID = args[0], args[1]
			default:
				return UsageErrorf("config set-context-cluster-id takes [CONTEXT] CLUSTER_ID")
			}
			if clusterID == "" {
				return UsageErrorf("the cluster identity is empty")
			}

			path, err := DefaultConfigPath()
			if err != nil {
				return RuntimeErrorf("%w", err)
			}
			cfg, err := LoadConfig(path)
			if err != nil {
				return RuntimeErrorf("%w", err)
			}
			if cfg.Contexts == nil {
				cfg.Contexts = map[string]string{}
			}
			cfg.Contexts[contextName] = clusterID

			if err := SaveConfig(path, cfg); err != nil {
				return RuntimeErrorf("%w", err)
			}
			return writeLine(streams.ErrOut,
				fmt.Sprintf("→ context %q reads cluster %q", contextName, clusterID))
		},
	}
}

// commandNameOr is the invocation name for a message, defaulting to the
// standalone spelling.
func commandNameOr(invokedAs string) string {
	if invokedAs == "" {
		return StandaloneName
	}
	return invokedAs
}

// writeLine writes one line, reporting a failure rather than discarding it.
//
// Unlike a resolution notice — which is diagnostic, and whose loss costs nothing
// the exit code does not already carry — these lines are the whole of what a
// `config` command produces. A `set-profile` that wrote the file and then could not
// say so has done something the user cannot see, and reporting the write failure is
// the only way they find out that their terminal, not their configuration, is what
// went wrong.
func writeLine(out io.Writer, line string) error {
	if out == nil {
		return nil
	}
	if _, err := io.WriteString(out, line+"\n"); err != nil {
		return RuntimeErrorf("writing output: %w", err)
	}
	return nil
}

// writeAll writes a rendered document, reporting a short write as the failure it
// is.
func writeAll(out io.Writer, content string) error {
	if _, err := io.WriteString(out, content); err != nil {
		return RuntimeErrorf("writing output: %w", err)
	}
	return nil
}
