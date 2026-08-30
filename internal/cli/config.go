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
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"sigs.k8s.io/yaml"
)

// The configuration file's own identity and location.
const (
	// ConfigAPIVersion is the versioned contract this file is written against.
	//
	// It shares the group of the structured-output envelope (D19) because it is the
	// same promise made in the other direction: people commit this file to a dotfile
	// repository and share it across a team, so a field that changed meaning between
	// releases would be as expensive as one that changed in `-o json`. A file
	// carrying a version this build does not know is refused rather than
	// interpreted.
	ConfigAPIVersion = "cli.kuberecord.io/v1alpha1"

	// ConfigKind is the document kind, spelled the way every other Kubernetes
	// configuration file an engineer edits spells one.
	ConfigKind = "Config"

	// ConfigDirName and ConfigFileName are the path under the XDG config home.
	ConfigDirName  = "kuberecord"
	ConfigFileName = "config.yaml"

	// ConfigFileMode is the file's permissions, and ConfigDirMode its directory's.
	//
	// The file holds no secret — a password may only be *referenced* here, never
	// stored — so 0600 is not protecting a credential. It is protecting the pointer
	// to one: the name of the environment variable, the path of the file, the
	// address of the server and the user to authenticate as together describe how to
	// obtain the credential, and that is worth as little exposure as a config file
	// can be given for free.
	ConfigFileMode = 0o600
	ConfigDirMode  = 0o700
)

// BackendKind names which of the read plane's backends a profile describes.
type BackendKind string

// The backends a profile may name. They are the two query backends this release
// ships, with the archive one split by where the archive is — a bucket and a
// directory are the same format read through different sources, and asking a user
// to say which they have is cheaper than guessing from a string.
const (
	// BackendClickHouse reads the frozen v1 schema in a ClickHouse instance.
	BackendClickHouse BackendKind = "clickhouse"

	// BackendS3 reads a jsonl-v1 archive in an S3-compatible bucket.
	BackendS3 BackendKind = "s3"

	// BackendLocal reads a jsonl-v1 archive in a directory: an archive synced to a
	// laptop, or a mounted volume. It is the zero-infrastructure path (D18).
	BackendLocal BackendKind = "local"
)

// backendKinds is the accepted set, in the order it is shown to a user.
var backendKinds = []BackendKind{BackendClickHouse, BackendS3, BackendLocal}

// Config is the CLI's configuration file.
//
// It exists so that an engineer who cannot read Secrets in the operator's
// namespace — which is most engineers, and by design (D7) — can still use this
// tool without passing four flags every time. Everything in it is a *choice about
// where to read from*; nothing in it is a credential, and the validation below is
// what keeps that true.
type Config struct {
	// APIVersion and Kind identify the document. Both are stamped by every write
	// this package performs. An empty value in a hand-written file is accepted as
	// meaning the current version, because refusing to read a file for the sake of
	// a line the user was never shown is friction with no safety in it; a value
	// that is present and wrong is refused, because that one is a real
	// disagreement about what the fields mean.
	APIVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`

	// CurrentProfile is the profile used when --profile is not given. Empty means
	// no profile is active, which is an ordinary state: a user whose cluster has a
	// sink CR needs no profile at all.
	CurrentProfile string `json:"currentProfile,omitempty"`

	// OperatorNamespace is where discovery looks for the operator's Deployment and
	// for the Secrets a sink references. Empty means "work it out" — see
	// Resolver.operatorNamespace, which searches and then falls back to
	// DefaultOperatorNamespace.
	OperatorNamespace string `json:"operatorNamespace,omitempty"`

	// Profiles are the configured backends, by name.
	Profiles map[string]Profile `json:"profiles,omitempty"`

	// Contexts maps a kubeconfig context name to the kuberecord cluster identity
	// recorded from that cluster (D21: the two are different things, and this is
	// the file that relates them).
	//
	// It is the second step of the cluster-id chain and the one that makes a
	// long-lived setup zero-flag: an engineer who works across four clusters names
	// each once, and thereafter `--context prod-eu` carries the identity with it.
	Contexts map[string]string `json:"contexts,omitempty"`
}

// Profile is one configured place to read history from.
//
// The backend is named explicitly rather than inferred from which stanza is
// present, so that a profile with the wrong stanza filled in is a validation error
// naming both halves rather than a silent switch to whichever one was found.
type Profile struct {
	// Backend selects which stanza below describes this profile.
	Backend BackendKind `json:"backend"`

	// ClickHouse, S3 and Local are the per-backend settings. Exactly the one
	// matching Backend must be present.
	ClickHouse *ClickHouseProfile `json:"clickhouse,omitempty"`
	S3         *S3Profile         `json:"s3,omitempty"`
	Local      *LocalProfile      `json:"local,omitempty"`
}

// ClickHouseProfile is how to reach a ClickHouse instance holding the frozen v1
// schema.
//
// The recommended posture is a read-only user with SELECT on the two tables and
// nothing else; docs/CLI.md spells out the grants. This tool never writes, and a
// credential that cannot write is a credential whose leak costs an audit trail
// nothing.
type ClickHouseProfile struct {
	// Addr is the native-protocol endpoint as "host:port".
	Addr string `json:"addr"`

	// Database holds the frozen v1 tables. Empty leaves the server's default,
	// which is rarely right — the operator's own default is `kuberecord`.
	Database string `json:"database,omitempty"`

	// Username is the user to authenticate as.
	Username string `json:"username,omitempty"`

	// PasswordEnv names an environment variable holding the password.
	// PasswordFile names a file holding it, trailing newline trimmed. At most one
	// may be set; neither means no password, which is what a local evaluation
	// server usually wants.
	PasswordEnv  string `json:"passwordEnv,omitempty"`
	PasswordFile string `json:"passwordFile,omitempty"`

	// Password is a trap, and it is declared so that setting it produces an
	// explanation instead of an "unknown field" error.
	//
	// A password written here would sit in plain text in a file people commit to
	// dotfile repositories, sync between machines and paste into issues while
	// asking for help. Refusing it is not a policy this tool applies on a user's
	// behalf; it is the difference between a config file being shareable and being
	// a credential.
	Password string `json:"password,omitempty"`

	// TLS connects over TLS with the platform's trust store and a TLS 1.2 floor.
	// A private CA belongs in that store, where every other client on the machine
	// will also find it.
	TLS bool `json:"tls,omitempty"`
}

// S3Profile is an archive in an S3-compatible bucket.
//
// It carries no credentials, deliberately and permanently. The AWS SDK already
// resolves them — environment, shared config, SSO, instance role — and every
// engineer with a bucket has that configured for the other tools they use. A second
// credential chain here would be a worse copy of a solved problem, and the field a
// user would inevitably fill in with a literal secret key.
type S3Profile struct {
	// Bucket holds the archive.
	Bucket string `json:"bucket"`

	// Prefix is the archive's key prefix within the bucket — the sink's
	// spec.prefix — with no leading or trailing slash. Empty is ordinary: a bucket
	// dedicated to one archive.
	Prefix string `json:"prefix,omitempty"`

	// Region is the bucket's region. Empty selects DefaultS3Region, for the same
	// reason the CRD defaults it: the SDK requires one even against MinIO, which
	// ignores it, and a wrong region cannot resolve to somebody else's bucket
	// because S3 bucket names are global — it fails loudly instead.
	Region string `json:"region,omitempty"`

	// Endpoint overrides the S3 API endpoint, which is how MinIO and other
	// S3-compatible stores are addressed. The scheme is mandatory.
	Endpoint string `json:"endpoint,omitempty"`

	// ForcePathStyle addresses the bucket as <endpoint>/<bucket>/<key>, which is
	// what most in-cluster MinIO deployments need.
	ForcePathStyle bool `json:"forcePathStyle,omitempty"`

	// AccessKeyID, SecretAccessKey and SessionToken are traps, declared so that a
	// user who reaches for them is told where credentials come from instead of
	// being told the field does not exist.
	AccessKeyID     string `json:"accessKeyId,omitempty"`
	SecretAccessKey string `json:"secretAccessKey,omitempty"`
	SessionToken    string `json:"sessionToken,omitempty"`
}

// LocalProfile is an archive in a directory.
type LocalProfile struct {
	// Path is the directory holding the archive — the one containing
	// `format=jsonl-v1/`, or its parent if the archive was written under a prefix
	// (in which case set Prefix as well).
	Path string `json:"path"`

	// Prefix is the archive's key prefix within that directory, if it was written
	// with one.
	Prefix string `json:"prefix,omitempty"`
}

// DefaultS3Region is the region assumed when a profile or a source URL names
// none. It matches the CRD's own default.
const DefaultS3Region = "us-east-1"

// DefaultConfigPath returns where the configuration file lives.
//
// ${XDG_CONFIG_HOME:-~/.config}/kuberecord/config.yaml, which is the location an
// engineer's dotfile tooling already knows how to carry between machines. The home
// directory is resolved through os.UserHomeDir so that $HOME being unset is an
// error naming the problem rather than a path beginning with "/.config".
func DefaultConfigPath() (string, error) {
	if dir := os.Getenv("XDG_CONFIG_HOME"); dir != "" {
		return filepath.Join(dir, ConfigDirName, ConfigFileName), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locating the configuration file: %w "+
			"(set XDG_CONFIG_HOME to say where it should live)", err)
	}
	return filepath.Join(home, ".config", ConfigDirName, ConfigFileName), nil
}

// LoadConfig reads and validates the configuration file at path.
//
// A missing file is not an error. It is the ordinary state of a first invocation,
// and of every user whose cluster has a sink CR to discover — so it returns an
// empty configuration, and the caller carries on to the next step of the
// resolution chain rather than telling somebody to create a file they do not need.
//
// Everything else is an error, including a file that parses but says something
// impossible. A configuration silently ignored in part is how a user ends up
// reading the wrong cluster's history and believing the tool agreed with them.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &Config{APIVersion: ConfigAPIVersion, Kind: ConfigKind}, nil
		}
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}

	var cfg Config
	// Strict, so a misspelled key is a message rather than a setting that silently
	// did nothing. `passwordEnv` typed as `passwordENV` is exactly the mistake that
	// otherwise ends with an empty password and an authentication failure three
	// steps away from its cause.
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &cfg, nil
}

// Validate reports the first thing wrong with a configuration.
//
// First rather than all of them: these are edits a person makes one at a time, and
// a list of five complaints about a file with one mistake in it is harder to read
// than the one complaint that matters.
func (c *Config) Validate() error {
	if c.APIVersion != "" && c.APIVersion != ConfigAPIVersion {
		return fmt.Errorf("apiVersion %q is not %s; this build reads only that version",
			c.APIVersion, ConfigAPIVersion)
	}
	if c.Kind != "" && c.Kind != ConfigKind {
		return fmt.Errorf("kind %q is not %s", c.Kind, ConfigKind)
	}

	for _, name := range slices.Sorted(maps.Keys(c.Profiles)) {
		if err := c.Profiles[name].validate(); err != nil {
			return fmt.Errorf("profile %q: %w", name, err)
		}
	}

	if c.CurrentProfile != "" {
		if _, ok := c.Profiles[c.CurrentProfile]; !ok {
			return fmt.Errorf("currentProfile %q names no profile in this file (%s)",
				c.CurrentProfile, describeProfileNames(c.Profiles))
		}
	}

	for _, context := range slices.Sorted(maps.Keys(c.Contexts)) {
		if c.Contexts[context] == "" {
			return fmt.Errorf("contexts[%q] is empty: a context maps to the cluster identity recorded "+
				"from it, and an empty identity would match nothing", context)
		}
	}
	return nil
}

// validate reports the first thing wrong with one profile.
func (p Profile) validate() error {
	if !slices.Contains(backendKinds, p.Backend) {
		if p.Backend == "" {
			return fmt.Errorf("no backend given: one of %s", joinValues(backendKinds))
		}
		return fmt.Errorf("backend %q is not one of %s", p.Backend, joinValues(backendKinds))
	}

	// Which stanzas are present must match which backend was named. A profile that
	// says `clickhouse` and carries an `s3:` block is a half-finished edit, and
	// reading it as either one would be a guess about where somebody's audit trail
	// is.
	present := make([]string, 0, 3)
	if p.ClickHouse != nil {
		present = append(present, string(BackendClickHouse))
	}
	if p.S3 != nil {
		present = append(present, string(BackendS3))
	}
	if p.Local != nil {
		present = append(present, string(BackendLocal))
	}
	if !slices.Equal(present, []string{string(p.Backend)}) {
		return fmt.Errorf("backend is %q but the settings present are [%s]: a profile carries exactly "+
			"the one stanza its backend names", p.Backend, strings.Join(present, " "))
	}

	switch p.Backend {
	case BackendClickHouse:
		return p.ClickHouse.validate()
	case BackendS3:
		return p.S3.validate()
	case BackendLocal:
		return p.Local.validate()
	}
	return nil
}

func (p *ClickHouseProfile) validate() error {
	if p.Addr == "" {
		return errors.New("clickhouse.addr is required, as host:port")
	}
	if p.Password != "" {
		return errors.New("clickhouse.password holds a password inline, which this file will not " +
			"carry: it is committed to dotfile repositories, synced between machines and pasted into " +
			"issues. Use clickhouse.passwordEnv to name an environment variable, or " +
			"clickhouse.passwordFile to name a file")
	}
	if p.PasswordEnv != "" && p.PasswordFile != "" {
		return errors.New("clickhouse.passwordEnv and clickhouse.passwordFile are both set; " +
			"a password comes from one place, and reading either would be a coin toss")
	}
	return nil
}

func (p *S3Profile) validate() error {
	if p.Bucket == "" {
		return errors.New("s3.bucket is required")
	}
	if p.Prefix != strings.Trim(p.Prefix, "/") {
		return fmt.Errorf("s3.prefix %q has a leading or trailing slash: it is a path fragment, and a "+
			"slash here produces a doubled separator in every key it is joined into", p.Prefix)
	}
	if p.Endpoint != "" && !strings.HasPrefix(p.Endpoint, "http://") && !strings.HasPrefix(p.Endpoint, "https://") {
		return fmt.Errorf("s3.endpoint %q has no scheme: the SDK needs an absolute URL, so write it "+
			"as http://%s or https://%s", p.Endpoint, p.Endpoint, p.Endpoint)
	}
	if p.AccessKeyID != "" || p.SecretAccessKey != "" || p.SessionToken != "" {
		return errors.New("s3 credentials do not belong in this file: they are resolved from the " +
			"AWS credential chain (AWS_ACCESS_KEY_ID and friends, ~/.aws/config, SSO, an instance " +
			"role), which every tool on this machine already reads. Remove accessKeyId, " +
			"secretAccessKey and sessionToken")
	}
	return nil
}

func (p *LocalProfile) validate() error {
	if p.Path == "" {
		return errors.New("local.path is required: the directory holding format=jsonl-v1/")
	}
	if p.Prefix != strings.Trim(p.Prefix, "/") {
		return fmt.Errorf("local.prefix %q has a leading or trailing slash: it is a path fragment "+
			"within the directory, not a path", p.Prefix)
	}
	return nil
}

// ResolvePassword reads this profile's password from wherever it was referenced.
//
// Neither reference set means no password, which is what a local evaluation server
// usually wants and is returned without complaint.
//
// An environment variable that is *unset* is an error, and that distinction is the
// reason this indirection is worth having: a profile naming
// KUBERECORD_CLICKHOUSE_PASSWORD in a shell that never exported it must say so,
// rather than authenticate with an empty password and hand the user whatever the
// server says about that — which is an authentication failure three steps from its
// cause. A variable that is set and empty is honoured as an empty password,
// because that one was a decision somebody made.
//
// The value is returned and never logged. Nothing in this package renders it,
// puts it in an error, or writes it back to the configuration file.
func (p *ClickHouseProfile) ResolvePassword() (string, error) {
	switch {
	case p.PasswordEnv != "":
		password, ok := os.LookupEnv(p.PasswordEnv)
		if !ok {
			return "", fmt.Errorf("the environment variable %s is not set, and this profile names it "+
				"as where its password comes from", p.PasswordEnv)
		}
		return password, nil

	case p.PasswordFile != "":
		content, err := os.ReadFile(filepath.Clean(p.PasswordFile))
		if err != nil {
			return "", fmt.Errorf("reading the password file this profile names: %w", err)
		}
		// One trailing newline is trimmed, because `echo secret > file` is how such a
		// file is made and a password with a newline on the end authenticates as
		// nothing while looking correct in every editor.
		return strings.TrimRight(string(content), "\r\n"), nil
	}
	return "", nil
}

// SaveConfig writes the configuration to path, atomically and with the file mode
// this package promises.
//
// Atomically because the alternative — truncate, then write — leaves an empty or
// half-written configuration behind if anything interrupts it, and the thing it
// would have destroyed is a file the user hand-edited. The temporary file is
// created in the destination directory so that the rename is within one filesystem
// and therefore atomic; /tmp may well be a different one.
func SaveConfig(path string, cfg *Config) error {
	cfg.APIVersion = ConfigAPIVersion
	cfg.Kind = ConfigKind
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("refusing to write an invalid configuration to %s: %w", path, err)
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("encoding the configuration: %w", err)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, ConfigDirMode); err != nil {
		return fmt.Errorf("creating %s: %w", dir, err)
	}

	temp, err := os.CreateTemp(dir, ConfigFileName+".*")
	if err != nil {
		return fmt.Errorf("creating a temporary file beside %s: %w", path, err)
	}
	tempName := temp.Name()
	// Every path from here removes the temporary file unless the rename consumed
	// it, so an interrupted write leaves the old configuration and nothing else.
	//
	// This is the one discarded error in the package, and it is discarded on
	// purpose: on the success path the rename has already consumed the file and
	// this fails with ENOENT every time, while on a failure path the error being
	// returned is the one the user needs and a second complaint about the
	// temporary file would bury it.
	defer func() { _ = os.Remove(tempName) }()

	if err := writeAndClose(temp, data); err != nil {
		return fmt.Errorf("writing %s: %w", tempName, err)
	}
	// CreateTemp makes the file 0600 already; setting it explicitly is what keeps
	// that a promise of this package rather than of the standard library.
	if err := os.Chmod(tempName, ConfigFileMode); err != nil {
		return fmt.Errorf("setting the mode of %s: %w", tempName, err)
	}
	if err := os.Rename(tempName, path); err != nil {
		return fmt.Errorf("moving %s into place at %s: %w", tempName, path, err)
	}
	return nil
}

// writeAndClose writes the whole of data to file and closes it, reporting
// whichever step failed.
//
// The Close is checked rather than deferred and discarded, because Close is where
// a buffered write actually reaches the disk — a function that ignored it would
// report success for a configuration that never landed. On a failed write both
// errors are joined: the file still has to be closed, and neither failure should
// hide the other.
func writeAndClose(file *os.File, data []byte) error {
	if _, err := file.Write(data); err != nil {
		return errors.Join(err, file.Close())
	}
	return file.Close()
}

// describeProfileNames renders the profiles a file holds, for a message about one
// that is missing.
func describeProfileNames(profiles map[string]Profile) string {
	if len(profiles) == 0 {
		return "the file defines no profiles"
	}
	return "defined: " + strings.Join(slices.Sorted(maps.Keys(profiles)), ", ")
}
