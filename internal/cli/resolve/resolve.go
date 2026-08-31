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

// Package resolve turns what the user configured into an open backend and a
// cluster identity.
//
// It owns the whole chain a command does not want to know about: the
// --source/--sink flags, the profile stanza, discovery of a sink custom resource
// through the kubeconfig, reading the Secret that sink names, constructing the
// query engine behind it, and inferring which cluster's history is being asked
// about when nobody said.
//
// In the CLI's dependency order it sits above options and exit and below the
// command tree. It reads GlobalFlags and it names the program in its remediation
// messages, but it never constructs a command and never renders anything: a
// resolver that could reach the cobra tree would put the whole command surface
// on the path of every backend that is opened (Task 11.8).
//
// The one contract it is a client of on the far side is internal/query — the
// read plane — which is exactly the boundary deps_test.go in this directory
// asserts (D16, D20).
package resolve

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/kuberecord/kuberecord/api/v1alpha1"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/query"
	chquery "github.com/kuberecord/kuberecord/internal/query/clickhouse"
	"github.com/kuberecord/kuberecord/internal/query/objectsource"
	"github.com/kuberecord/kuberecord/internal/query/objectsource/awssource"
)

// Where a command's data comes from is resolved here, once, by a chain whose steps
// are ordered by how explicit the user was: --source, then --sink, then the active
// profile, then whatever the cluster's own sink CRs say, and then an error.
//
// Two properties make the chain worth being a chain rather than a flag.
//
// The first is that the ordinary case needs nothing. A cluster with an operator
// streaming to one ClickHouse has already written down where that is, in an object
// with cluster-read access; asking the user to repeat it would be asking them to
// maintain a second copy of a fact the cluster holds.
//
// The second is that every step announces itself on stderr. A tool that silently
// chose between four sources would eventually read the wrong one and be believed —
// and for an audit trail, being believed while wrong is the worst failure
// available. The notice costs one line and makes the choice checkable.

// Origin says which step of the resolution chain produced a backend.
type Origin string

// The four ways a backend can be chosen, in the order they are tried.
const (
	// OriginSourceFlag is --source: a directory or a bucket URL, read directly.
	// It is the zero-infrastructure path (D18) and it bypasses the cluster
	// entirely — no kubeconfig, no CRs, no operator.
	OriginSourceFlag Origin = "source-flag"

	// OriginSinkFlag is --sink kind/name: a sink CR named explicitly, which is
	// what a cluster with several of them needs.
	OriginSinkFlag Origin = "sink-flag"

	// OriginProfile is a stanza in the configuration file, which is the answer
	// for the many engineers who cannot read Secrets in the operator's namespace.
	OriginProfile Origin = "profile"

	// OriginDiscovered is the cluster's own sink CR, found because there was
	// exactly one. This is the step that makes the common case zero-config.
	OriginDiscovered Origin = "discovered"
)

// phrase renders an origin for the notice line, which is the sentence a user reads
// to check that the tool chose what they meant.
func (o Origin) phrase() string {
	switch o {
	case OriginSourceFlag:
		return "using --" + options.FlagSource
	case OriginSinkFlag:
		return "using --" + options.FlagSink
	case OriginProfile:
		return "using profile"
	case OriginDiscovered:
		return "discovered"
	}
	return "using"
}

// Backend is an opened read plane together with the provenance of the choice.
//
// Description and Origin travel with the engine because a command that renders a
// result has to be able to say where it came from — in the notice already printed,
// in a failure message, and in metadata.backend once structured output lands
// (Task 11.5). An engine alone cannot answer "which of my three clusters is this?"
type Backend struct {
	// Engine answers the read-plane contract.
	Engine query.QueryEngine

	// ClusterID is the resolved kuberecord cluster identity (D21).
	ClusterID string

	// Origin is the step of the chain that chose this backend.
	Origin Origin

	// Description names it the way the notice on stderr does — "ClickHouseSink/default
	// (host:port/database)" — with no credential in it, at any verbosity.
	Description string

	// ClusterIDSource says how the identity above was arrived at, in the words the
	// notice used.
	ClusterIDSource string

	// closers release what opening this backend created, in reverse order of
	// creation. The engine never closes a source it was lent, so this is where the
	// source is closed too.
	closers []func() error
}

// Close releases everything opening this backend created.
//
// Every closer runs even if an earlier one failed: they are independent resources,
// and stopping at the first failure would leak the rest to save reporting one
// error. The failures are joined so none is hidden by another.
func (b *Backend) Close() error {
	var errs []error
	for i := len(b.closers) - 1; i >= 0; i-- {
		if err := b.closers[i](); err != nil {
			errs = append(errs, err)
		}
	}
	b.closers = nil
	return errors.Join(errs...)
}

// BackendResolver turns one invocation's flags and configuration into an opened backend.
//
// It is a struct rather than a function because the steps share state that is
// expensive or awkward to recompute: the Kubernetes clients, the operator's
// Deployment, and the record of which notices have already been printed.
type BackendResolver struct {
	// Flags is the parsed global flag surface.
	Flags *options.GlobalFlags

	// Streams is where notices go — stderr, always, so that structured output on
	// stdout stays pipeable (Invariant 4 is about not hiding things, not about
	// putting them in the pipe).
	Streams genericiooptions.IOStreams

	// InvokedAs is how this process was invoked, so that remediation in a message
	// names a command the reader can actually type.
	InvokedAs string

	// Config is the loaded configuration file, never nil after NewResolver.
	Config *Config

	// ConfigPath is where that file lives, named in messages about it.
	ConfigPath string

	// Clients is the Kubernetes access, built from the kubeconfig on first use.
	// A test supplies its own; nothing else should.
	Clients *Clients

	// clientErr remembers a failure to build them, so a resolution that consults
	// the cluster three times reports one failure rather than three.
	clientErr    error
	clientsBuilt bool

	// The memoized operator lookup. See findOperatorDeployment.
	operator         *operatorInfo
	operatorErr      error
	operatorSearched bool
	operatorNS       string

	// noticesFailed records that stderr has gone, so the writer stands down.
	noticesFailed bool
}

// NewBackendResolver builds a resolver for one invocation, loading the configuration file.
//
// A missing configuration file is not an error (see LoadConfig); a malformed one
// is, and it is reported here rather than at the step that would have used it,
// because a file with a typo in it is a mistake the user wants to hear about
// whether or not this particular command needed a profile.
func NewBackendResolver(flags *options.GlobalFlags, streams genericiooptions.IOStreams, invokedAs string) (*BackendResolver, error) {
	path, err := DefaultConfigPath()
	if err != nil {
		return nil, exit.RuntimeErrorf("%w", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, exit.RuntimeErrorf("%w", err)
	}
	return &BackendResolver{
		Flags:      flags,
		Streams:    streams,
		InvokedAs:  invokedAs,
		Config:     cfg,
		ConfigPath: path,
	}, nil
}

// commandName is what a remediation message tells the reader to type.
func (r *BackendResolver) commandName() string {
	if r.InvokedAs == "" {
		return options.StandaloneName
	}
	return r.InvokedAs
}

// notef writes one resolution notice to stderr.
//
// The write is checked, because every fallible call in this package is, and then
// deliberately not propagated: a failed write to stderr means stderr itself has
// gone — a closed pipe, a full disk — and there is nowhere left to report that.
// The resolution it describes is unaffected and the answer still belongs on
// stdout, so failing the command would trade a working answer for an unreportable
// diagnostic. Every later notice would fail identically, so the writer stands down
// instead of retrying once a line for the rest of the invocation.
func (r *BackendResolver) notef(format string, args ...any) {
	if r.noticesFailed || r.Streams.ErrOut == nil {
		return
	}
	if _, err := fmt.Fprintf(r.Streams.ErrOut, "→ "+format+"\n", args...); err != nil {
		r.noticesFailed = true
	}
}

// clients builds the Kubernetes access from the kubeconfig, once.
//
// The failure is memoized as well as the success, because a resolution consults the
// cluster from three places — the sink listing, the Secret, the operator's
// Deployment — and a machine with no kubeconfig should say so once rather than
// three times.
func (r *BackendResolver) clients() (*Clients, error) {
	if r.Clients != nil {
		return r.Clients, nil
	}
	if r.clientsBuilt {
		if r.clientErr == nil {
			// Unreachable: every path below sets one or the other. Stated anyway,
			// because the alternative to a stated error here is a nil dereference
			// at a call site that had no way to know.
			return nil, exit.RuntimeErrorf("no Kubernetes client, and no reason recorded for it")
		}
		return nil, r.clientErr
	}
	r.clientsBuilt = true

	restConfig, err := r.Flags.ConfigFlags.ToRESTConfig()
	if err != nil {
		r.clientErr = exit.RuntimeErrorf("no Kubernetes cluster to ask: %w", err)
		return nil, r.clientErr
	}
	dyn, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		r.clientErr = exit.RuntimeErrorf("building a Kubernetes client: %w", err)
		return nil, r.clientErr
	}
	typed, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		r.clientErr = exit.RuntimeErrorf("building a Kubernetes client: %w", err)
		return nil, r.clientErr
	}
	r.Clients = &Clients{Dynamic: dyn, Typed: typed}
	return r.Clients, nil
}

// declaredOperatorNamespace is the operator namespace the user stated, or the
// empty string meaning "search for it".
func (r *BackendResolver) declaredOperatorNamespace() string {
	if r.Flags != nil && r.Flags.OperatorNamespace != "" {
		return r.Flags.OperatorNamespace
	}
	if r.Config != nil {
		return r.Config.OperatorNamespace
	}
	return ""
}

// operatorNamespace decides where a sink's credentials Secret is read from.
//
// The default is a security boundary rather than a convenience, and it belongs to
// the CRD: a SecretReference with no namespace means the operator's own namespace,
// because that is the only namespace the operator's aggregated ClusterRole grants
// Secret reads in (D7). This reader has to arrive at the same namespace the writer
// used, or it will look for the credential in the wrong place and report a Secret
// that does not exist.
//
// Stated first, searched second, defaulted last. The search is what makes an
// ordinary install need no configuration; the default is what makes a locked-down
// cluster — where the search is forbidden — still work when the install is the
// ordinary one.
func (r *BackendResolver) operatorNamespace(ctx context.Context) (string, error) {
	if r.operatorNS != "" {
		return r.operatorNS, nil
	}
	if declared := r.declaredOperatorNamespace(); declared != "" {
		r.operatorNS = declared
		return r.operatorNS, nil
	}

	info, err := r.findOperatorDeployment(ctx)
	if err != nil {
		return "", err
	}
	if info != nil {
		r.operatorNS = info.namespace
		return r.operatorNS, nil
	}

	r.operatorNS = options.DefaultOperatorNamespace
	return r.operatorNS, nil
}

// Resolve runs the chain and opens what it chose.
//
// The order of the two halves matters: the backend is chosen and announced before
// the cluster identity is worked out, because the identity's own last resort is to
// ask the backend, and a user watching a slow question should already know which
// sink is being asked.
func (r *BackendResolver) Resolve(ctx context.Context) (*Backend, error) {
	chosen, origin, err := r.resolveTarget(ctx)
	if err != nil {
		return nil, err
	}

	engine, closers, err := chosen.open(ctx)
	if err != nil {
		return nil, err
	}
	backend := &Backend{
		Engine:      engine,
		Origin:      origin,
		Description: chosen.description,
		closers:     closers,
	}
	r.notef("%s %s", origin.phrase(), chosen.description)

	clusterID, source, err := r.resolveClusterID(ctx, engine)
	if err != nil {
		// The engine was opened by this call and nothing else has a reference to
		// it, so a failure here must not leak it. The close error is joined into
		// the failure rather than replacing it: the reason the command is ending is
		// the resolution failure, not the tidying up.
		return nil, errors.Join(err, backend.Close())
	}
	backend.ClusterID = clusterID
	backend.ClusterIDSource = source
	r.notef("cluster-id %s (%s)", clusterID, source)

	return backend, nil
}

// resolveTarget walks the chain and returns the first step that answered.
func (r *BackendResolver) resolveTarget(ctx context.Context) (target, Origin, error) {
	if source := r.Flags.Source; source != "" {
		chosen, err := targetFromSource(source)
		return chosen, OriginSourceFlag, err
	}

	if named := r.Flags.Sink; named != "" {
		ref, err := ParseSinkRef(named)
		if err != nil {
			return target{}, OriginSinkFlag, err
		}
		chosen, err := r.targetFromSinkRef(ctx, ref)
		return chosen, OriginSinkFlag, err
	}

	name, profile, err := r.activeProfile()
	if err != nil {
		return target{}, OriginProfile, err
	}
	if profile != nil {
		chosen, profileErr := targetFromProfile(name, *profile)
		return chosen, OriginProfile, profileErr
	}

	chosen, err := r.targetFromDiscovery(ctx)
	return chosen, OriginDiscovered, err
}

// activeProfile returns the profile this invocation should use, or nil for none.
//
// A --profile naming something the file does not define is a usage error and never
// a fall-through to discovery. The user named a profile; reading a different
// backend because that name was misspelled would answer a question they did not
// ask, from a source they did not choose.
func (r *BackendResolver) activeProfile() (string, *Profile, error) {
	name := r.Flags.Profile
	explicit := name != ""
	if !explicit {
		name = r.Config.CurrentProfile
	}
	if name == "" {
		return "", nil, nil
	}

	profile, ok := r.Config.Profiles[name]
	if !ok {
		if explicit {
			return "", nil, exit.UsageErrorf("--%s %q names no profile in %s (%s)",
				options.FlagProfile, name, r.ConfigPath, DescribeProfileNames(r.Config.Profiles))
		}
		// Unreachable through LoadConfig, which validates the reference. Reported
		// rather than ignored so that a configuration assembled in memory by a
		// future caller cannot silently fall through to discovery.
		return "", nil, exit.RuntimeErrorf("%s names the current profile %q, which it does not define",
			r.ConfigPath, name)
	}
	return name, &profile, nil
}

// targetFromDiscovery reads the cluster's own sink CRs.
//
// Exactly one is the case this step exists for. None and several are both errors,
// and both messages name every route still open, because a user who reaches this
// point has told the tool nothing and needs to know what they *can* say — not that
// something was missing.
func (r *BackendResolver) targetFromDiscovery(ctx context.Context) (target, error) {
	candidates, err := r.listSinks(ctx)
	if err != nil {
		return target{}, exit.RuntimeErrorf("%w. %s", err, r.otherRoutes())
	}

	switch len(candidates) {
	case 0:
		return target{}, exit.RuntimeErrorf("%w. %s", errNoSinksFound, r.otherRoutes())
	case 1:
		return r.targetFromSink(ctx, candidates[0])
	}
	return target{}, exit.RuntimeErrorf("this cluster has %d sinks (%s); name one with --%s",
		len(candidates), describeSinks(candidates), options.FlagSink)
}

// otherRoutes is the sentence appended to every discovery failure.
//
// It is one sentence and it names all three alternatives, because the reason
// discovery failed decides which of them the reader can use and this function does
// not know that reason: a missing CRD, a forbidden list and an empty cluster are
// three different situations with the same three ways out.
func (r *BackendResolver) otherRoutes() string {
	return fmt.Sprintf("Read an archive directly with --%s <dir|s3://bucket/prefix>, "+
		"name a sink with --%s <kind>/<name>, or configure a profile with `%s config set-profile`",
		options.FlagSource, options.FlagSink, r.commandName())
}

// targetFromSinkRef fetches one named sink and resolves it.
func (r *BackendResolver) targetFromSinkRef(ctx context.Context, ref SinkRef) (target, error) {
	object, err := r.getSink(ctx, ref)
	if err != nil {
		return target{}, err
	}
	return r.targetFromSink(ctx, sinkCandidate{ref: ref, object: object})
}

// targetFromSink turns a discovered custom resource into something openable.
func (r *BackendResolver) targetFromSink(ctx context.Context, candidate sinkCandidate) (target, error) {
	switch candidate.ref.Kind {
	case KindClickHouseSink:
		sink, err := decodeClickHouseSink(candidate.object)
		if err != nil {
			return target{}, err
		}
		return r.clickHouseTarget(ctx, candidate.ref, sink)
	case KindS3Sink:
		sink, err := decodeS3Sink(candidate.object)
		if err != nil {
			return target{}, err
		}
		return r.s3Target(ctx, candidate.ref, sink)
	}
	return target{}, exit.RuntimeErrorf("no sink kind named %q", candidate.ref.Kind)
}

// The CRD's own defaults, repeated for the case where a resource reaches this code
// without having been through the API server's defaulting — a fixture, or a
// manifest read from disk by something later. They are copies of the
// +kubebuilder:default markers in api/v1alpha1/clickhousesink_types.go.
const (
	// DefaultClickHouseDatabase is where the operator writes unless told otherwise.
	DefaultClickHouseDatabase = "kuberecord"

	// DefaultClickHouseUsername is the user it authenticates as unless told
	// otherwise.
	DefaultClickHouseUsername = "default"
)

// clickHouseTarget resolves a ClickHouseSink into a dialable configuration.
//
// The sink's dial timeout is carried over and its read timeout is not, and that is
// a decision rather than an omission: a bound on reaching a host is the same
// question whoever is asking, while spec.connection.readTimeout is sized for the
// operator's inserts — a bound that must not let a write wedge the hot path — and a
// human's cold query over a large table legitimately takes longer than any insert
// should. See chquery.DefaultReadTimeout.
func (r *BackendResolver) clickHouseTarget(
	ctx context.Context, ref SinkRef, sink *v1alpha1.ClickHouseSink,
) (target, error) {
	connection := sink.Spec.Connection

	namespace, err := r.secretNamespace(ctx, connection.CredentialsSecretRef)
	if err != nil {
		return target{}, err
	}
	data, err := r.secretData(ctx, connection.CredentialsSecretRef, ref)
	if err != nil {
		return target{}, err
	}
	password, err := requireSecretKey(data, secretKeyPassword, ref, namespace,
		connection.CredentialsSecretRef.Name)
	if err != nil {
		return target{}, err
	}

	dial := chquery.DialConfig{
		Addr:     connection.Addr,
		Database: valueOr(connection.Database, DefaultClickHouseDatabase),
		Username: valueOr(connection.Username, DefaultClickHouseUsername),
		Password: password,
	}
	if connection.DialTimeout != nil {
		dial.DialTimeout = connection.DialTimeout.Duration
	}

	return target{
		backend:    BackendClickHouse,
		clickhouse: dial,
		// The AC's shape exactly: the sink, then host:port/database. The username
		// is deliberately absent and so, of course, is the password — a notice is
		// read over shoulders and pasted into issues.
		description: fmt.Sprintf("%s (%s/%s)", ref, dial.Addr, dial.Database),
	}, nil
}

// s3Target resolves an S3Sink into an openable archive.
//
// A sink with no credentials block is not a broken sink: authenticating from the
// ambient chain is supported and, on a cloud provider with an instance role, is the
// preferred state. So the Secret is read only when the sink names one, which also
// means the common cloud install needs no Secret permission from the person running
// this command.
func (r *BackendResolver) s3Target(ctx context.Context, ref SinkRef, sink *v1alpha1.S3Sink) (target, error) {
	spec := sink.Spec

	credentials := awssource.Credentials{}
	if spec.Credentials != nil && spec.Credentials.SecretRef != nil {
		secretRef := *spec.Credentials.SecretRef
		namespace, err := r.secretNamespace(ctx, secretRef)
		if err != nil {
			return target{}, err
		}
		data, err := r.secretData(ctx, secretRef, ref)
		if err != nil {
			return target{}, err
		}
		accessKeyID, err := requireSecretKey(data, secretKeyAccessKeyID, ref, namespace, secretRef.Name)
		if err != nil {
			return target{}, err
		}
		secretAccessKey, err := requireSecretKey(data, secretKeySecretAccessKey, ref, namespace, secretRef.Name)
		if err != nil {
			return target{}, err
		}
		credentials = awssource.Credentials{
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
			// Optional: only temporary credentials carry one, and a Secret without
			// it is the ordinary static-key case rather than an incomplete one.
			SessionToken: string(data[secretKeySessionToken]),
		}
	}

	chosen := target{
		backend: BackendS3,
		s3: awssource.Config{
			Bucket:         spec.Bucket,
			Region:         valueOr(spec.Region, DefaultS3Region),
			Endpoint:       spec.Endpoint,
			ForcePathStyle: spec.ForcePathStyle,
			Credentials:    credentials,
		},
		archivePrefix: spec.Prefix,
		description: fmt.Sprintf("%s (s3://%s, region %s)", ref,
			joinBucketPrefix(spec.Bucket, spec.Prefix), valueOr(spec.Region, DefaultS3Region)),
	}
	if spec.Rotation.MaxObjectAge != nil {
		// The sink's own rotation age is how far past its partition an object may
		// carry records, and passing the real value is the difference between
		// listing one extra partition and listing an unnecessary one. A reader
		// that guessed would be widening every query by a default it had no
		// evidence for.
		chosen.objectSpan = spec.Rotation.MaxObjectAge.Duration
	}
	return chosen, nil
}

// secretNamespace reports which namespace a SecretReference resolves to, for the
// messages that name it.
func (r *BackendResolver) secretNamespace(ctx context.Context, ref v1alpha1.SecretReference) (string, error) {
	if ref.Namespace != "" {
		return ref.Namespace, nil
	}
	return r.operatorNamespace(ctx)
}

// targetFromProfile turns a configured profile into something openable.
func targetFromProfile(name string, profile Profile) (target, error) {
	switch profile.Backend {
	case BackendClickHouse:
		password, err := profile.ClickHouse.ResolvePassword()
		if err != nil {
			return target{}, exit.RuntimeErrorf("profile %q: %w", name, err)
		}
		return target{
			backend: BackendClickHouse,
			clickhouse: chquery.DialConfig{
				Addr:     profile.ClickHouse.Addr,
				Database: valueOr(profile.ClickHouse.Database, DefaultClickHouseDatabase),
				Username: profile.ClickHouse.Username,
				Password: password,
				TLS:      profile.ClickHouse.TLS,
			},
			description: fmt.Sprintf("%s (ClickHouse at %s/%s)", name,
				profile.ClickHouse.Addr, valueOr(profile.ClickHouse.Database, DefaultClickHouseDatabase)),
		}, nil

	case BackendS3:
		return target{
			backend: BackendS3,
			s3: awssource.Config{
				Bucket:         profile.S3.Bucket,
				Region:         valueOr(profile.S3.Region, DefaultS3Region),
				Endpoint:       profile.S3.Endpoint,
				ForcePathStyle: profile.S3.ForcePathStyle,
			},
			archivePrefix: profile.S3.Prefix,
			description: fmt.Sprintf("%s (s3://%s, region %s)", name,
				joinBucketPrefix(profile.S3.Bucket, profile.S3.Prefix),
				valueOr(profile.S3.Region, DefaultS3Region)),
		}, nil

	case BackendLocal:
		return target{
			backend:       BackendLocal,
			localPath:     profile.Local.Path,
			archivePrefix: profile.Local.Prefix,
			description:   fmt.Sprintf("%s (local archive at %s)", name, profile.Local.Path),
		}, nil
	}
	return target{}, exit.RuntimeErrorf("profile %q names the backend %q, which is not one of %s",
		name, profile.Backend, options.JoinValues(BackendKinds))
}

// The environment this package reads when --source names a bucket.
//
// It reads the SDK's own variables rather than inventing kuberecord ones, because
// an engineer pointing this at a bucket has already configured them for every other
// tool on the machine. AWS_ENDPOINT_URL and the credential chain are resolved by
// the SDK itself and are deliberately absent here; these two are the settings the
// SDK's config does not carry to the place this code needs them.
const (
	envRegion         = "AWS_REGION"
	envDefaultRegion  = "AWS_DEFAULT_REGION"
	envForcePathStyle = "AWS_S3_FORCE_PATH_STYLE"
)

// targetFromSource reads --source: a bucket URL, a file URL, or a plain path.
//
// It performs no I/O and consults no cluster. That is the point of the flag: an
// evaluator with an archive on a laptop, or an auditor with a bucket and a
// read-only key, gets an answer with no operator, no CRs and no kubeconfig
// anywhere in the path (D18).
func targetFromSource(source string) (target, error) {
	switch {
	case strings.HasPrefix(source, "s3://"):
		parsed, err := url.Parse(source)
		if err != nil {
			return target{}, exit.UsageErrorf("--%s %q is not a URL: %v", options.FlagSource, source, err)
		}
		bucket := parsed.Host
		if bucket == "" {
			return target{}, exit.UsageErrorf("--%s %q names no bucket: expected s3://bucket[/prefix]",
				options.FlagSource, source)
		}
		prefix := strings.Trim(parsed.Path, "/")
		region := valueOr(os.Getenv(envRegion), os.Getenv(envDefaultRegion))

		return target{
			backend: BackendS3,
			s3: awssource.Config{
				Bucket: bucket,
				// The SDK needs a region even against MinIO, which ignores it. A
				// wrong one cannot resolve to somebody else's bucket, since S3
				// bucket names are global — it fails loudly, which is why
				// defaulting here is safe and refusing would be friction.
				Region:         valueOr(region, DefaultS3Region),
				ForcePathStyle: os.Getenv(envForcePathStyle) == "true",
			},
			archivePrefix: prefix,
			description: fmt.Sprintf("s3://%s (region %s)",
				joinBucketPrefix(bucket, prefix), valueOr(region, DefaultS3Region)),
		}, nil

	case strings.HasPrefix(source, "file://"):
		parsed, err := url.Parse(source)
		if err != nil {
			return target{}, exit.UsageErrorf("--%s %q is not a URL: %v", options.FlagSource, source, err)
		}
		if parsed.Host != "" && parsed.Host != "localhost" {
			return target{}, exit.UsageErrorf("--%s %q names the host %q; a file URL this tool can read "+
				"is file:///an/absolute/path", options.FlagSource, source, parsed.Host)
		}
		return localTarget(parsed.Path), nil

	case strings.Contains(source, "://"):
		scheme, _, _ := strings.Cut(source, "://")
		return target{}, exit.UsageErrorf("--%s names the scheme %q, which this build cannot read; "+
			"use s3:// for a bucket, or a path for a directory", options.FlagSource, scheme)
	}
	return localTarget(source), nil
}

// localTarget is an archive in a directory.
func localTarget(path string) target {
	return target{
		backend:     BackendLocal,
		localPath:   path,
		description: fmt.Sprintf("%s (local archive)", path),
	}
}

// target is a resolved place to read from, before anything has been opened.
//
// Resolution and opening are separate for the same reason parsing an object
// address and resolving it are (see ResourceArg): only opening needs the network,
// so keeping the decision pure is what lets the whole chain be tested without one.
type target struct {
	backend BackendKind

	// clickhouse is the dial configuration when backend is BackendClickHouse. It
	// holds the resolved password, in memory, and nothing renders it.
	clickhouse chquery.DialConfig

	// s3 is the bucket configuration when backend is BackendS3.
	s3 awssource.Config

	// localPath is the directory when backend is BackendLocal.
	localPath string

	// archivePrefix is the archive's key prefix within the bucket or directory,
	// for either archive backend.
	archivePrefix string

	// objectSpan is the sink's rotation age, when it is known. Zero leaves the
	// engine's own default, which is correct for any legally configured archive.
	objectSpan time.Duration

	// description is how the notice names this choice. No credential ever appears
	// in it.
	description string
}

// open builds the engine this target describes.
//
// The closers come back separately, and in creation order, because the engine
// never closes a source it was lent — that is the read-plane contract's rule, and
// it means the caller owns both. Returning them rather than hiding them in the
// engine is what keeps that ownership visible at the one place it matters.
func (t target) open(ctx context.Context) (query.QueryEngine, []func() error, error) {
	switch t.backend {
	case BackendClickHouse:
		engine, err := chquery.Dial(t.clickhouse)
		if err != nil {
			return nil, nil, exit.RuntimeErrorf("%w", err)
		}
		return engine, []func() error{engine.Close}, nil

	case BackendS3:
		source, err := awssource.New(ctx, t.s3)
		if err != nil {
			return nil, nil, exit.RuntimeErrorf("%w", err)
		}
		engine, err := t.openArchive(source)
		if err != nil {
			return nil, nil, errors.Join(err, source.Close())
		}
		return engine, []func() error{source.Close, engine.Close}, nil

	case BackendLocal:
		source, err := objectsource.NewLocal(t.localPath)
		if err != nil {
			return nil, nil, exit.RuntimeErrorf("%w", err)
		}
		engine, err := t.openArchive(source)
		if err != nil {
			return nil, nil, errors.Join(err, source.Close())
		}
		return engine, []func() error{source.Close, engine.Close}, nil
	}
	return nil, nil, exit.RuntimeErrorf("no backend named %q", t.backend)
}

// openArchive builds the archive engine over a source, with the options this
// target resolved.
func (t target) openArchive(source objectsource.ObjectSource) (query.QueryEngine, error) {
	engine, err := objectsource.NewEngine(source, objectsource.Options{
		Prefix:     t.archivePrefix,
		ObjectSpan: t.objectSpan,
	})
	if err != nil {
		return nil, exit.RuntimeErrorf("%w", err)
	}
	return engine, nil
}

// joinBucketPrefix renders a bucket and prefix as the URL a user would type.
func joinBucketPrefix(bucket, prefix string) string {
	if prefix == "" {
		return bucket
	}
	return bucket + "/" + prefix
}

// valueOr returns value, or fallback when value is empty.
func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
