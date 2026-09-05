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
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kuberecord/kuberecord/api/v1alpha1"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
)

// Turning a discovered sink into a written profile: the permanent half of what
// Task 13.1's message tells a user to do.
//
// `config set-profile` takes one flag per field, and for somebody who has just
// watched the CLI *discover* all of those from a custom resource, retyping them is
// a transcription exercise with a typo in it. --from-sink reads the same custom
// resource the resolution chain reads, through the same discovery path, and writes
// the stanza its kind calls for.
//
// # The address is the interesting field
//
// It is precisely the one that must differ from the custom resource — that is why
// the user is writing a profile at all. A Service DNS name copied into a profile
// unchanged produces a profile that fails exactly as discovery did, which is worse
// than not writing one: the failure now looks like a configuration the user chose.
// So the rule is stated in three cases rather than defaulted, and the case that
// rewrites announces itself.
//
// The classifier is ClusterInternalAddr, which diagnose.go exports for this. One
// classifier, so the command that rewrites an address and the message that
// explains why it needed rewriting can never disagree.
//
// # The password is not copied, and is never held
//
// This is the path most likely to tempt an inline write, because it is the one
// standing next to a Secret it can read. Nothing here extracts the value: the
// Secret is fetched to confirm that the key the sink names is actually there, the
// key's *presence* is the whole of what is read, and the profile carries a
// reference to an environment variable exactly as a hand-written one does.
//
// # Why a Secret it cannot read is not a failure
//
// The operator's aggregated ClusterRole grants Secret reads in its own namespace
// and most engineers have less than that (D7) — and those engineers are the
// audience for profiles in the first place. The verification is therefore
// best-effort: what it cannot check, it says it could not check. Nothing about the
// written profile depends on the answer, because the profile's password comes from
// the reader's own environment and not from the operator's Secret.

// ProfileOverrides are the fields the user stated on the command line, which win
// over what the custom resource says.
//
// It carries exactly the settings a ClickHouseSink cannot state, or must not state
// for a *reader*: the endpoint (the field a forwarded port changes), the TLS
// setting (spec.connection carries none at all), and the user and credential —
// which should be a read-only ClickHouse user's rather than the operator's write
// credential, and are therefore never derivable from the sink. Everything else the
// custom resource says is a fact about where the sink writes, and a profile that
// disagreed with it would read somewhere else; those flags are refused by the
// command rather than layered here.
type ProfileOverrides struct {
	// Addr replaces the recorded endpoint, as host:port.
	Addr string

	// Username replaces the user the sink authenticates as.
	Username string

	// PasswordEnv and PasswordFile name where the reader's password comes from.
	// At most one, as in any profile.
	PasswordEnv  string
	PasswordFile string

	// TLS connects over TLS, which spec.connection cannot express.
	TLS bool
}

// SinkCredential names where a sink's own credential lives, without reading it.
//
// It exists so that a message can tell an engineer minting a read-only user where
// the operator's credential is, and so that a Secret missing the key it promises
// is reported while somebody is looking. The value never travels in it.
type SinkCredential struct {
	// Namespace and Name locate the Secret. The namespace is the one the operator
	// itself would have resolved: the reference's own, or the operator's.
	Namespace string
	Name      string

	// Key is the entry within it the sink expects.
	Key string
}

// String renders the Secret the way kubectl addresses one.
func (c SinkCredential) String() string { return c.Namespace + "/" + c.Name }

// SinkProfile is a profile derived from a sink custom resource, together with what
// had to change on the way.
//
// The facts and the prose are separated for the reason UnreachableSinkError
// separates Error from Render: the caller writes a file with the profile and
// prints the explanation, and only the caller knows whether stderr is a terminal.
type SinkProfile struct {
	// Ref is the sink this came from.
	Ref SinkRef

	// Profile is the stanza to write. It holds no credential — see
	// ClickHouseProfile.Password for why that is a property rather than a habit.
	Profile Profile

	// RecordedAddr is the endpoint the custom resource states, for a ClickHouse
	// profile. It is kept even when it was used unchanged, because the message
	// names it either way.
	RecordedAddr string

	// AddrRewritten reports that RecordedAddr resolves only inside the cluster and
	// the profile therefore names a forwarded loopback port instead.
	AddrRewritten bool

	// AddrOverridden reports that the user named the endpoint themselves.
	AddrOverridden bool

	// UsernameOverridden reports the same about the ClickHouse user. It is
	// separate from the address because the message says which fields came from
	// the custom resource, and a profile that claimed a read-only user was the
	// sink's own would be describing the operator's writer.
	UsernameOverridden bool

	// PortForward is the command that makes a rewritten address work, pre-filled
	// from the Service the recorded address names. Empty unless AddrRewritten.
	PortForward string

	// Credential names the Secret the sink itself authenticates with, or nil for a
	// sink that names none — which is the ordinary state of an S3Sink using the
	// ambient credential chain.
	Credential *SinkCredential

	// CredentialUnverified says why that Secret could not be checked, and is empty
	// when it was. It is a notice rather than a failure: nothing in the written
	// profile depends on it.
	CredentialUnverified string

	// EndpointInternal reports that an S3 profile's endpoint resolves only inside
	// the cluster. It is written unchanged regardless — see Explain.
	EndpointInternal bool
}

// ProfileFromSink reads one sink custom resource and derives the profile it
// describes.
//
// It goes through getSink, so a forbidden read, a missing CRD and an absent
// resource are classified exactly as they are for a query: the failure modes of
// reading a sink do not change according to why it was being read.
//
// The overrides must already be ones the sink's kind has somewhere to put. The
// command refuses each inapplicable flag by name, before the cluster is contacted
// at all; the guard below is what keeps that true for any other caller.
func (r *BackendResolver) ProfileFromSink(
	ctx context.Context, ref SinkRef, over ProfileOverrides,
) (*SinkProfile, error) {
	object, err := r.getSink(ctx, ref)
	if err != nil {
		return nil, err
	}

	switch ref.Kind {
	case KindClickHouseSink:
		sink, decodeErr := decodeClickHouseSink(object)
		if decodeErr != nil {
			return nil, decodeErr
		}
		return r.clickHouseProfile(ctx, ref, sink, over), nil

	case KindS3Sink:
		if over != (ProfileOverrides{}) {
			return nil, exit.UsageErrorf("%s is an object store: it has no dialled endpoint, no user "+
				"and no password, so there is nothing for --%s, --%s, --%s, --%s or --%s to replace",
				ref, options.FlagAddr, options.FlagUsername, options.FlagPasswordEnv,
				options.FlagPasswordFile, options.FlagTLS)
		}
		sink, decodeErr := decodeS3Sink(object)
		if decodeErr != nil {
			return nil, decodeErr
		}
		return s3Profile(ref, sink), nil
	}
	return nil, exit.RuntimeErrorf("no sink kind named %q", ref.Kind)
}

// clickHouseProfile derives a ClickHouse stanza from a ClickHouseSink.
//
// The database and the username are written *defaulted* rather than left empty,
// because a profile is a complete description of where to read from and an empty
// database leaves the server's own default — which is not the one the operator
// wrote to. The CRD's defaults are the same constants the resolution chain applies
// (see clickHouseTarget), so a profile written from a sink reads what that sink
// writes.
func (r *BackendResolver) clickHouseProfile(
	ctx context.Context, ref SinkRef, sink *v1alpha1.ClickHouseSink, over ProfileOverrides,
) *SinkProfile {
	connection := sink.Spec.Connection

	stanza := &ClickHouseProfile{
		Addr:     connection.Addr,
		Database: valueOr(connection.Database, DefaultClickHouseDatabase),
		Username: valueOr(over.Username, valueOr(connection.Username, DefaultClickHouseUsername)),
		TLS:      over.TLS,
	}
	switch {
	case over.PasswordEnv != "":
		stanza.PasswordEnv = over.PasswordEnv
	case over.PasswordFile != "":
		stanza.PasswordFile = over.PasswordFile
	default:
		// The variable Task 13.1's message tells this user to export and
		// docs/CLI.md names throughout. A profile with no password reference at
		// all would validate and then authenticate as nobody, which is not what
		// "complete and usable" means.
		stanza.PasswordEnv = passwordEnvName
	}

	derived := &SinkProfile{
		Ref:                ref,
		Profile:            Profile{Backend: BackendClickHouse, ClickHouse: stanza},
		RecordedAddr:       connection.Addr,
		UsernameOverridden: over.Username != "",
	}
	credential, unverified := r.verifyCredential(ctx, connection.CredentialsSecretRef)
	derived.Credential, derived.CredentialUnverified = credential, unverified

	// The three cases of the address rule, in the order they are decided. The
	// override wins outright — it is the common case, and the one Task 13.1's
	// message tells the reader to run. A recorded address that resolves from
	// anywhere is written unchanged, because a user reaching a public endpoint
	// wants what the custom resource says.
	switch {
	case over.Addr != "":
		stanza.Addr = over.Addr
		derived.AddrOverridden = true

	case ClusterInternalAddr(connection.Addr):
		_, port := splitAddr(connection.Addr)
		stanza.Addr = net.JoinHostPort(loopbackHost, port)
		derived.AddrRewritten = true
		// Built from the same diagnosis serviceTarget serves the unreachable
		// message from, so the port-forward printed here and the one printed
		// there are one sentence with one implementation. The namespace is the
		// fallback for a bare single-label host, which states none of its own.
		fallback := ""
		if credential != nil {
			fallback = credential.Namespace
		}
		service, namespace, forwarded := diagnosis{addr: connection.Addr, namespace: fallback}.serviceTarget()
		derived.PortForward = fmt.Sprintf("kubectl port-forward -n %s svc/%s %s:%s",
			namespace, service, forwarded, forwarded)
	}
	return derived
}

// s3Profile derives an S3 stanza from an S3Sink.
//
// Everything transfers directly. An object store is addressed by bucket, prefix,
// region and an optional endpoint URL, none of which a reader outside the cluster
// has to change to reach a bucket the operator writes to — and its credentials are
// not in this file at all, by the same permanent decision S3Profile documents.
func s3Profile(ref SinkRef, sink *v1alpha1.S3Sink) *SinkProfile {
	spec := sink.Spec
	return &SinkProfile{
		Ref: ref,
		Profile: Profile{Backend: BackendS3, S3: &S3Profile{
			Bucket:         spec.Bucket,
			Prefix:         spec.Prefix,
			Region:         valueOr(spec.Region, DefaultS3Region),
			Endpoint:       spec.Endpoint,
			ForcePathStyle: spec.ForcePathStyle,
		}},
		EndpointInternal: clusterInternalEndpoint(spec.Endpoint),
	}
}

// clusterInternalEndpoint reports whether an S3 endpoint URL names a host that
// resolves only inside the cluster.
//
// The same classifier as the ClickHouse address, applied to the host half of a
// URL. It changes nothing — see Explain for why an endpoint is not rewritten — and
// exists so that the case can be *said*, which is the difference between an
// archive a user cannot reach and an archive a user cannot reach for a reason
// nobody mentioned (Invariant 9).
func clusterInternalEndpoint(endpoint string) bool {
	if endpoint == "" {
		return false
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return false
	}
	return ClusterInternalAddr(parsed.Host)
}

// verifyCredential confirms that the Secret a sink names holds the key it names.
//
// It reads the Secret and looks at its key set. The value is never touched: the
// profile references an environment variable, so the password has no destination
// here, and a function that extracted it would be one refactor away from writing
// it to a file.
//
// Every failure is a sentence rather than an error. reasonFor and describeKeys are
// the two helpers discovery already has for exactly these two messages — "you were
// not allowed to look" and "here is what the Secret does hold" — and the second is
// the one that catches a Secret created with --from-literal=PASSWORD=…, which is
// invisible until something says the key it looked for was `password`.
func (r *BackendResolver) verifyCredential(
	ctx context.Context, ref v1alpha1.SecretReference,
) (*SinkCredential, string) {
	namespace, err := r.secretNamespace(ctx, ref)
	if err != nil {
		return &SinkCredential{Name: ref.Name, Key: secretKeyPassword},
			fmt.Sprintf("its namespace could not be worked out (%s)", reasonFor(err))
	}
	credential := &SinkCredential{Namespace: namespace, Name: ref.Name, Key: secretKeyPassword}

	clients, err := r.clients()
	if err != nil {
		return credential, fmt.Sprintf("it could not be read (%s)", reasonFor(err))
	}
	secret, err := clients.Typed.CoreV1().Secrets(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return credential, fmt.Sprintf("it could not be read (%s)", reasonFor(err))
	}
	if _, ok := secret.Data[secretKeyPassword]; !ok {
		return credential, fmt.Sprintf("it holds no %q key (keys present: %s)",
			secretKeyPassword, describeKeys(secret.Data))
	}
	return credential, ""
}

// Explain is what the command prints to stderr about what it derived.
//
// It says three things, in the order somebody reads them: what the custom resource
// recorded, what this profile says instead and why, and where the password comes
// from. The first two are the honesty property --sink-addr also keeps — a profile
// whose address differs from the sink it was derived from must say so at the
// moment it is written, because nothing later will.
//
// colorize is decided by the caller from --color, NO_COLOR and whether stderr is a
// terminal, for the same reason UnreachableSinkError.Render takes it as an
// argument: a function that consulted the environment itself would have golden
// files that changed with the shell they were generated in. Only the lines meant
// to be typed are painted.
func (p *SinkProfile) Explain(colorize bool) string {
	paint := diagnosticPalette{enabled: colorize}

	var out strings.Builder
	line := func(text string) { out.WriteString(text + "\n") }

	switch p.Profile.Backend {
	case BackendClickHouse:
		p.explainClickHouse(line, paint)
	case BackendS3:
		p.explainS3(line)
	}
	return out.String()
}

// explainClickHouse is the address paragraph and the credential paragraph.
//
// The recorded address gets a line to itself, unwrapped, for the reason the
// unreachable-backend message gives it one: it is the string a reader may need to
// compare character by character with what they have in a manifest.
func (p *SinkProfile) explainClickHouse(line func(string), paint diagnosticPalette) {
	stanza := p.Profile.ClickHouse

	line(fmt.Sprintf("%s records %s.", p.Ref, p.RecordedAddr))
	line("")
	switch {
	case p.AddrRewritten:
		line("That name resolves inside the cluster and nowhere else, so the profile records")
		line(fmt.Sprintf("%s instead and expects a forwarded port beside it:", stanza.Addr))
		line("")
		line(paint.bold("    " + p.PortForward))
	case p.AddrOverridden:
		line(fmt.Sprintf("The profile records %s instead, as --%s asked.", stanza.Addr, options.FlagAddr))
	default:
		line("That is not a cluster-internal name, so the profile records it unchanged.")
	}
	line("")
	if p.UsernameOverridden {
		line(fmt.Sprintf("Database %s is the sink's own; the user is %s, as --%s asked.",
			stanza.Database, stanza.Username, options.FlagUsername))
	} else {
		line(fmt.Sprintf("Database %s and user %s are the sink's own.", stanza.Database, stanza.Username))
	}
	if stanza.TLS {
		line(fmt.Sprintf("It connects over TLS, as --%s asked: spec.connection cannot state that.",
			options.FlagTLS))
	}
	p.explainCredential(line)

	// The reference gets a line of its own and the advice two fixed ones, so that
	// a long variable name or a long path lengthens one line rather than
	// reflowing the paragraph under it.
	switch {
	case stanza.PasswordEnv != "":
		line(fmt.Sprintf("The profile does not copy it: it reads $%s.", stanza.PasswordEnv))
		line("Export a read-only ClickHouse user's password there rather than the operator's,")
		line("which is a credential that can write to the audit trail. See")
		line(docsReadOnlyUser)
	case stanza.PasswordFile != "":
		line(fmt.Sprintf("The profile does not copy it: it reads the file %s.", stanza.PasswordFile))
		line("Put a read-only ClickHouse user's password there rather than the operator's,")
		line("which is a credential that can write to the audit trail. See")
		line(docsReadOnlyUser)
	}
}

// explainCredential says where the sink's own credential lives, and whether this
// command was able to look at it.
//
// Naming the Secret is not naming a secret: a namespace, a name and a key are how
// an engineer finds the thing they are about to mint a read-only alternative to.
// The value is not read at all — see verifyCredential.
func (p *SinkProfile) explainCredential(line func(string)) {
	if p.Credential == nil {
		line("The sink names no credentials Secret.")
		return
	}
	line(fmt.Sprintf("Its own credential is Secret %s, key %q.", p.Credential, p.Credential.Key))
	if p.CredentialUnverified != "" {
		line(fmt.Sprintf("That Secret was not checked: %s.", p.CredentialUnverified))
	}
}

// explainS3 is the archive's paragraph: what transferred, and where credentials
// come from instead.
func (p *SinkProfile) explainS3(line func(string)) {
	stanza := p.Profile.S3

	line(fmt.Sprintf("%s records the archive at s3://%s, region %s.",
		p.Ref, joinBucketPrefix(stanza.Bucket, stanza.Prefix), stanza.Region))
	line("")
	line("An object store has no address that resolves only inside a cluster, so all of it")
	line("transfers unchanged.")
	if p.EndpointInternal {
		line("")
		line(fmt.Sprintf("Its endpoint %s resolves inside the cluster too.", stanza.Endpoint))
		line("")
		line("It is recorded as it stands: an endpoint carries a scheme and a certificate name as")
		line("well as a host, so substituting a forwarded port for it is not a guess this command")
		line("will make. Edit s3.endpoint in the configuration file if you read it from outside.")
	}
	line("")
	line("Credentials are not in this file and never will be: the AWS chain resolves them —")
	line("AWS_ACCESS_KEY_ID and friends, ~/.aws/config, SSO, an instance role.")
}
