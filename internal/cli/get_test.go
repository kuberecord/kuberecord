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

package cli_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// `get --at`, and the two things about a reconstruction that no error reports.
//
// The first is that a Checkpoint carries both the full state and the diff that
// produced it, and applying the second over the first yields a document the object
// was never in. The failure is silent for a replace — a value set twice looks like
// a value set once — so the fixture below uses an array append, where a
// re-application is visible as a third container that never existed.
//
// The second is that a replay can run without error and still be wrong. --verify
// is the check for it, and the fixtures carry real digests so that the check is
// being exercised rather than asserted around.

// The reconstruction fixture: a first sighting, then a checkpoint that adds a
// sidecar and carries both the state and the patch describing the addition.
const (
	checkpointBaseState = `{"apiVersion":"apps/v1","kind":"Deployment",` +
		`"metadata":{"name":"checkout","namespace":"payments","uid":"` + fixtureUID + `"},` +
		`"spec":{"template":{"spec":{"containers":[` +
		`{"name":"checkout","image":"registry.example/checkout:1.4.2"}]}}}}`

	checkpointState = `{"apiVersion":"apps/v1","kind":"Deployment",` +
		`"metadata":{"name":"checkout","namespace":"payments","uid":"` + fixtureUID + `"},` +
		`"spec":{"template":{"spec":{"containers":[` +
		`{"name":"checkout","image":"registry.example/checkout:1.4.2"},` +
		`{"name":"envoy","image":"registry.example/envoy:1.29"}]}}}}`

	// The checkpoint's own diff, which describes the transition its data already
	// reflects. Re-applying it appends the sidecar a second time, which is what
	// makes the rule testable rather than merely stated.
	checkpointDiff = `[{"op":"add","path":"/spec/template/spec/containers/-",` +
		`"value":{"name":"envoy","image":"registry.example/envoy:1.29"}}]`
)

// canonicalSHA is the digest the schema's sha256 column holds for a state: the
// hex SHA-256 of the document re-serialized with sorted object keys.
//
// The fixtures compute it rather than hard-coding one, because a hard-coded digest
// is a digest that stops matching the state beside it the first time somebody edits
// the fixture — and the test would then be asserting that --verify reports a
// mismatch, which is the opposite of what it is for.
func canonicalSHA(t *testing.T, state string) string {
	t.Helper()

	var decoded map[string]any
	if err := json.Unmarshal([]byte(state), &decoded); err != nil {
		t.Fatalf("the fixture state is not valid JSON: %v", err)
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("re-encoding the fixture state: %v", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// checkpointEngine is a backend whose StateAt really replays the history below.
func checkpointEngine(t *testing.T) *fakeEngine {
	t.Helper()

	return &fakeEngine{
		caps:   clickHouseCapabilities(),
		replay: true,
		changes: []query.Change{
			{
				TS: at("2026-08-28T14:02:58.001Z"), EventType: query.EventAdded, UID: fixtureUID,
				Actors: []string{"kubectl-client-side-apply"}, ResourceVersion: "1001",
				APIVersion: "apps/v1", Data: checkpointBaseState,
				SHA256: canonicalSHA(t, checkpointBaseState),
			},
			{
				TS: at("2026-08-28T14:05:02.117Z"), EventType: query.EventCheckpoint, UID: fixtureUID,
				Actors: []string{"kubectl-client-side-apply"}, ResourceVersion: "1002",
				APIVersion: "apps/v1", Data: checkpointState, Diff: checkpointDiff,
				SHA256: canonicalSHA(t, checkpointState),
			},
		},
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}
}

// runGet drives the command against a fake engine and returns both streams.
func runGet(
	t *testing.T, engine *fakeEngine, request cli.GetRequest,
) (stdout, stderr string, err error) {
	t.Helper()

	var out, errOut bytes.Buffer
	backend := &cli.Backend{Engine: engine, ClusterID: fixtureCluster}
	err = cli.RunGet(context.Background(), backend, request,
		ioStreams(&out, &errOut), render.Options{Width: goldenWidth})
	return out.String(), errOut.String(), err
}

// getRequest is a `get deploy/checkout -n payments --at …` for the fixture.
func getRequest(format render.StructuredFormat) cli.GetRequest {
	return cli.GetRequest{
		Ref:    fixtureRef(),
		At:     at("2026-08-28T15:00:00Z"),
		Format: format,
	}
}

// TestGetReconstructsStateAsYAML is the flagship of this command, header and all.
func TestGetReconstructsStateAsYAML(t *testing.T) {
	stdout, stderr, err := runGet(t, checkpointEngine(t), getRequest(render.StructuredYAML))
	if err != nil {
		t.Fatalf("RunGet: %v", err)
	}
	assertGoldenIn(t, "get", "yaml", stdout, stderr)

	// The one line that must survive every future edit to this output.
	if !strings.Contains(stdout, "NOT A DEPLOYABLE MANIFEST") {
		t.Errorf("the mandatory header is missing, so somebody will apply this:\n%s", stdout)
	}
}

// TestGetReconstructsStateAsJSON covers the format with no comment syntax.
func TestGetReconstructsStateAsJSON(t *testing.T) {
	stdout, stderr, err := runGet(t, checkpointEngine(t), getRequest(render.StructuredJSON))
	if err != nil {
		t.Fatalf("RunGet: %v", err)
	}
	assertGoldenIn(t, "get", "json", stdout, stderr)

	var decoded map[string]any
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("stdout is not valid JSON, so a script cannot read it: %v\n%s", err, stdout)
	}
	assertEnvelope(t, decoded, render.KindObject)
}

// TestGetDoesNotReapplyACheckpointsOwnDiff is the acceptance criterion, tested
// explicitly.
//
// The checkpoint's data already holds the sidecar its diff describes. A
// reconstruction that applied both would produce a Deployment with three
// containers — an object that never existed, assembled without a single error
// being reported.
func TestGetDoesNotReapplyACheckpointsOwnDiff(t *testing.T) {
	stdout, _, err := runGet(t, checkpointEngine(t), getRequest(render.StructuredJSON))
	if err != nil {
		t.Fatalf("RunGet: %v", err)
	}

	containers := containersOf(t, stdout)
	if len(containers) != 2 {
		t.Fatalf("the reconstruction holds %d containers, want 2; the checkpoint's own diff was "+
			"re-applied over the state it already produced:\n%s", len(containers), stdout)
	}
	for i, want := range []string{"checkout", "envoy"} {
		container, ok := containers[i].(map[string]any)
		if !ok {
			t.Fatalf("container %d is not an object: %#v", i, containers[i])
		}
		if container["name"] != want {
			t.Errorf("container %d is %q, want %q", i, container["name"], want)
		}
	}
}

// TestGetUsesTheCheckpointAsTheBase is the other half of the same rule.
//
// The base is the newest data-bearing row, which for this fixture is the
// checkpoint — not the first sighting with the checkpoint's patch replayed over
// it. The provenance header is where a reader checks that, so that is where it is
// asserted.
func TestGetUsesTheCheckpointAsTheBase(t *testing.T) {
	stdout, _, err := runGet(t, checkpointEngine(t), getRequest(render.StructuredYAML))
	if err != nil {
		t.Fatalf("RunGet: %v", err)
	}

	for _, want := range []string{
		"# base row:        2026-08-28T14:05:02Z (Checkpoint)",
		"# patches applied: 0",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("the replay did not start from the checkpoint.\nwant %q\ngot:\n%s", want, stdout)
		}
	}
}

// containersOf digs the container list out of a rendered JSON document.
func containersOf(t *testing.T, document string) []any {
	t.Helper()

	var decoded map[string]any
	if err := json.Unmarshal([]byte(document), &decoded); err != nil {
		t.Fatalf("the reconstruction is not valid JSON: %v", err)
	}
	spec, ok := envelopeObject(t, decoded)["spec"].(map[string]any)
	if !ok {
		t.Fatalf("the reconstruction has no spec: %#v", decoded)
	}
	template, ok := spec["template"].(map[string]any)
	if !ok {
		t.Fatalf("the reconstruction has no pod template: %#v", spec)
	}
	podSpec, ok := template["spec"].(map[string]any)
	if !ok {
		t.Fatalf("the pod template has no spec: %#v", template)
	}
	containers, ok := podSpec["containers"].([]any)
	if !ok {
		t.Fatalf("the pod spec has no containers: %#v", podSpec)
	}
	return containers
}

// envelopeObject digs the reconstructed state out of a decoded Object envelope.
//
// The state is at .items[0].object rather than at the root, which is the shape
// D19 fixes for every answer this CLI gives. It is worth a helper rather than an
// index expression because the assertion it makes on the way past — that the
// envelope is the one it claims to be — is the contract these tests exist to pin.
func envelopeObject(t *testing.T, decoded map[string]any) map[string]any {
	t.Helper()

	items := assertEnvelope(t, decoded, render.KindObject)
	if len(items) != 1 {
		t.Fatalf("a reconstruction is one item, and this envelope holds %d", len(items))
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("the item is not an object: %#v", items[0])
	}
	state, ok := item["object"].(map[string]any)
	if !ok {
		t.Fatalf("the item carries no reconstructed object: %#v", item)
	}
	return state
}

// TestGetVerifyReportsAMatch covers the successful half of the chain-of-custody
// check.
func TestGetVerifyReportsAMatch(t *testing.T) {
	request := getRequest(render.StructuredYAML)
	request.Verify = true

	stdout, stderr, err := runGet(t, checkpointEngine(t), request)
	if err != nil {
		t.Fatalf("--verify failed against a reconstruction that matches its digest: %v\n%s", err, stderr)
	}
	if !strings.Contains(stderr, "verified: the reconstructed state hashes to") {
		t.Errorf("a successful verification was not reported:\n%s", stderr)
	}
	if !strings.Contains(stderr, canonicalSHA(t, checkpointState)) {
		t.Errorf("the reported digest is not the recorded one:\n%s", stderr)
	}
	if strings.Contains(stdout, "verified:") {
		t.Errorf("the verification result reached stdout, where it would corrupt the document:\n%s", stdout)
	}
}

// TestGetVerifyReportsAMismatch is the finding the flag exists for.
//
// A reconstruction that does not hash to the digest recorded for it means the
// archive and the replay disagree about what this object looked like. That is a
// chain-of-custody finding, it exits 1, and the message has to say which two
// digests disagree so somebody can act on it.
func TestGetVerifyReportsAMismatch(t *testing.T) {
	engine := checkpointEngine(t)
	engine.changes[len(engine.changes)-1].SHA256 =
		"0000000000000000000000000000000000000000000000000000000000000000"

	request := getRequest(render.StructuredYAML)
	request.Verify = true

	stdout, _, err := runGet(t, engine, request)
	if err == nil {
		t.Fatal("--verify passed a reconstruction that does not match its recorded digest")
	}
	if code := cli.ExitCodeFor(err); code != cli.ExitRuntimeError {
		t.Errorf("exit code %d, want %d", code, cli.ExitRuntimeError)
	}

	// --verify is an assertion, so a failure withholds the document. Writing it
	// anyway would put a disputed reconstruction into
	// `kuberecord get … --verify > object.yaml`, which is precisely how somebody
	// uses this flag.
	if stdout != "" {
		t.Errorf("a reconstruction that failed verification was written anyway:\n%s", stdout)
	}
	for _, want := range []string{
		"chain-of-custody finding",
		canonicalSHA(t, checkpointState),
		"0000000000000000000000000000000000000000000000000000000000000000",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the mismatch message does not contain %q: %v", want, err)
		}
	}
}

// TestGetVerifyRefusesToPassWithoutADigest keeps Invariant 4 on the one path
// where a pass would be an invention.
//
// A row carrying no digest cannot be checked. Reporting success would be the tool
// manufacturing an assurance nobody gave it, and reporting a mismatch would be a
// finding about nothing.
func TestGetVerifyRefusesToPassWithoutADigest(t *testing.T) {
	engine := checkpointEngine(t)
	for i := range engine.changes {
		engine.changes[i].SHA256 = ""
	}

	request := getRequest(render.StructuredYAML)
	request.Verify = true

	stdout, _, err := runGet(t, engine, request)
	if err == nil {
		t.Fatal("--verify reported success for a reconstruction with no digest to compare against")
	}
	if !strings.Contains(err.Error(), "no digest is recorded") {
		t.Errorf("the message does not say why the check could not run: %v", err)
	}
	if stdout != "" {
		t.Errorf("an unverifiable reconstruction was written as though it had been checked:\n%s", stdout)
	}
}

// TestGetWithoutCoverageIsTheNoCoverageFinding is Invariant 9 applied to a
// reconstruction.
//
// "The object was not there" and "nothing was ever watching this kind" lead an
// engineer in opposite directions, and only the second is a finding about
// kuberecord rather than about the cluster.
func TestGetWithoutCoverageIsTheNoCoverageFinding(t *testing.T) {
	engine := &fakeEngine{caps: clickHouseCapabilities()}

	_, _, err := runGet(t, engine, getRequest(render.StructuredYAML))
	if err == nil {
		t.Fatal("a scope nobody watched was reported as success")
	}
	if code := cli.ExitCodeFor(err); code != cli.ExitNoCoverage {
		t.Errorf("exit code %d, want %d", code, cli.ExitNoCoverage)
	}
	if !errors.Is(err, query.ErrNoCoverage) {
		t.Errorf("the finding does not carry the read plane's own sentinel: %v", err)
	}
	if !strings.Contains(err.Error(), "not evidence that the object did not exist") {
		t.Errorf("the message reads as an absent object rather than as an unwatched one: %v", err)
	}
}

// TestGetWithCoverageButNoStateIsAnAbsence is the other side of that fork: the
// scope was watched, so the absence is a fact about the object.
func TestGetWithCoverageButNoStateIsAnAbsence(t *testing.T) {
	engine := &fakeEngine{
		caps:      clickHouseCapabilities(),
		intervals: watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}

	_, _, err := runGet(t, engine, getRequest(render.StructuredYAML))
	if err == nil {
		t.Fatal("a missing object was reported as success")
	}
	if code := cli.ExitCodeFor(err); code != cli.ExitRuntimeError {
		t.Errorf("exit code %d, want %d", code, cli.ExitRuntimeError)
	}
	if !strings.Contains(err.Error(), "The scope was watched over") {
		t.Errorf("the absence is reported without the coverage that makes it meaningful: %v", err)
	}
}

// TestGetReportsABackendThatCannotReconstruct covers Invariant 4's other half: a
// capability gap is reported as one and never as an empty answer.
func TestGetReportsABackendThatCannotReconstruct(t *testing.T) {
	engine := &fakeEngine{caps: archiveCapabilities(), stateErr: query.ErrCapabilityUnsupported}

	_, _, err := runGet(t, engine, getRequest(render.StructuredYAML))
	if err == nil {
		t.Fatal("a backend that cannot reconstruct state produced a successful empty answer")
	}
	if !strings.Contains(err.Error(), "cannot reconstruct state") {
		t.Errorf("the capability gap is not named: %v", err)
	}
	if !strings.Contains(err.Error(), archiveCapabilities().Backend) {
		t.Errorf("the message does not say which backend could not answer: %v", err)
	}
}

// TestGetNamesTheIncarnationItReconstructed keeps Invariant 7 visible.
//
// The UID in the header is read out of the reconstructed document rather than
// echoed from the flag, so it always names the incarnation the document is
// actually of.
func TestGetNamesTheIncarnationItReconstructed(t *testing.T) {
	request := getRequest(render.StructuredYAML)
	request.UID = fixtureUID

	stdout, _, err := runGet(t, checkpointEngine(t), request)
	if err != nil {
		t.Fatalf("RunGet: %v", err)
	}
	if !strings.Contains(stdout, "# uid:             "+fixtureUID) {
		t.Errorf("the header does not name the incarnation:\n%s", stdout)
	}
}
