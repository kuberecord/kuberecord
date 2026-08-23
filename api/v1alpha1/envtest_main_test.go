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

package v1alpha1

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// k8sClient talks to the envtest API server that every validation test in this
// package shares.
//
// The CEL and structural rules on these types are enforced by the API server,
// not by Go code, so the only honest way to test them is to install the
// generated CRDs into a real apiserver and watch it reject bad objects. A
// single package-wide apiserver (rather than one per test) keeps that cheap:
// booting envtest dominates the runtime of these tests by orders of magnitude.
var k8sClient client.Client

// TestMain boots one envtest apiserver with the generated CRDs installed, runs
// the package's tests against it, and tears it down. ErrorIfCRDPathMissing is
// true here — unlike the controller suite, which can meaningfully run without
// CRDs — because a missing config/crd/bases would otherwise turn every
// rejection test into a silent pass.
func TestMain(m *testing.M) {
	os.Exit(runTestsWithEnvtest(m))
}

// runTestsWithEnvtest exists so the envtest teardown runs through a deferred
// call rather than racing os.Exit, which never runs defers.
func runTestsWithEnvtest(m *testing.M) (code int) {
	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	if dir := firstEnvtestBinaryDir(); dir != "" {
		testEnv.BinaryAssetsDirectory = dir
	}

	cfg, err := testEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start envtest (run `make setup-envtest`): %v\n", err)
		return 1
	}
	defer func() {
		if stopErr := testEnv.Stop(); stopErr != nil {
			fmt.Fprintf(os.Stderr, "failed to stop envtest: %v\n", stopErr)
			code = 1
		}
	}()

	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		fmt.Fprintf(os.Stderr, "failed to register kuberecord.io/v1alpha1: %v\n", err)
		return 1
	}
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build client: %v\n", err)
		return 1
	}

	return m.Run()
}

// firstEnvtestBinaryDir locates a downloaded envtest binary directory so these
// tests also run straight from an IDE, where KUBEBUILDER_ASSETS is not set by
// the Makefile. It mirrors the helper in the controller suite; an empty result
// simply leaves envtest to its own KUBEBUILDER_ASSETS lookup.
func firstEnvtestBinaryDir() string {
	basePath := filepath.Join("..", "..", "bin", "k8s")
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return ""
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return filepath.Join(basePath, entry.Name())
		}
	}
	return ""
}

// clientObject is an alias for client.Object, kept short because the shared
// rule table below passes builders and editors of this type around constantly.
type clientObject = client.Object

// apiCase is one admission expectation against a live apiserver.
//
// A case either checks *creation* (mutate == nil) or a *transition* (mutate
// non-nil: the object is created valid, then edited). Transition cases are the
// only way to exercise a CEL rule referencing oldSelf, which is exactly what
// the sink reference's immutability rule is.
type apiCase struct {
	// name doubles as the object's name, so every case gets a fresh object
	// without the table having to invent unique names by hand.
	name string
	// obj is the object to create. Its name is overwritten from name.
	obj clientObject
	// mutate, when set, edits the created object before an Update; the
	// expectation then applies to the Update rather than the Create.
	mutate func(clientObject)
	// wantErr is a substring the rejection message must contain. Empty means
	// the operation must succeed — asserting the *accepting* direction too,
	// so a rule that rejects everything cannot pass this suite.
	wantErr string
}

// runAPICases drives a table of apiCases against the shared envtest apiserver.
func runAPICases(t *testing.T, cases []apiCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			obj := tc.obj
			obj.SetName(objectNameFor(tc.name))

			createErr := k8sClient.Create(ctx, obj)
			if tc.mutate == nil {
				assertAPIResult(t, "create", createErr, tc.wantErr)
				if createErr == nil {
					deleteObject(ctx, t, obj)
				}
				return
			}

			if createErr != nil {
				t.Fatalf("setup: creating the valid object failed: %v", createErr)
			}
			defer deleteObject(ctx, t, obj)
			tc.mutate(obj)
			assertAPIResult(t, "update", k8sClient.Update(ctx, obj), tc.wantErr)
		})
	}
}

// assertAPIResult checks an apiserver call against the case's expectation. A
// rejection must be an Invalid error carrying wantErr: a 404 or a conflict that
// merely happens to fail would otherwise let a missing validation rule pass.
func assertAPIResult(t *testing.T, verb string, err error, wantErr string) {
	t.Helper()
	if wantErr == "" {
		if err != nil {
			t.Fatalf("%s: expected the object to be accepted, got: %v", verb, err)
		}
		return
	}
	if err == nil {
		t.Fatalf("%s: expected rejection containing %q, but the object was accepted", verb, wantErr)
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("%s: expected an Invalid (422) rejection, got: %v", verb, err)
	}
	if !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("%s: rejection message %q does not contain %q", verb, err.Error(), wantErr)
	}
}

// deleteObject removes a test object, reporting any failure rather than
// discarding it (Invariant 4: no silent errors, tests included).
func deleteObject(ctx context.Context, t *testing.T, obj client.Object) {
	t.Helper()
	if err := k8sClient.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		t.Errorf("cleanup: deleting %s failed: %v", obj.GetName(), err)
	}
}

// objectNameFor turns a subtest name into a DNS-1123 object name so a table's
// case names are also its object names.
func objectNameFor(caseName string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			return r
		case r >= 'A' && r <= 'Z':
			return r + ('a' - 'A')
		default:
			return '-'
		}
	}, caseName)
}

// ptrTo returns a pointer to v. The optional-with-default fields on these
// types are pointers so "unset" stays distinguishable from "explicitly zero",
// which is what lets a test drive a deliberately out-of-range 0.
func ptrTo[T any](v T) *T { return &v }

// testNamespace is the namespace every namespaced test object is created in;
// envtest provides it out of the box.
const testNamespace = "default"

// objectMeta builds the metadata shared by the builders below. The name is
// overwritten by runAPICases from the case name.
func objectMeta(namespace string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: "placeholder", Namespace: namespace}
}

// unsetRendering is what an optional field the apiserver applied no default to
// renders as in the defaulting tables. It is deliberately not a
// plausible-looking zero: "0" or "" would read as a default that fired and set
// that value, which is the one failure these tables exist to catch.
const unsetRendering = "<nil>"

// durationString, int32String and int64String render an optional field for
// comparison in the defaulting tables.
func durationString(d *metav1.Duration) string {
	if d == nil {
		return unsetRendering
	}
	return d.Duration.String()
}

func int32String(v *int32) string {
	if v == nil {
		return unsetRendering
	}
	return strconv.Itoa(int(*v))
}

func int64String(v *int64) string {
	if v == nil {
		return unsetRendering
	}
	return strconv.FormatInt(*v, 10)
}

// boolString renders a plain bool for the same tables. It takes a value rather
// than a pointer because a bool field defaulted to false is indistinguishable
// from an un-defaulted one either way, so a pointer would promise a distinction
// it cannot keep.
func boolString(v bool) string { return strconv.FormatBool(v) }
