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

package harness

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// The manifests a scenario applies, rendered as strings and piped through
// KubectlStdin. See that function for why they are not templated onto disk.

// RuleResource is one entry of a rule's spec.resources.
type RuleResource struct {
	Group   string
	Version string
	Kind    string
}

// sinkYAML renders a rule's spec.sink block at the given indent.
//
// An empty sinkKind leaves `kind` out of the manifest altogether, which is what
// carries the CRD's default all the way through a real cluster rather than only
// through envtest: the chaos suite asserts on the metric label
// "ClickHouseSink/<name>" (see its sinkLabel), so a default that stopped being
// applied surfaces as a failing assertion rather than as a rule nobody noticed
// was pointed elsewhere. Every renderer below that takes only a name therefore
// keeps omitting it.
//
// A non-empty sinkKind is spelled out, which is what a rule naming anything other
// than a ClickHouseSink has to do — a rule meaning to archive to an S3Sink must
// say so, since the default is not "whatever sink exists" (see
// v1alpha1.SinkReference).
func sinkYAML(sinkKind, sinkName, indent string) string {
	if sinkKind == "" {
		return fmt.Sprintf("%ssink:\n%s  name: %q\n", indent, indent, sinkName)
	}
	return fmt.Sprintf("%ssink:\n%s  kind: %q\n%s  name: %q\n",
		indent, indent, sinkKind, indent, sinkName)
}

// resourcesYAML renders a rule's spec.resources list at the given indent.
func resourcesYAML(resources []RuleResource, indent string) string {
	var b strings.Builder
	for _, r := range resources {
		fmt.Fprintf(&b, "%s- group: %q\n%s  version: %q\n%s  kind: %q\n",
			indent, r.Group, indent, r.Version, indent, r.Kind)
	}
	return b.String()
}

// StreamRuleYAML renders a namespaced StreamRule streaming to sinkName, leaving
// the sink's kind to the CRD default.
//
// The sink is a parameter rather than a constant here because which sink a
// scenario streams to is the suite's decision, not the vocabulary's — the two
// suites install their own, tuned differently on purpose.
func StreamRuleYAML(namespace, name, sinkName string, resources []RuleResource) string {
	return StreamRuleYAMLForSinkKind(namespace, name, "", sinkName, resources)
}

// StreamRuleYAMLForSinkKind renders a namespaced StreamRule naming its sink's
// kind explicitly.
//
// It exists for the sinks the default does not cover: an S3Sink is reached by a
// rule that says so, and a rule that omitted the kind would resolve to a
// ClickHouseSink of the same name — which either does not exist (the rule parks
// with SinkMissing) or does, and quietly streams to the wrong backend. That is
// the failure this renderer's existence prevents in the S3 scenarios.
//
// StreamRuleYAML stays the way in for everything else, so the scenarios that say
// nothing about a sink kind keep rendering byte-identical manifests and keep
// covering the CRD's default (see sinkYAML).
func StreamRuleYAMLForSinkKind(namespace, name, sinkKind, sinkName string, resources []RuleResource) string {
	return fmt.Sprintf(`apiVersion: kuberecord.io/v1alpha1
kind: StreamRule
metadata:
  name: %s
  namespace: %s
spec:
%s  resources:
%s`, name, namespace, sinkYAML(sinkKind, sinkName, "  "), resourcesYAML(resources, "  "))
}

// RedactionEntry is one entry of a rule's spec.extraRedaction (or a sink's
// spec.policy.redaction — they are the same type). Exactly one field is set,
// which the CRD enforces; rendering both would produce a manifest the API server
// rejects, and a scenario asserting on redaction would fail for the wrong reason.
type RedactionEntry struct {
	FieldPath  string
	Annotation string
}

// redactionYAML renders an extraRedaction list at the given indent.
func redactionYAML(entries []RedactionEntry, indent string) string {
	var b strings.Builder
	for _, e := range entries {
		if e.Annotation != "" {
			fmt.Fprintf(&b, "%s- annotation: %q\n", indent, e.Annotation)
			continue
		}
		fmt.Fprintf(&b, "%s- fieldPath: %q\n", indent, e.FieldPath)
	}
	return b.String()
}

// RedactingStreamRuleYAML renders a namespaced StreamRule that scrubs the given
// paths out of every object it streams (Task 3.3).
//
// It is a separate renderer rather than a variadic on StreamRuleYAML so that the
// scenarios which say nothing about redaction keep rendering byte-identical
// manifests to the ones they rendered before redaction existed.
func RedactingStreamRuleYAML(namespace, name, sinkName string, resources []RuleResource,
	redaction []RedactionEntry) string {
	return fmt.Sprintf(`apiVersion: kuberecord.io/v1alpha1
kind: StreamRule
metadata:
  name: %s
  namespace: %s
spec:
%s  resources:
%s  extraRedaction:
%s`, name, namespace, sinkYAML("", sinkName, "  "),
		resourcesYAML(resources, "  "), redactionYAML(redaction, "  "))
}

// ClusterStreamRuleYAML renders a cluster-scoped ClusterStreamRule with no
// namespaceSelector, i.e. one all-namespaces target per named resource.
func ClusterStreamRuleYAML(name, sinkName string, resources []RuleResource) string {
	return fmt.Sprintf(`apiVersion: kuberecord.io/v1alpha1
kind: ClusterStreamRule
metadata:
  name: %s
spec:
%s  resources:
%s`, name, sinkYAML("", sinkName, "  "), resourcesYAML(resources, "  "))
}

// DeploymentYAML renders the object the workload scenarios stream.
//
// The pause image is what every kind node already has cached, so no scenario
// ever waits on a registry pull; whether the pods actually run is irrelevant,
// since what is being watched is the Deployment object itself.
func DeploymentYAML(namespace, name string, replicas int) string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
  namespace: %s
  labels:
    app: %s
spec:
  replicas: %d
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      containers:
      - name: pause
        image: registry.k8s.io/pause:3.10
        imagePullPolicy: IfNotPresent
`, name, namespace, name, replicas, name, name)
}

// IngressYAML renders a minimal, valid Ingress. It needs no ingress controller
// to exist: the scenario watches the object, not the traffic.
func IngressYAML(namespace, name string) string {
	return fmt.Sprintf(`apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: %s
  namespace: %s
spec:
  rules:
  - host: %s.e2e.kuberecord.io
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: %s
            port:
              number: 80
`, name, namespace, name, name)
}

// CrashLoopPodYAML renders a Pod that is guaranteed to enter CrashLoopBackOff:
// the container exits non-zero immediately and the default restartPolicy brings
// it straight back.
//
// It exists to *manufacture Events*, which is the only way to test the count-bump
// case honestly. The kubelet emits a `BackOff` Event for a crash-looping
// container and then updates that same Event in place — same name, same UID, a
// higher `count` — every time it backs off again. That in-place update is the
// case naive Event exporters drop, so the fixture has to produce a real one
// rather than a Event authored by the test.
//
// The pause image is used with an argument it does not understand so the
// container fails without any registry pull: kind nodes already have it cached.
func CrashLoopPodYAML(namespace, name string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  restartPolicy: Always
  terminationGracePeriodSeconds: 1
  containers:
  - name: crasher
    image: registry.k8s.io/pause:3.10
    imagePullPolicy: IfNotPresent
    command: ["/no-such-binary"]
`, name, namespace)
}

// ConfigMapYAML renders a ConfigMap carrying data.
//
// It is the chaos suite's workhorse object: nothing schedules for it, so a
// hundred of them cost the cluster under test nothing, and its arbitrary string
// values are how that suite dials a record's size — from an ordinary few hundred
// bytes up to the oversized payload its poison-row scenario needs.
//
// Keys are emitted in sorted order and values are Go-quoted, so a payload
// containing newlines, quotes or a megabyte of filler stays a single valid YAML
// scalar and the rendered manifest is byte-stable across calls.
func ConfigMapYAML(namespace, name string, data map[string]string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `apiVersion: v1
kind: ConfigMap
metadata:
  name: %s
  namespace: %s
data:
`, name, namespace)
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(&b, "  %s: %s\n", key, strconv.Quote(data[key]))
	}
	return b.String()
}
