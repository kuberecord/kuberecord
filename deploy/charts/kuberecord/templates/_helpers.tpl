{{/*
Naming and labelling helpers.

Every name this chart produces is deliberately identical to what
`kustomize build config/default` produces for the same object — with the
conventional release name `kuberecord`, both paths install
`kuberecord-controller-manager`, `kuberecord-metrics-reader`,
`kuberecord-watcher-core-workloads` and so on. That is not cosmetic: it is what
lets the acceptance suite (test/e2e) run against either install path without a
single assertion changing, and what lets test/chart compare the two renderings
object for object.
*/}}

{{/* Chart name, overridable. */}}
{{- define "kuberecord.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Release-qualified base name. A release named after the chart (the documented
`helm install kuberecord ...`) collapses to plain `kuberecord`, which is what
makes the kustomize-identical names above fall out.
*/}}
{{- define "kuberecord.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/* The manager Deployment, its ServiceAccount and everything named after it. */}}
{{- define "kuberecord.managerName" -}}
{{- printf "%s-controller-manager" (include "kuberecord.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "kuberecord.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Selector labels: the two labels the shipped Deployment and metrics Service select
on. A Deployment's selector is immutable, so nothing user-supplied may ever land
here — see the `extraLabels` note in values.yaml.
*/}}
{{- define "kuberecord.selectorLabels" -}}
control-plane: controller-manager
app.kubernetes.io/name: {{ include "kuberecord.name" . }}
{{- end }}

{{/* Common labels, carried by every object the chart renders. */}}
{{- define "kuberecord.labels" -}}
{{ include "kuberecord.selectorLabels" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ include "kuberecord.chart" . }}
{{- with .Values.extraLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{- define "kuberecord.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "kuberecord.managerName" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
The image reference. A digest pins by content and therefore wins over any tag;
an empty tag follows the chart's appVersion, so `helm upgrade` to a newer chart
moves the operator with it.
*/}}
{{- define "kuberecord.image" -}}
{{- if .Values.image.digest }}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else }}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}
{{- end }}

{{/*
Manager arguments: the flags this chart owns, then whatever the user adds. The
metrics endpoint is disabled by passing no --metrics-bind-address at all rather
than by passing "0": the binary's own default is already off, so an install with
metrics off carries no argument to contradict.
*/}}
{{- define "kuberecord.managerArgs" -}}
{{- if .Values.leaderElection.enabled }}
- --leader-elect
{{- end }}
- --health-probe-bind-address=:{{ .Values.healthProbe.port }}
{{- if .Values.metrics.enabled }}
- --metrics-bind-address=:{{ .Values.metrics.port }}
{{- if not .Values.metrics.secure }}
- --metrics-secure=false
{{- end }}
{{- end }}
{{- with .Values.extraArgs }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Fail fast on value combinations that would install a broken operator, rather
than leaving the user to discover it from the pods.
*/}}
{{- define "kuberecord.validateValues" -}}
{{- if and (gt (int .Values.replicaCount) 1) (not .Values.leaderElection.enabled) }}
{{- fail "kuberecord: replicaCount > 1 requires leaderElection.enabled=true — two active operators would double-write every row" }}
{{- end }}
{{- include "kuberecord.validateSinks" . }}
{{- end }}

{{/*
The sink kinds this chart can create: the sink CRDs it ships in crds/.

The list is here rather than derived, because there is nothing to derive it from
at render time — and it is worth spelling out, since a kind that is not on it is
one the installed operator has no reconciler for. Rendering such a CR would
produce a manifest the API server rejects for a *missing CRD*, which names
neither the typo that caused it nor the file it is in. Adding a backend means
adding its CRD to crds/ (via `make helm-sync`) and its kind here.
*/}}
{{- define "kuberecord.sinkKinds" -}}
ClickHouseSink S3Sink
{{- end }}

{{/*
Validate the `sinks` list, per entry, before any of it is rendered.

Only four things are checked, and the boundary is deliberate. Everything about
whether a *spec* is valid belongs to the CRD's structural schema and its CEL
rules — duplicating any of it here would mean two validators to keep in
step, and the chart's copy would be the stale one. What the API server cannot
catch in time, or cannot phrase usefully, is what is checked:

  - an unknown `kind`, which fails as a missing CRD rather than as a typo;
  - a missing `name`, which is what rules name in spec.sink.name;
  - a duplicated (kind, name) identity — the pair is a sink's identity, so
    two entries sharing both are one object rendered twice, and Helm would apply
    the second over the first with no warning;
  - the one connection field per backend that has no default and cannot be
    guessed, so the failure lands at `helm install` rather than on a CR that
    admits and then never connects.
*/}}
{{- define "kuberecord.validateSinks" -}}
{{- $kinds := splitList " " (include "kuberecord.sinkKinds" .) }}
{{- $seen := dict }}
{{- range $i, $sink := .Values.sinks }}
{{- $at := printf "sinks[%d]" $i }}
{{- if not (kindIs "map" $sink) }}
{{- fail (printf "kuberecord: %s is not a mapping — each entry is {kind, name, spec}" $at) }}
{{- end }}
{{- if not (has $sink.kind $kinds) }}
{{- fail (printf "kuberecord: %s.kind=%s is not a sink kind this release serves; expected one of %s" $at ($sink.kind | default "<empty>" | quote) (join ", " $kinds)) }}
{{- end }}
{{- if not $sink.name }}
{{- fail (printf "kuberecord: %s has no name — the name is what a rule names in spec.sink.name" $at) }}
{{- end }}
{{- $identity := printf "%s/%s" $sink.kind $sink.name }}
{{- if hasKey $seen $identity }}
{{- fail (printf "kuberecord: %s duplicates %s — a sink is identified by its (kind, name) pair, so both entries are the same object and only the last would survive" $at $identity) }}
{{- end }}
{{- $_ := set $seen $identity true }}
{{- if not (kindIs "map" $sink.spec) }}
{{- fail (printf "kuberecord: %s has no spec — it is passed through verbatim to the %s CR, so an empty one creates a sink that configures nothing" $at $sink.kind) }}
{{- end }}
{{- if eq $sink.kind "ClickHouseSink" }}
{{- if not (dig "connection" "addr" "" $sink.spec) }}
{{- fail (printf "kuberecord: %s needs spec.connection.addr (host:9000 of ClickHouse's native protocol)" $at) }}
{{- end }}
{{- if not (dig "connection" "credentialsSecretRef" "name" "" $sink.spec) }}
{{- fail (printf "kuberecord: %s needs spec.connection.credentialsSecretRef.name — create the Secret yourself in the release namespace; the chart never templates a password" $at) }}
{{- end }}
{{- else if eq $sink.kind "S3Sink" }}
{{- if not (dig "bucket" "" $sink.spec) }}
{{- fail (printf "kuberecord: %s needs spec.bucket — kuberecord never creates a bucket, because a bucket carries retention, encryption and Object Lock settings that are yours to own" $at) }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
