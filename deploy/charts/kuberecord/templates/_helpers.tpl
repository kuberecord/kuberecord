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
{{- if and .Values.createDefaultSink (not .Values.defaultSink.connection.addr) }}
{{- fail "kuberecord: createDefaultSink=true requires defaultSink.connection.addr (host:9000 of your ClickHouse)" }}
{{- end }}
{{- if and .Values.createDefaultSink (not .Values.defaultSink.connection.credentialsSecretRef.name) }}
{{- fail "kuberecord: createDefaultSink=true requires defaultSink.connection.credentialsSecretRef.name — create the Secret yourself; the chart never templates a password" }}
{{- end }}
{{- end }}
