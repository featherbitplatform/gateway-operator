{{/*
Chart name, release-scoped full name, labels.
*/}}
{{- define "featherbit-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "featherbit-operator.fullname" -}}
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

{{- define "featherbit-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "featherbit-operator.labels" -}}
helm.sh/chart: {{ include "featherbit-operator.chart" . }}
{{ include "featherbit-operator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "featherbit-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "featherbit-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Image reference: repository@digest, or repository:tag. The tag defaults to
appVersion so chart and operator versions move together.
*/}}
{{- define "featherbit-operator.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- if .Values.image.digest -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else -}}
{{ printf "%s:%s" .Values.image.repository $tag }}
{{- end -}}
{{- end }}

{{- define "featherbit-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "featherbit-operator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "featherbit-operator.webhookSecretName" -}}
{{ include "featherbit-operator.fullname" . }}-webhook-tls
{{- end }}

{{- define "featherbit-operator.webhookServiceName" -}}
{{ include "featherbit-operator.fullname" . }}-webhook
{{- end }}

{{/*
Webhook serving pair, computed ONCE per render and shared by webhook-cert.yaml
(the Secret) and webhook.yaml (the caBundle). Returns JSON with base64 values
{crt, key, ca}. An existing Secret (lookup, i.e. an upgrade) wins so the pair
survives `helm upgrade`; otherwise a fresh CA and signed cert are generated.
The result is memoised on the root context, so both includes see the same CA.
*/}}
{{- define "featherbit-operator.webhookCA" -}}
{{- if not (hasKey . "_webhookPair") -}}
{{- $existing := lookup "v1" "Secret" .Release.Namespace (include "featherbit-operator.webhookSecretName" .) -}}
{{- $pair := dict -}}
{{- if and $existing $existing.data (hasKey $existing.data "tls.crt") (hasKey $existing.data "tls.key") (hasKey $existing.data "ca.crt") -}}
  {{- $_ := set $pair "crt" (index $existing.data "tls.crt") -}}
  {{- $_ := set $pair "key" (index $existing.data "tls.key") -}}
  {{- $_ := set $pair "ca" (index $existing.data "ca.crt") -}}
{{- else -}}
  {{- $svc := include "featherbit-operator.webhookServiceName" . -}}
  {{- $altNames := list (printf "%s.%s.svc" $svc .Release.Namespace) (printf "%s.%s.svc.cluster.local" $svc .Release.Namespace) -}}
  {{- $ca := genCA "featherbit-operator-ca" 3650 -}}
  {{- $cert := genSignedCert $svc nil $altNames 3650 $ca -}}
  {{- $_ := set $pair "crt" ($cert.Cert | b64enc) -}}
  {{- $_ := set $pair "key" ($cert.Key | b64enc) -}}
  {{- $_ := set $pair "ca" ($ca.Cert | b64enc) -}}
{{- end -}}
{{- $_ := set . "_webhookPair" $pair -}}
{{- end -}}
{{- toJson ._webhookPair -}}
{{- end }}
