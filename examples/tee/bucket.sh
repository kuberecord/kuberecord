#!/usr/bin/env bash
# Make the tee example's bucket exist.
#
# kuberecord never creates a bucket, on any backend and in any configuration.
# Retention, encryption, lifecycle rules and Object Lock all belong to whoever
# owns the account, and Object Lock in particular can only be enabled when a
# bucket is created — so an operator that created buckets would be creating them
# with the one property that matters permanently switched off.
#
# The work happens inside the MinIO pod, using the `mc` that ships in its image
# and the writable $HOME examples/tee/minio.yaml gives it. That means no S3
# client on your machine, no port-forward, and no credentials leaving the
# cluster. It is also how you inspect the archive afterwards — see the README.
#
# Idempotent: `mc alias set` overwrites, `mc mb --ignore-existing` succeeds on a
# bucket that is already there. Re-run it after the MinIO pod restarts, since the
# alias lives in an emptyDir.
set -euo pipefail

NAMESPACE="${NAMESPACE:-kuberecord-tee}"
DEPLOYMENT="${DEPLOYMENT:-minio}"
BUCKET="${BUCKET:-kuberecord-tee}"
ALIAS="${ALIAS:-local}"

# The root credentials examples/tee/minio.yaml commits. Read out of the running
# Secret rather than repeated here, so this script cannot drift from the server
# it is talking to.
creds() {
	kubectl get secret minio-credentials -n "${NAMESPACE}" -o "jsonpath={.data.$1}" | base64 -d
}

ACCESS_KEY="$(creds accessKeyId)"
SECRET_KEY="$(creds secretAccessKey)"

kubectl exec -n "${NAMESPACE}" "deploy/${DEPLOYMENT}" -- \
	mc alias set "${ALIAS}" http://localhost:9000 "${ACCESS_KEY}" "${SECRET_KEY}" >/dev/null

kubectl exec -n "${NAMESPACE}" "deploy/${DEPLOYMENT}" -- \
	mc mb --ignore-existing "${ALIAS}/${BUCKET}"

echo "bucket ${BUCKET} is ready; read it with:"
echo "  kubectl exec -n ${NAMESPACE} deploy/${DEPLOYMENT} -- mc ls --recursive ${ALIAS}/${BUCKET}"
