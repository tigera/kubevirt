#!/usr/bin/env bash

# Deploy KubeVirt simulation mode to an existing KIND cluster.
#
# This script is used by Calico CI to deploy pre-built KubeVirt manifests
# (with baked-in image references) onto a KIND cluster for live-migration
# network testing.
#
# Environment variables:
#   MOCKVIRT_KUBECONFIG      — path to kubeconfig (falls back to KUBECONFIG, then ~/.kube/config)
#   MOCKVIRT_MANIFESTS_DIR   — directory containing kubevirt-operator.yaml and kubevirt-cr.yaml
#                              (defaults to <repo-root>/manifests/release)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

NAMESPACE="kubevirt"
MANIFESTS_DIR="${MOCKVIRT_MANIFESTS_DIR:-${REPO_ROOT}/manifests/release}"
OPERATOR_MANIFEST="${MANIFESTS_DIR}/kubevirt-operator.yaml"
CR_MANIFEST="${MANIFESTS_DIR}/kubevirt-cr.yaml"

# --- Resolve kubeconfig ---

KUBECONFIG="${MOCKVIRT_KUBECONFIG:-${KUBECONFIG:-${HOME}/.kube/config}}"
export KUBECONFIG

if [[ ! -f "${KUBECONFIG}" ]]; then
    echo "ERROR: kubeconfig not found at ${KUBECONFIG}" >&2
    exit 1
fi

echo "Using kubeconfig: ${KUBECONFIG}"

# --- Validate cluster reachability ---

if ! kubectl cluster-info &>/dev/null; then
    echo "ERROR: cannot reach cluster. Check kubeconfig and cluster status." >&2
    exit 1
fi

echo "Cluster is reachable."

# --- Validate manifests exist ---

if [[ ! -f "${OPERATOR_MANIFEST}" ]]; then
    echo "ERROR: operator manifest not found at ${OPERATOR_MANIFEST}" >&2
    echo "Run hack/ci-push-images.sh first, then copy _out/manifests/release/ to manifests/release/" >&2
    exit 1
fi

if [[ ! -f "${CR_MANIFEST}" ]]; then
    echo "ERROR: CR manifest not found at ${CR_MANIFEST}" >&2
    exit 1
fi

# --- Validate KIND cluster (nodes are docker containers) ---

echo "Validating KIND cluster..."
NODES=$(kubectl get nodes -o name 2>/dev/null)
if [[ -z "${NODES}" ]]; then
    echo "ERROR: no nodes found in the cluster" >&2
    exit 1
fi

for node in $(echo "${NODES}" | cut -d/ -f2); do
    if ! docker inspect "${node}" &>/dev/null; then
        echo "WARNING: node '${node}' is not a docker container — this may not be a KIND cluster"
    fi
done

# --- Increase inotify limits on KIND nodes ---

echo "Increasing inotify limits on KIND nodes..."
for node in $(kubectl get nodes -o name | cut -d/ -f2); do
    echo "  ${node}: setting inotify limits"
    docker exec "${node}" sysctl -w fs.inotify.max_user_watches=1048576 >/dev/null
    docker exec "${node}" sysctl -w fs.inotify.max_user_instances=8192 >/dev/null
done

# --- Create namespace with privileged PSA ---

echo "Creating namespace '${NAMESPACE}'..."
kubectl apply -f - <<EOF
---
apiVersion: v1
kind: Namespace
metadata:
  name: ${NAMESPACE}
  labels:
    kubevirt.io: ""
    pod-security.kubernetes.io/enforce: "privileged"
EOF

# --- Deploy operator ---

echo "Deploying KubeVirt operator..."
kubectl apply -f "${OPERATOR_MANIFEST}"

# --- Wait for CRD ---

echo "Waiting for KubeVirt CRD..."
count=0
until kubectl get crd kubevirts.kubevirt.io &>/dev/null; do
    ((count++)) && ((count == 30)) && echo "ERROR: KubeVirt CRD not found after 30s" && exit 1
    echo "  waiting for CRD... (${count}/30)"
    sleep 1
done
echo "  CRD is available."

# --- Wait for API ---

echo "Waiting for KubeVirt API..."
count=0
until kubectl api-resources --api-group=kubevirt.io 2>/dev/null | grep -q kubevirts; do
    ((count++)) && ((count == 30)) && echo "ERROR: KubeVirt API not available after 30s" && exit 1
    echo "  waiting for API... (${count}/30)"
    sleep 1
done
echo "  API is available."

# --- Deploy KubeVirt CR ---

echo "Deploying KubeVirt CR..."
kubectl apply -n "${NAMESPACE}" -f "${CR_MANIFEST}"

# --- Enable simulation mode ---

echo "Enabling simulation mode..."
kubectl patch kubevirt kubevirt -n "${NAMESPACE}" --type merge \
    -p '{"spec":{"configuration":{"developerConfiguration":{"simulationMode":true}}}}'

# --- Wait for CR to exist ---

echo "Waiting for KubeVirt CR to be registered..."
count=0
until kubectl -n "${NAMESPACE}" get kv kubevirt &>/dev/null; do
    ((count++)) && ((count == 30)) && echo "ERROR: KubeVirt CR not found after 30s" && exit 1
    echo "  waiting for CR... (${count}/30)"
    sleep 1
done
echo "  CR is registered."

# --- Wait for Available condition ---

echo "Waiting for KubeVirt to become Available (up to 10m)..."
count=0
until kubectl wait -n "${NAMESPACE}" kv kubevirt --for condition=Available --timeout 10m; do
    ((count++)) && ((count == 3)) && echo "ERROR: KubeVirt not ready in time" && exit 1
    echo "  Retrying wait for Available condition... (${count}/3)"
    sleep 30
done

# --- Restart Calico Typha to pick up KubeVirt CRDs ---
#
# Typha watches KubeVirt VirtualMachineInstanceMigration (VMIM) resources to
# feed Felix's LiveMigration calculator. If Calico was deployed before KubeVirt,
# Typha's initial CRD discovery misses the VMIM API and only retries every 30
# minutes. Restarting Typha (and calico-node, which reconnects to Typha) forces
# immediate detection of the newly available VMIM API.

CALICO_NS="calico-system"
if kubectl get namespace "${CALICO_NS}" &>/dev/null; then
    echo "Restarting Calico Typha to detect KubeVirt VMIM CRD..."
    if kubectl -n "${CALICO_NS}" get deployment calico-typha &>/dev/null; then
        kubectl -n "${CALICO_NS}" rollout restart deployment calico-typha
        kubectl -n "${CALICO_NS}" rollout status deployment calico-typha --timeout=2m
        echo "  Typha restarted."
    fi

else
    echo "Calico namespace '${CALICO_NS}' not found — skipping Typha restart."
fi

# --- Summary ---

echo ""
echo "=== KubeVirt simulation mode deployed successfully ==="
echo ""
echo "Namespace: ${NAMESPACE}"
echo "Simulation mode: enabled"
echo ""
echo "Component pods:"
kubectl get pods -n "${NAMESPACE}" -o wide
echo ""
echo "KubeVirt status:"
kubectl get kubevirt -n "${NAMESPACE}"
