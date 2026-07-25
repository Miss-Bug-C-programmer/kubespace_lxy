#!/usr/bin/env python3
from pathlib import Path


def read(path):
    return Path(path).read_text()


def write(path, text):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


def replace_once(path, old, new):
    text = read(path)
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: marker count {count} for {old[:100]!r}")
    write(path, text.replace(old, new, 1))


# Build/test entry points include the independent domain-agent component.
replace_once(
    "Makefile",
    '''.PHONY: space-compute-reporter-webhook
space-compute-reporter-webhook:
\tmkdir -p bin
\tgo build -buildvcs=false -o bin/space-compute-reporter-webhook ./cmd/space-compute-reporter-webhook
''',
    '''.PHONY: space-compute-reporter-webhook
space-compute-reporter-webhook:
\tmkdir -p bin
\tgo build -buildvcs=false -o bin/space-compute-reporter-webhook ./cmd/space-compute-reporter-webhook

.PHONY: space-compute-domain-agent
space-compute-domain-agent:
\tmkdir -p bin
\tgo build -buildvcs=false -o bin/space-compute-domain-agent ./cmd/space-compute-domain-agent
''',
)

space_script = "scripts/space-compute"
replace_once(
    space_script,
    'go test ./pkg/scheduler/plugins/gpustability ./cmd/space-compute-scheduler ./cmd/space-compute-mission-planner ./cmd/space-compute-reporter-webhook ./contrib/space-compute/pkg/... ./tests/integration/spacecomputescheduler -count=1',
    'go test ./pkg/scheduler/plugins/gpustability ./cmd/space-compute-scheduler ./cmd/space-compute-mission-planner ./cmd/space-compute-reporter-webhook ./cmd/space-compute-domain-agent ./contrib/space-compute/pkg/... ./tests/integration/spacecomputescheduler -count=1',
)
replace_once(
    space_script,
    'go test -race ./pkg/scheduler/plugins/gpustability ./cmd/space-compute-scheduler ./cmd/space-compute-mission-planner ./cmd/space-compute-reporter-webhook ./contrib/space-compute/pkg/... ./tests/integration/spacecomputescheduler -count=1',
    'go test -race ./pkg/scheduler/plugins/gpustability ./cmd/space-compute-scheduler ./cmd/space-compute-mission-planner ./cmd/space-compute-reporter-webhook ./cmd/space-compute-domain-agent ./contrib/space-compute/pkg/... ./tests/integration/spacecomputescheduler -count=1',
)
replace_once(
    space_script,
    'cmd/space-compute-reporter-webhook/*.go contrib/space-compute/pkg/*/*.go',
    'cmd/space-compute-reporter-webhook/*.go cmd/space-compute-domain-agent/*.go contrib/space-compute/pkg/*/*.go',
)
replace_once(
    space_script,
    'go vet ./pkg/scheduler/plugins/gpustability ./cmd/space-compute-scheduler ./cmd/space-compute-mission-planner ./cmd/space-compute-reporter-webhook ./contrib/space-compute/pkg/... ./tests/integration/spacecomputescheduler',
    'go vet ./pkg/scheduler/plugins/gpustability ./cmd/space-compute-scheduler ./cmd/space-compute-mission-planner ./cmd/space-compute-reporter-webhook ./cmd/space-compute-domain-agent ./contrib/space-compute/pkg/... ./tests/integration/spacecomputescheduler',
)
replace_once(
    space_script,
    '\tgo build -buildvcs=false -o "${SPACE_COMPUTE_ROOT}/bin/space-compute-reporter-webhook" ./cmd/space-compute-reporter-webhook\n',
    '\tgo build -buildvcs=false -o "${SPACE_COMPUTE_ROOT}/bin/space-compute-reporter-webhook" ./cmd/space-compute-reporter-webhook\n\tgo build -buildvcs=false -o "${SPACE_COMPUTE_ROOT}/bin/space-compute-domain-agent" ./cmd/space-compute-domain-agent\n',
)
replace_once(
    space_script,
    'go test ./cmd/space-compute-mission-planner ./cmd/space-compute-reporter-webhook ./contrib/space-compute/pkg/... -count=1',
    'go test ./cmd/space-compute-mission-planner ./cmd/space-compute-reporter-webhook ./cmd/space-compute-domain-agent ./contrib/space-compute/pkg/... -count=1',
)

# Mission planner consumes Phase 5 evidence and creates transfer desired state.
planner_manifest = "docs/space-compute/manifests/mission-planner.yaml"
replace_once(
    planner_manifest,
    'resources: [spacemissions, spaceplacementintents, spacelinksnapshots, spacedomainresourcesummaries]',
    'resources: [spacemissions, spaceplacementintents, spacelinksnapshots, spacedomainresourcesummaries, spacetransferintents, spacetransferreceipts, spaceexecutionleases, spaceexecutionobservations, spaceresultreceipts]',
)
replace_once(
    planner_manifest,
    'resources: [spaceplacementintents]\n  verbs: [create, update, patch]',
    'resources: [spaceplacementintents, spacetransferintents]\n  verbs: [create, update, patch]',
)
replace_once(
    planner_manifest,
    'resources: [spacelinksnapshots, spacedomainresourcesummaries, spacetransferreceipts, spaceresultreceipts]',
    'resources: [spacelinksnapshots, spacedomainresourcesummaries, spacetransferreceipts, spaceexecutionleases, spaceexecutionobservations, spaceresultreceipts]',
)
replace_once(
    planner_manifest,
    'args: [--leader-elect=true, --leader-election-namespace=kube-system, --workers=4, --metrics-bind-address=:10261]\n        ports:',
    '''args: [--leader-elect=true, --leader-election-namespace=kube-system, --workers=4, --metrics-bind-address=:10261]
        env:
        - name: SPACE_COMPUTE_LOCAL_DOMAIN_JSON
          valueFrom:
            configMapKeyRef:
              name: space-compute-domain-identity
              key: domain.json
              optional: true
        ports:''',
)

# The fail-closed reporter webhook must authenticate all signed Phase 5 evidence.
webhook_manifest = "docs/space-compute/manifests/reporter-admission-webhook.yaml"
replace_once(
    webhook_manifest,
    '''    - spacetransferreceipts
    - spaceresultreceipts
''',
    '''    - spacetransferreceipts
    - spaceexecutionleases
    - spaceexecutionobservations
    - spaceresultreceipts
''',
)

crd_manifest = r'''# Phase 5 CRDs that were not present in the Phase 4 receipt API.
# Reporter-backed resources are additionally validated by the fail-closed
# reporter admission webhook; CRD schema validation is not the trust boundary.
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: spacetransferintents.spacecompute.k3s.io
spec:
  group: spacecompute.k3s.io
  scope: Cluster
  names:
    plural: spacetransferintents
    singular: spacetransferintent
    kind: SpaceTransferIntent
    shortNames: [stransfer]
  versions:
  - name: v1alpha1
    served: true
    storage: true
    subresources:
      status: {}
    schema:
      openAPIV3Schema:
        type: object
        required: [spec]
        properties:
          spec:
            type: object
            required: [transferID, missionUID, planID, attempt, purpose, coordinator, source, destination, dataID, bytes, payloadDigest, window, expiresAt]
            properties:
              transferID: {type: string, maxLength: 63}
              missionUID: {type: string, maxLength: 128}
              planID: {type: string, maxLength: 63}
              attempt: {type: integer, format: int32, minimum: 1, maximum: 100}
              purpose: {type: string, enum: [Input, Result]}
              coordinator: &domain
                type: object
                required: [name, clusterID, orbitClass]
                properties:
                  name: {type: string, minLength: 1, maxLength: 63}
                  clusterID: {type: string, minLength: 1, maxLength: 253}
                  orbitClass: {type: string, minLength: 1, maxLength: 32}
              source: *domain
              destination: *domain
              dataID: {type: string, minLength: 1, maxLength: 253}
              bytes: {type: integer, format: int64, minimum: 0}
              payloadDigest: {type: string, pattern: '^[0-9a-f]{64}$'}
              leaseEpoch: {type: integer, format: int64, minimum: 0}
              tokenHash: {type: string, pattern: '^(|[0-9a-f]{64})$'}
              window:
                type: object
                required: [source, destination, start, end, bytes]
                properties:
                  linkSnapshotName: {type: string}
                  windowID: {type: string}
                  dataID: {type: string}
                  source: *domain
                  destination: *domain
                  start: {type: string, format: date-time}
                  end: {type: string, format: date-time}
                  bytes: {type: integer, format: int64, minimum: 0}
              expiresAt: {type: string, format: date-time}
          status:
            type: object
            properties:
              observedGeneration: {type: integer, format: int64}
              phase: {type: string}
              receiptName: {type: string}
              conditions:
                type: array
                items: {type: object, x-kubernetes-preserve-unknown-fields: true}
---
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: spaceexecutionleases.spacecompute.k3s.io
spec:
  group: spacecompute.k3s.io
  scope: Cluster
  names:
    plural: spaceexecutionleases
    singular: spaceexecutionlease
    kind: SpaceExecutionLease
    shortNames: [sexeclease]
  versions:
  - name: v1alpha1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
        required: [spec]
        properties:
          spec:
            type: object
            required: [source, destination, fence, heartbeatAt, maximumClockSkewSeconds, provenance]
            properties:
              source: *domain
              destination: *domain
              fence:
                type: object
                required: [missionUID, planID, attempt, leaseEpoch, tokenHash, expiresAt]
                properties:
                  missionUID: {type: string, minLength: 1, maxLength: 128}
                  planID: {type: string, minLength: 1, maxLength: 63}
                  attempt: {type: integer, format: int32, minimum: 1, maximum: 100}
                  leaseEpoch: {type: integer, format: int64, minimum: 1}
                  tokenHash: {type: string, pattern: '^[0-9a-f]{64}$'}
                  expiresAt: {type: string, format: date-time}
              heartbeatAt: {type: string, format: date-time}
              maximumClockSkewSeconds: {type: integer, format: int64, minimum: 0, maximum: 300}
              provenance: &provenance
                type: object
                required: [reporterID, source, digest, sequence]
                properties:
                  reporterID: {type: string, minLength: 1, maxLength: 253}
                  source: {type: string, minLength: 1, maxLength: 253}
                  digest: {type: string, pattern: '^[0-9a-f]{64}$'}
                  signature: {type: string}
                  sequence: {type: integer, format: int64, minimum: 1}
                  previousDigest: {type: string, pattern: '^(|[0-9a-f]{64})$'}
---
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: spaceexecutionobservations.spacecompute.k3s.io
spec:
  group: spacecompute.k3s.io
  scope: Cluster
  names:
    plural: spaceexecutionobservations
    singular: spaceexecutionobservation
    kind: SpaceExecutionObservation
    shortNames: [sexecobs]
  versions:
  - name: v1alpha1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
        required: [spec]
        properties:
          spec:
            type: object
            required: [observationID, missionUID, planID, attempt, leaseEpoch, tokenHash, source, destination, phase, observedAt, provenance]
            properties:
              observationID: {type: string, minLength: 1, maxLength: 63}
              missionUID: {type: string, minLength: 1, maxLength: 128}
              planID: {type: string, minLength: 1, maxLength: 63}
              attempt: {type: integer, format: int32, minimum: 1, maximum: 100}
              leaseEpoch: {type: integer, format: int64, minimum: 1}
              tokenHash: {type: string, pattern: '^[0-9a-f]{64}$'}
              source: *domain
              destination: *domain
              phase: {type: string, enum: [Heartbeat, Stopped, Checkpointed, Completed, Failed]}
              checkpointID: {type: string, maxLength: 253}
              observedAt: {type: string, format: date-time}
              provenance: *provenance
'''
write("docs/space-compute/manifests/phase5-transport-execution-crds.yaml", crd_manifest)

agent_manifest = r'''# Per-domain transport/execution agent. Administrators must provision:
# - Secret space-compute-domain-agent-config with domain-agent.json
# - Secret space-compute-domain-agent-identity with tls.crt/tls.key/ca.crt,
#   signing.key and peer public keys referenced by domain-agent.json.
# The serving certificate must contain the local domain SPIFFE URI SAN used by
# the peer registry. Persistent state is mandatory for bounded at-least-once
# retry and lease-confirmation recovery across process restarts.
apiVersion: v1
kind: ServiceAccount
metadata:
  name: space-compute-domain-agent
  namespace: kube-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: system:space-compute-domain-agent
rules:
- apiGroups: [spacecompute.k3s.io]
  resources: [spacemissions, spaceplacementintents, spacetransferintents, spacetransferreceipts, spaceexecutionleases, spaceexecutionobservations, spaceresultreceipts]
  verbs: [get, list, watch]
- apiGroups: [spacecompute.k3s.io]
  resources: [spacetransferintents, spacetransferreceipts, spaceexecutionleases, spaceexecutionobservations, spaceresultreceipts]
  verbs: [create, update, patch]
- apiGroups: [""]
  resources: [pods]
  verbs: [get, create, delete]
- apiGroups: [""]
  resources: [secrets]
  verbs: [get, create]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: space-compute-domain-agent
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: system:space-compute-domain-agent
subjects:
- kind: ServiceAccount
  name: space-compute-domain-agent
  namespace: kube-system
---
apiVersion: v1
kind: Service
metadata:
  name: space-compute-domain-agent
  namespace: kube-system
spec:
  clusterIP: None
  selector:
    app.kubernetes.io/name: space-compute-domain-agent
  ports:
  - {name: envelope, port: 10443, targetPort: envelope}
  - {name: report, port: 10445, targetPort: report}
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: space-compute-domain-agent
  namespace: kube-system
spec:
  replicas: 1
  serviceName: space-compute-domain-agent
  selector:
    matchLabels: {app.kubernetes.io/name: space-compute-domain-agent}
  template:
    metadata:
      labels: {app.kubernetes.io/name: space-compute-domain-agent}
    spec:
      serviceAccountName: space-compute-domain-agent
      priorityClassName: system-cluster-critical
      terminationGracePeriodSeconds: 30
      securityContext:
        seccompProfile: {type: RuntimeDefault}
      containers:
      - name: domain-agent
        image: space-compute-domain-agent:v1.33.7-k3s1
        imagePullPolicy: IfNotPresent
        args: [--config=/etc/space-compute/domain-agent.json]
        ports:
        - {name: envelope, containerPort: 10443}
        - {name: report, containerPort: 10445}
        - {name: health, containerPort: 10446}
        readinessProbe: {httpGet: {path: /readyz, port: health}, initialDelaySeconds: 3}
        livenessProbe: {httpGet: {path: /livez, port: health}, initialDelaySeconds: 10}
        resources:
          requests: {cpu: 100m, memory: 128Mi}
          limits: {cpu: "2", memory: 1Gi}
        securityContext:
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true
          runAsNonRoot: true
          runAsUser: 65532
          runAsGroup: 65532
          capabilities: {drop: [ALL]}
        volumeMounts:
        - {name: config, mountPath: /etc/space-compute, readOnly: true}
        - {name: identity, mountPath: /identity, readOnly: true}
        - {name: state, mountPath: /var/lib/space-compute}
      volumes:
      - name: config
        secret: {secretName: space-compute-domain-agent-config}
      - name: identity
        secret: {secretName: space-compute-domain-agent-identity}
  volumeClaimTemplates:
  - metadata: {name: state}
    spec:
      accessModes: [ReadWriteOnce]
      resources:
        requests: {storage: 10Gi}
'''
write("docs/space-compute/manifests/domain-agent.yaml", agent_manifest)

ops_doc = r'''# Phase 5 cross-domain transport, result return, and execution fencing

This phase keeps every WAN operation outside scheduler framework callbacks. `space-compute-scheduler` remains an independent local scheduler; the mission planner/workload controller consume Kubernetes evidence, and `space-compute-domain-agent` owns cross-domain I/O and fencing.

## Trust and transport contract

Peer traffic uses TLS 1.3 mutual certificate authentication. The envelope source must equal the peer certificate SPIFFE URI SAN for the configured trust domain, and the source domain must have a configured Ed25519 public key. Reporter objects are additionally admitted through `space-compute-reporter-webhook`; reporter identity must match a `SpaceDomainReporterBinding` or an explicitly allowed gateway.

Every envelope is signed over version, ID, kind, source, destination, mission UID, plan ID, attempt, sequence, timestamp, expiry, and payload digest. Receivers verify the signature and payload digest before durable handling. `(envelope ID, sequence)` is persisted for idempotent duplicate suppression.

Delivery is bounded at-least-once. Message size, queue items/bytes, concurrency, retries and retention are hard-bounded. Failed delivery uses exponential backoff with jitter and a per-destination circuit breaker. The outbox and dedupe state are disk-backed, so a process restart resumes outstanding retries rather than converting a disconnect into success.

No scheduler/planner callback performs a synchronous cross-domain transaction.

## Transfer and dispatch ordering

`SpacePlacementIntent.spec.notBefore` is the earliest dispatch time, not transfer start. The planner records transfer windows separately.

For each input, the workload controller creates a `SpaceTransferIntent`. The coordinator routes that desired state to source and destination agents. A receiver rejects transfer bytes unless an exact, unexpired intent is already durable locally. Transfer completion is represented only by a signed `SpaceTransferReceipt`; wall-clock arrival at `NotBefore`, `ComputeStart`, or transfer-window end never implies success.

A compute Pod can be created only when all of the following are true:

1. every planned input has a matching trusted transfer receipt;
2. current time is at or after both `ComputeStart` and `NotBefore`;
3. the placement has not expired;
4. a trusted current execution lease from the target domain exists;
5. for replacement attempts, the prior attempt satisfies the fencing rules below.

Without the transfer coordinator/agent, receipts, or lease, placement remains `TransferPending` or `ExecutionLeasePending` and no Pod is created. For a remote target the coordinator does not create a local shadow Pod; execution is created only by the target domain agent.

## Execution fence and partition behavior

Every attempt is identified by `(MissionUID, PlanID, Attempt, LeaseEpoch, TokenHash, ExpiresAt)`. The plaintext random token is kept in a Kubernetes Secret and is never stored in the lease CRD. A target accepts only a current monotonically increasing lease epoch; a higher epoch requires a new attempt and non-reused token. Heartbeat, checkpoint, result and completion reports carrying an older epoch/token are rejected.

A replacement attempt is not requested until the coordinator has trusted prior-attempt evidence. For non-checkpointable work, lease expiry alone is insufficient: a signed remote `Stopped` observation is required, so a partition cannot create a duplicate execution. Checkpointable migration requires a signed `Checkpointed` observation first and then either a signed stop or lease expiry beyond the declared skew.

Remote lease renewal is two-phase. The execution domain proposes a same-epoch signed heartbeat/expiry extension to the coordinator, but does not treat the extension as confirmed until a durable `LeaseAck` for that exact provenance sequence/digest returns. An unconfirmed or near-expiry remote execution is actively fenced locally before authority can overlap; only after the local Pod is actually gone does the agent sign `Stopped`. Deleting a Pod in a different local controller is never treated as remote fencing proof.

Lease clock skew is configured separately from transport timestamp skew and is bounded to less than one quarter of the lease TTL. The default lease TTL is 120 seconds and default lease skew is 2 seconds.

## Result return

Execution completion is reported to the domain agent with the current fence token. If result return is required, the agent follows the placement's `ResultTransfer`, persists a result `SpaceTransferIntent`, and returns the bytes through the same bounded transport. Only the independent agent writes the signed `SpaceResultReceipt`. A matching trusted result receipt moves `ReturnPending` to `Completed`.

`spacecompute.k3s.io/result-returned` and `spacecompute.k3s.io/checkpoint-id` Pod annotations are untrusted hints only. They never directly drive `Completed` or `Checkpointed`.

## Operator prerequisites

Each domain must provision `space-compute-domain-identity` for the mission planner and the two domain-agent Secrets documented in `manifests/domain-agent.yaml`. Certificates must chain to the configured CA and contain the exact local SPIFFE URI SAN. Reporter bindings must allow `SpaceTransferReceipt`, `SpaceExecutionLease`, `SpaceExecutionObservation`, and `SpaceResultReceipt` for the relevant peers/gateway principal.

The domain-agent StatefulSet uses persistent storage because retry, dedupe, remote assignment, and lease-confirmation state must survive process restart. Do not replace it with ephemeral storage in production.
'''
write("docs/space-compute/PHASE5_TRANSPORT_EXECUTION.md", ops_doc)

status_path = "docs/space-compute/IMPLEMENTATION_STATUS.md"
status = read(status_path)
marker = "## Phase 5 transport / transfer / result / execution fence"
if marker in status:
    raise SystemExit("Phase 5 final status section already exists")
status += r'''

## Phase 5 transport / transfer / result / execution fence

Implemented as independent components above the scheduler hot path:

- `cmd/space-compute-domain-agent` plus `contrib/space-compute/pkg/transport` and `pkg/execution`;
- versioned `SpaceTransferIntent`, `SpaceTransferReceipt`, `SpaceExecutionLease`, `SpaceExecutionObservation`, and `SpaceResultReceipt` APIs;
- TLS 1.3 mTLS cross-domain envelopes with SPIFFE identity binding, Ed25519 signatures, persistent bounded at-least-once retry/dedupe, jittered exponential backoff and circuit breaking;
- monotonic execution epochs, non-reusable fence tokens, durable two-phase lease renewal acknowledgements, signed stop/checkpoint/result evidence, and fail-closed partition behavior;
- transfer-receipt + compute-time + placement-expiry + execution-lease dispatch gates; remote placements do not create local shadow Pods;
- result completion only from signed result receipts; legacy workload annotations remain untrusted hints.

The repository `scripts/space-compute all` gate includes the domain-agent command and Phase 5 packages. Cluster/hardware E2E remain environment-gated separately and are not silently substituted by unit tests.
'''
write(status_path, status)

print("stage5 deployment, CRD, RBAC, build and operations patch staged")
