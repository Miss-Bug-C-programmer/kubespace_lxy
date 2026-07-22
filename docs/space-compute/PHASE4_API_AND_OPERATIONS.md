# Phase 4 API, state machine and operations

Phase 4 adds an opt-in mission-planning layer above the independent local
scheduler. The ownership and consistency contract is normative in
[`adr/0001-phase4-control-loop-ownership.md`](adr/0001-phase4-control-loop-ownership.md).
No Phase 4 component is used by the embedded K3s default scheduler.

## Install and remove

Build the planner and independent scheduler from the same K3s source version:

```shell
go build -buildvcs=false -o bin/space-compute-mission-planner ./cmd/space-compute-mission-planner
go build -buildvcs=false -o bin/space-compute-scheduler ./cmd/space-compute-scheduler
docker build -f Dockerfile.space-compute-mission-planner -t space-compute-mission-planner:v1.33.7-k3s1 .
```

Review reporter identities, image references, TLS and retention settings before
installing. CRDs must be Established before the admission and controller
manifests are applied.

```shell
kubectl apply -f docs/space-compute/manifests/phase4-crds.yaml
kubectl wait --for=condition=Established --timeout=60s \
  crd/spacelinksnapshots.spacecompute.k3s.io \
  crd/spacedomainresourcesummaries.spacecompute.k3s.io \
  crd/spacemissions.spacecompute.k3s.io \
  crd/spaceplacementintents.spacecompute.k3s.io
kubectl apply -f docs/space-compute/manifests/phase4-admission.yaml
kubectl apply -f docs/space-compute/manifests/mission-planner.yaml
kubectl apply -f docs/gpu-scheduler/manifests/space-compute-scheduler.yaml
```

Remove the planner before CRDs. Removing it leaves ordinary Pods unaffected and
leaves explicitly selected space Pods Pending. Preserve mission/placement
objects in an audit sink before deleting CRDs.

## Versioned APIs

All objects use `spacecompute.k3s.io/v1alpha1`. Unknown fields are pruned by the
structural CRD schema and production Go validation rejects invalid identity,
time, bounds and contradictions.

| Kind | Scope | Writer and purpose |
| --- | --- | --- |
| `SpaceLinkSnapshot` | cluster | authenticated domain reporter; directed link measurements, bounded predicted contact windows and provenance |
| `SpaceDomainResourceSummary` | cluster | authenticated resource reporter; bounded exporter-derived capabilities, queue, energy, thermal and resilience state |
| `SpaceMission` | namespace | mission submitter; capability/data/deadline/retry/checkpoint/result policy and Pod template |
| `SpacePlacementIntent` | namespace | planner; durable target domain, guarded epoch, exact transfer windows, material digest, score and explanations |

Fixed units are part of the API contract: data sizes are bytes, bandwidth is
bits/second, RTT is microseconds, loss/error are parts per million, API
durations are seconds, and confidence/stability/headroom/resilience are
integers in `[0,1000]`. Contact windows are half-open `[start,end)`. Timestamps
are RFC3339 UTC instants.

Link snapshots require different source/destination identities, positive
monotonic provenance sequence, SHA-256 digest, non-overlapping possible
windows, `observedAt < validUntil`, bounded clock skew, update frequency and a
history limit from 1 through 64. The controller retains acceptance/rejection,
window digest and provenance hash—not unbounded raw telemetry. Missing or stale
predictions are never converted into future availability.

Mission admission requires at least one required capability or complete
alternative set, compatible duration bounds, a future feasible deadline,
locations for non-empty inputs, destinations for required result return,
checkpointability for migration, exactly one allowed concurrent execution, and
a Pod template with no preselected Node. Resource quantities remain Kubernetes
Pod requests; this API does not allocate virtual devices.

## Planning and local scheduling

For each domain, the planner evaluates capability/software compatibility,
validated snapshot freshness, transfer fit, predicted bounded compute time,
return fit and guarded deadline. It selects the highest deterministic score,
then earliest completion, then lexical domain identity. The fixed planner score
is:

| Component | Weight | Normalized input |
| --- | ---: | --- |
| predicted completion | 30 | remaining deadline slack / total horizon |
| data locality | 20 | local input bytes / total input bytes |
| link risk | 20 | minimum confidence, stability and loss/error quality |
| energy/thermal | 15 | mean validated headroom, with declared degraded penalty |
| resilience | 10 | validated domain resilience |
| fragmentation | 5 | resource-summary fragmentation fit |

Every component is an integer `[0,100]`; total is `[0,100]`. Rejected domains
carry a stable constraint code plus observed/required values and a message.
Identical mission, snapshot objects and injected clock produce the same plan ID,
target, epochs, scores and explanation order.

The workload controller waits until `notBefore`, creates only
`<mission>-attempt-N`, fences it by plan ID and observed Pod UID, and converts
watched Pod status into monotonic execution observations. It owns store-and-
forward waiting, checkpoint/retry/migration and result-return tracking. Pod
success does not imply result delivery; a return agent must set
`spacecompute.k3s.io/result-returned=true` when return is required.

The scheduler consumes only immutable Pod annotations, watched Node projection
and asynchronous exporter snapshots. It enforces the planned domain, immediate
epoch, expiry, guarded execution/return windows and freshness. `strict` rejects
stale/missing projection, `degraded` retains only the exact durable planned
window with a capped penalty, and `best-effort` gives that fallback a zero link
score. It never invents a window or performs API/exporter/network I/O in
`Filter`, `PreScore`, `Score`, `Reserve` or binding callbacks.

## State and restart behavior

Planner state is persisted in `SpacePlacementIntent`, not process memory.
Material input digest and plan ID make duplicate reconciliation a no-op. An
expired or materially changed plan is replaced only after the active attempt is
fenced. A checkpointable attempt moves through `Replanning` and
`Checkpointed`; a non-checkpointable attempt fails rather than allowing a
second execution. Observation sequence and attempt number discard duplicates
and out-of-order updates. Each orbital/ground domain keeps an independent K3s
API and etcd quorum; cross-domain delivery is bounded at-least-once exchange,
not a synchronous global transaction.

## Metrics, Events and conditions

Planner metrics use the `space_compute_planner_` prefix:

- `planning_duration_seconds{result}` and `planning_active`;
- `replans_total{reason}` and `reconciliation_errors_total{stage}`;
- `deadline_slack_seconds`, `snapshot_age_seconds` and
  `link_risk_decisions_total{class}`;
- `queue_depth{queue}`, `retry_exhausted_total{queue}` and
  `api_writes_total{resource,operation,result}`.

All labels are closed bounded enums; no mission, Node, endpoint, reporter or
device identity is a label. Scheduler/exporter metrics remain documented in
[`../gpu-scheduler/README.md`](../gpu-scheduler/README.md).

Normal Events include `MissionPlanned` and `MissionAttemptDispatched`. Warning
Events include `InvalidMissionIntent`, `MissionPlanningBlocked`,
`MissionExecutionFailed` and `LinkSnapshotRejected`. `SpaceMission` reports a
`Planned` condition. Link/resource status reports `Validated`; placement status
reports `ExecutionSafe` and accepted observation conditions. Check Events and
conditions before logs because they preserve bounded rejection details.

## Security and retention review

The planner ServiceAccount can read link/resource summaries; create/update
placement intents; update CRD status; create watched attempt Pods/Events; and
project validated state to Nodes. It cannot read Secrets or bind Pods. The
independent scheduler alone has binding permission. The unbound domain-reporter
ClusterRole permits only link/resource object access. Bind it to a dedicated
ServiceAccount per trust domain; the fail-closed admission policy requires
`provenance.reporterID` to equal the authenticated principal and makes that
identity immutable.

Use TLS-authenticated API transport and exporter HTTPS in production. Treat
cross-domain payloads as untrusted: authenticate the reporter, verify digest
and source, restrict object size, and export Kubernetes audit logs to an
append-only system. Never put bearer tokens or endpoints in annotations.

Operational link history is capped at 64 entries per directed link and contact
windows at 256 per snapshot. Exporter/raw orbit products belong in the source
system; Kubernetes retains only bounded decision inputs/status. Set external
audit retention from mission policy and regulatory needs. Do not expand CRD
history to replace archival storage.

All domains must run authenticated time synchronization with a monitored error
bound. Configure `maximumClockSkewSeconds` from measured worst-case error, not
an optimistic target. Alert before error reaches the bound. A reporter time in
the future beyond the bound, a stale validity instant, or an edge contact that
cannot cover skew plus safety margin is rejected.

## Troubleshooting

| Symptom | Checks and action |
| --- | --- |
| Mission remains `Blocked` | Read `Planned` condition/Event; verify future deadline, capability/software summary, source data location, return destination and non-stale link sequence. Publish corrected validated inputs; do not edit status. |
| Plan stays `TransferPending` | Compare controller clock with `notBefore`, window guard and snapshot validity. Waiting belongs here; do not move it into scheduler scoring. |
| Pod has `target_domain_mismatch` | Verify Node domain/orbit labels and resource-controller projection. Do not manually retarget the Pod; replan. |
| `link_projection_stale` | Check reporter/controller health, `validUntil`, clock error and Node annotation generation. Strict missions wait; degraded/best-effort use only their already-durable exact plan. |
| `execution_window_too_short` | The scheduler no longer has the declared bounded duration before the planned compute end. Expire/replan; do not lower duration or safety assertions. |
| Running plan changes | Check `ExecutionSafe` for checkpoint fencing. Confirm one active Pod UID, persist checkpoint if allowed, then let the controller advance the attempt. |
| Result-return mission never completes | Confirm workload success and the return agent's explicit result annotation/observation. Never mark success solely from local Pod completion. |
| Planner restarts repeatedly | Check `/livez`, leader `/readyz`, Lease, informer/RBAC errors and bounded reconciliation-error stage. Ordinary scheduling remains independent. |

CPU-only replay is provided by `scripts/space-compute scenarios`, `integration`
and `cluster-e2e`. Golden workload, Node, link, expected explanations and the
recorded Iluvatar exporter text are under `contrib/space-compute/testdata/golden`
and `pkg/scheduler/plugins/gpustability/testdata/fixtures`.
