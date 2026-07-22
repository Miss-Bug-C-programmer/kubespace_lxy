# Phase 5 operations, SLOs and runbook additions

## Multi-node Iluvatar deployment

Run one authenticated TianShu ZhiXin/Iluvatar exporter per accelerator Node.
Advertise `iluvatar.com/gpu` through the vendor device plugin and provide only
validated per-Node exporter port/path/profile metadata when defaults differ.
The collector derives each target from watched Node addresses; never share one
endpoint between Node identities or hard-code IPs.

Ordinary agents and a schedulable K3s server/master are ignored by background
collection unless they advertise a mapped positive accelerator resource or
explicit exporter metadata. A GPU-equipped master is handled like any other
Node; upstream taints, NodeResourcesFit and the device plugin still decide
whether it is schedulable. Size `cacheMaxEntries` above accelerator Node count,
`queueSize` for update bursts, workers for scrape latency and
`maxDevicesPerNode` above actual cards but no higher than necessary (default
256, maximum 4096).

Alert on collector failure/circuit/cache eviction/stale snapshot metrics and on
planner queue depth, retries exhausted, reconciliation errors, snapshot age,
negative deadline slack and link-risk classes. Labels are bounded enums; do not
add Node, mission, endpoint, device or reporter IDs as metric labels.

## Availability and degraded response

- Exporter loss: strict workloads remain Pending; degraded uses trusted static
  compatibility with capped score; best-effort never fabricates telemetry.
- Planner loss: ordinary K3s and already bound Pods continue. New mission plans
  wait; leader election selects the other replica.
- Independent scheduler loss: only explicitly selected space Pods wait.
- Link prediction loss/staleness: strict blocks; other modes may use only an
  already durable exact window according to policy, never invent a window.
- Controller retry exhaustion: inspect Event/condition and
  `retry_exhausted_total`; correct the object/input before requeueing.

## SLO starting points

Use the regression budgets in `PHASE5_PERFORMANCE.md`. Operational alert
starting points are: snapshot age > configured TTL, any retry exhaustion, error
rate >1% for five minutes, queue depth continuously increasing for ten minutes,
and p99 scheduler/planner latency exceeding the deployment-specific budget.
These are not release SLOs until repeated load/soak evidence exists.

## Backup, disaster recovery and cleanup

Back up each domain's own K3s datastore; never stretch etcd over intermittent
links. Export missions, placements, accepted link/resource status, Events and
audit logs to an append-only domain-local store. Raw exporter/orbit history
stays in its source system. On restore, keep missions suspended, restore API
objects, re-establish authenticated time/reporters, wait for fresh monotonic
summaries, then resume reconciliation.

For uninstall, stop planner/scheduler, archive audit objects, delete admission
bindings/policies and controller RBAC/workloads, then delete CRDs. Verify the
API group disappears and ordinary Pods still schedule. CRD deletion can cause
transient upstream garbage-collector reflector errors; use a maintenance window
and verify stabilization/restart.

## Clock and retention assumptions

All domains use authenticated time synchronization with monitored measured
error. Configure maximum skew and safety margin from worst-case error. Contact
history is capped at 64, windows at 256, devices at the configured bound,
controller retries at 15 and exporter response/sample/cache/queue sizes at
configuration bounds. External regulatory/audit retention is independent of
these operational Kubernetes objects.

See `PHASE4_API_AND_OPERATIONS.md` for state-machine troubleshooting and
`PHASE5_SECURITY.md`/`PHASE5_RISK_REGISTER.md` for release blockers.
