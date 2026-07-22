# Phase 5 chaos and resilience status

The CPU suite passed production-path deterministic fault tests for exporter
timeout/cancellation/loss, malformed and oversized responses, redirect/TLS
handling, queue saturation, bounded backoff/circuit state, endpoint generation
change during an in-flight request, Node delete/recreate/address/profile change,
stale/missing telemetry modes, clock skew, link partition, checkpointable and
non-checkpointable outcomes, duplicate/out-of-order observations, retry fencing,
controller restart, scheduler process failover, K3s restart and uninstall.
Race tests passed and 5,000-node lifecycle tests did not show goroutine growth
with node count.

Retries are rate limited and forgotten after 15 attempts; target/cache/queue,
history, response/sample/label/device and blocked-Pod stores are bounded. Metric
labels are closed enums. The resource controller coalesces global input changes
to one queue key instead of enqueuing every mission from the informer callback.

The following required campaigns were **not run** and remain release blockers
or risks:

- API-server throttling under sustained writes and controller leader kill under
  measured load;
- real network delay/loss/long partition between independent clusters and
  partial authenticated cross-domain synchronization;
- multi-hour exporter/Node churn soak with heap/goroutine/cardinality profiles;
- full-agent scheduler/kubelet/planner restart during real device execution;
- remote old-attempt fencing across domains (the local controller fence is
  tested, but no WAN execution agent exists).

Therefore deterministic chaos semantics pass for implemented local control
loops, but production chaos qualification is incomplete and cannot support a
release decision.
