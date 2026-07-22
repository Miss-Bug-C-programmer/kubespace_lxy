# ADR 0001: Phase 4 control-loop ownership and consistency boundary

Status: accepted  
Date: 2026-07-21

## Context

Phase 4 adds contact windows, deadlines and cross-domain mission placement to a
K3s-based system whose local scheduler is deliberately isolated from remote
telemetry.  Orbital links are intermittent, clocks are imperfect, and each
orbit domain must retain an independent Kubernetes API and etcd quorum.  A
scheduler framework callback cannot wait for a future window, coordinate a
transfer, or own a retry without blocking ordinary Pod scheduling and making a
local scheduling cycle depend on remote availability.

Phase 2 established asynchronous exporter collection and immutable local
snapshots.  Phase 3 established an independent `space-compute-scheduler`, kept
Kubernetes extended resources/device plugins as the physical allocation owner,
and selected fixed SLO normalization.  This decision extends those boundaries;
it does not replace them.

## Decision

The system uses four independently restartable control loops.

1. The **resource controller** validates `SpaceLinkSnapshot` and
   `SpaceDomainResourceSummary` objects, enforces source identity, observation
   ordering, update-rate and retention bounds, and projects only the current
   validated domain/link facts needed by a local cluster.  Raw high-frequency
   telemetry and unbounded history are never copied through the API.
2. The **mission planner** reads validated domain summaries and retained link
   snapshots, chooses a target domain and execution epoch, and creates or
   replaces a durable `SpacePlacementIntent`.  Its decision is deterministic
   for a fixed mission and snapshot set.  It never creates a Pod directly and
   never assumes an unavailable future link.  A material input digest makes
   reconciliation idempotent and makes stale plans explicitly expire/replan.
3. The **workload controller** owns store-and-forward transfer state,
   disconnected waiting, Pod creation, one-active-execution enforcement,
   checkpoints, retry/migration budgets, and result-return tracking.  It uses
   deterministic attempt names and observed UIDs so restart, duplicate and
   out-of-order observations cannot create undeclared concurrent execution.
4. The independent **local scheduler** owns only immediate Pod-to-Node
   feasibility and ranking.  It consumes the Pod's durable placement projection,
   watched Node data, exporter snapshots and validated local link projection.
   `Filter`, `PreScore`, `Score`, `Reserve` and binding paths perform no remote
   I/O and no future-window waiting.  Kubernetes extended resources, DRA or
   vendor device plugins continue to own physical allocation.

Each domain has its own Kubernetes API/etcd quorum.  Cross-domain replication is
at-least-once delivery of bounded summaries, snapshots, placement intents and
execution observations.  Resource versions are local concurrency tokens, not a
global transaction.  Object identity plus generation, attempt and observation
sequence provide idempotency across disconnected domains.

## Durable state machine

```text
Mission: Accepted -> Planning -> Planned -> Executing -> Returning -> Succeeded
                    |     ^         |          |            |
                    v     |         v          v            v
                  Blocked/Replanning       Checkpointed    Failed

PlacementIntent: Pending -> TransferPending -> Ready -> Dispatched -> Running
                         -> Checkpointed -> Replanning
                         -> ReturnPending -> Completed
                         -> Expired/Failed
```

Only the planner moves `Pending/Planning/Planned/Replanning` and changes the
target domain or epoch.  Only the workload controller moves transfer,
dispatch/execution, checkpoint, return and terminal states.  The resource
controller changes validation conditions and retained history, not mission
execution.  The scheduler binds one already-dispatched local Pod and never
advances mission state.

All reconcilers compare generation and material-input digest before writing.
Terminal observations are monotonic.  A lower attempt, older sequence or
already-observed Pod UID is ignored and recorded.  Replanning first fences the
old attempt; a non-checkpointable running attempt is failed rather than silently
duplicated.  `maxConcurrentExecutions` defaults to and is currently limited to
one.

## Time, prediction and expiry

Durations use nanoseconds in Go and seconds in API fields; bandwidth uses bits
per second, sizes use bytes, RTT uses microseconds, and loss/error use parts per
million.  All decision logic receives a clock.  Production uses the real clock;
tests inject a fake clock through the same interfaces without a success bypass.

Window feasibility subtracts configured safety margin, duration uncertainty and
maximum clock skew from usable time.  Missing prediction is unknown, never an
open link.  Every plan has `notBefore`, `expiresAt`, source observation sequence
and a material-input digest.  It expires and is replanned when its window,
snapshot validity or deadline passes, or when a material resource/link/mission
input changes.

## Failure and availability consequences

- Planner or resource-controller outage stops new plans/projections; ordinary
  K3s scheduling and already-running local Pods continue.
- Link partition leaves durable placement/execution state locally.  The
  workload controller waits or checkpoints within declared policy and reports
  observations when connectivity returns.
- Local scheduler outage affects only Pods selecting
  `space-compute-scheduler`.  Default-scheduler Pods remain independent.
- No component stretches one etcd quorum across a satellite link or relies on a
  synchronous cross-domain commit.

## Security and retention consequences

Cross-domain reporters may create/update only resource summaries and link
snapshots for their authenticated identity.  They cannot create Pods, change
placement intent, read Secrets, or bind workloads.  Planner RBAC is read-only
for summaries/links and write/status for missions and placement intents.
Workload-controller RBAC is write/status for placement intents and explicitly
scoped Pod/Event access.  Scheduler RBAC does not gain cross-domain write access.

The controller retains at most the configured number of validated observation
digests per directed link and rejects updates faster than the configured
minimum interval unless the current window materially changes.  Operators send
long-term audit records to an external append-only sink; Kubernetes objects are
bounded operational state, not an unlimited telemetry archive.

## Alternatives rejected

- Waiting or retrying in `Score`: blocks the scheduler hot path and couples local
  placement to remote availability.
- One Kubernetes/etcd cluster spanning all domains: quorum cannot be made
  reliable over intermittent orbital links.
- A second allocator in the planner: conflicts with Kubernetes extended
  resources/DRA and vendor device plugins.
- Treating a missing prediction as best-case connectivity: fabricates future
  capacity and makes deadline guarantees false.

