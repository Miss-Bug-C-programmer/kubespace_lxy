# Phase 5 cross-domain transport, result return, and execution fencing

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
