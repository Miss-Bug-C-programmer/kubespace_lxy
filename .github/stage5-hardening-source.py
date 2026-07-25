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
        raise SystemExit(f"{path}: marker count {count} for {old[:80]!r}")
    write(path, text.replace(old, new, 1))


def replace_between(path, start, end, new):
    text = read(path)
    i = text.find(start)
    if i < 0:
        raise SystemExit(f"{path}: start marker missing {start!r}")
    j = text.find(end, i + len(start))
    if j < 0:
        raise SystemExit(f"{path}: end marker missing {end!r}")
    write(path, text[:i] + new.rstrip() + "\n\n" + text[j:])


# ---- Versioned API and validation -------------------------------------------------
replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/phase5_transport.go",
    '\tPurpose       string          `json:"purpose"`\n\tSource        DomainReference `json:"source"`',
    '\tPurpose       string          `json:"purpose"`\n\tCoordinator   DomainReference `json:"coordinator"`\n\tSource        DomainReference `json:"source"`',
)

replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/phase5_validation.go",
    'validateReceiptCommon(intent.Spec.MissionUID, intent.Spec.PlanID, intent.Spec.Attempt, intent.Spec.Source, intent.Spec.Destination, intent.Spec.Bytes, intent.Spec.PayloadDigest, Provenance{ReporterID: "local", Source: "intent", Digest: strings.Repeat("0", 64), Sequence: 1}, &errs)\n',
    'validateReceiptCommon(intent.Spec.MissionUID, intent.Spec.PlanID, intent.Spec.Attempt, intent.Spec.Source, intent.Spec.Destination, intent.Spec.Bytes, intent.Spec.PayloadDigest, Provenance{ReporterID: "local", Source: "intent", Digest: strings.Repeat("0", 64), Sequence: 1}, true, &errs)\n\tvalidateDomain("spec.coordinator", intent.Spec.Coordinator, &errs)\n',
)
replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/phase5_validation.go",
    '\tif intent.Spec.Window.Bytes != intent.Spec.Bytes {\n\t\terrs.add("spec.window.bytes", "must equal spec.bytes")\n\t}\n',
    '\tif intent.Spec.Window.Bytes != intent.Spec.Bytes {\n\t\terrs.add("spec.window.bytes", "must equal spec.bytes")\n\t}\n\tif intent.Spec.Window.Source != intent.Spec.Source || intent.Spec.Window.Destination != intent.Spec.Destination {\n\t\terrs.add("spec.window", "source/destination must equal transfer intent")\n\t}\n\tif intent.Spec.Window.DataID != "" && intent.Spec.Window.DataID != intent.Spec.DataID {\n\t\terrs.add("spec.window.dataID", "must be empty or equal spec.dataID")\n\t}\n',
)

# Result receipts are valid even when the planned return destination is the same
# execution domain. Transfer receipts and transfer intents remain cross-domain.
replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/truth_validation.go",
    'validateReceiptCommon(receipt.Spec.MissionUID, receipt.Spec.PlanID, receipt.Spec.Attempt, receipt.Spec.Source, receipt.Spec.Destination, receipt.Spec.Bytes, receipt.Spec.PayloadDigest, receipt.Spec.Provenance, &errs)',
    'validateReceiptCommon(receipt.Spec.MissionUID, receipt.Spec.PlanID, receipt.Spec.Attempt, receipt.Spec.Source, receipt.Spec.Destination, receipt.Spec.Bytes, receipt.Spec.PayloadDigest, receipt.Spec.Provenance, true, &errs)',
)
replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/truth_validation.go",
    'validateReceiptCommon(receipt.Spec.MissionUID, receipt.Spec.PlanID, receipt.Spec.Attempt, receipt.Spec.Source, receipt.Spec.Destination, receipt.Spec.Bytes, receipt.Spec.PayloadDigest, receipt.Spec.Provenance, &errs)',
    'validateReceiptCommon(receipt.Spec.MissionUID, receipt.Spec.PlanID, receipt.Spec.Attempt, receipt.Spec.Source, receipt.Spec.Destination, receipt.Spec.Bytes, receipt.Spec.PayloadDigest, receipt.Spec.Provenance, false, &errs)',
)
replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/truth_validation.go",
    'func validateReceiptCommon(missionUID, planID string, attempt int32, source, destination DomainReference, bytes int64, payloadDigest string, provenance Provenance, errs *ValidationErrors) {',
    'func validateReceiptCommon(missionUID, planID string, attempt int32, source, destination DomainReference, bytes int64, payloadDigest string, provenance Provenance, requireDistinct bool, errs *ValidationErrors) {',
)
replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/truth_validation.go",
    '\tif source == destination {\n\t\terrs.add("spec.destination", "must differ from source")\n\t}',
    '\tif requireDistinct && source == destination {\n\t\terrs.add("spec.destination", "must differ from source")\n\t}',
)

# A signed Stopped observation is a remote-agent assertion that execution has
# actually ceased; it remains useful after lease expiry. All workload-token based
# phases remain invalid after expiry.
replace_once(
    "contrib/space-compute/pkg/execution/fence.go",
    '\tif observation.Spec.ObservedAt.After(f.ExpiresAt.Time) {\n\t\treturn fmt.Errorf("execution observation was produced after lease expiry")\n\t}',
    '\tif observation.Spec.Phase != spacev1.ExecutionObservationStopped && observation.Spec.ObservedAt.After(f.ExpiresAt.Time) {\n\t\treturn fmt.Errorf("execution observation was produced after lease expiry")\n\t}',
)

# Webhook structural updates must not allow a signed same-epoch heartbeat to
# mutate the fencing identity. Same-domain result receipts have no peer hop.
replace_once(
    "contrib/space-compute/pkg/admission/validator.go",
    '\tcase *spacev1.SpaceLinkSnapshot:\n\t\told, ok := previous.digestObject.(*spacev1.SpaceLinkSnapshot)\n\t\tif !ok {\n\t\t\treturn fmt.Errorf("previous reporter object kind does not match")\n\t\t}\n\t\treturn spacev1.ValidateLinkSnapshot(value, old, clock)\n\t}\n',
    '\tcase *spacev1.SpaceLinkSnapshot:\n\t\told, ok := previous.digestObject.(*spacev1.SpaceLinkSnapshot)\n\t\tif !ok {\n\t\t\treturn fmt.Errorf("previous reporter object kind does not match")\n\t\t}\n\t\treturn spacev1.ValidateLinkSnapshot(value, old, clock)\n\tcase *spacev1.SpaceExecutionLease:\n\t\told, ok := previous.digestObject.(*spacev1.SpaceExecutionLease)\n\t\tif !ok {\n\t\t\treturn fmt.Errorf("previous reporter object kind does not match")\n\t\t}\n\t\ta, b := old.Spec.Fence, value.Spec.Fence\n\t\tif a.MissionUID != b.MissionUID || a.PlanID != b.PlanID || a.Attempt != b.Attempt || a.LeaseEpoch != b.LeaseEpoch || a.TokenHash != b.TokenHash {\n\t\t\treturn fmt.Errorf("same-epoch lease heartbeat changed fencing identity")\n\t\t}\n\t\tif !value.Spec.HeartbeatAt.After(old.Spec.HeartbeatAt.Time) || !b.ExpiresAt.After(a.ExpiresAt.Time) {\n\t\t\treturn fmt.Errorf("lease heartbeat and expiry must strictly increase")\n\t\t}\n\t}\n',
)
replace_once(
    "contrib/space-compute/pkg/admission/validator.go",
    '\t\tdestination := value.Spec.Destination\n\t\treturn &reporterEnvelope{\n\t\t\tkind: "SpaceResultReceipt", name: value.Name, provenance: &value.Spec.Provenance,\n\t\t\tsource: value.Spec.Source, destination: &destination, digestObject: value,',
    '\t\tdestination := value.Spec.Destination\n\t\tvar peer *spacev1.DomainReference\n\t\tif destination != value.Spec.Source {\n\t\t\tpeer = &destination\n\t\t}\n\t\treturn &reporterEnvelope{\n\t\t\tkind: "SpaceResultReceipt", name: value.Name, provenance: &value.Spec.Provenance,\n\t\t\tsource: value.Spec.Source, destination: peer, digestObject: value,',
)

# ---- Durable queue helpers and wire kinds ----------------------------------------
replace_once(
    "contrib/space-compute/pkg/transport/chunks.go",
    'const TransferChunkKind = "transfer-chunk"\nconst TransferAckKind = "transfer-ack"\nconst ReporterObjectKind = "reporter-object"\nconst LeaseRequestKind = "lease-request"\nconst LeaseGrantKind = "lease-grant"',
    'const TransferIntentKind = "transfer-intent"\nconst TransferChunkKind = "transfer-chunk"\nconst TransferAckKind = "transfer-ack"\nconst ReporterObjectKind = "reporter-object"\nconst LeaseRequestKind = "lease-request"\nconst LeaseGrantKind = "lease-grant"\nconst LeaseAckKind = "lease-ack"',
)

# Queue membership is used to avoid colliding with an already-persisted retry.
replace_once(
    "contrib/space-compute/pkg/transport/spool.go",
    'func (q *DiskQueue) Due(now time.Time, limit int) ([]queuedEnvelope, error) {',
    '''func (q *DiskQueue) Contains(id string, sequence int64) (bool, error) {
\tq.mu.Lock()
\tdefer q.mu.Unlock()
\t_, err := os.Stat(q.path(id, sequence))
\tif err == nil {
\t\treturn true, nil
\t}
\tif os.IsNotExist(err) {
\t\treturn false, nil
\t}
\treturn false, err
}

func (q *DiskQueue) Due(now time.Time, limit int) ([]queuedEnvelope, error) {''',
)

# ---- Transport agent -------------------------------------------------------------
replace_once(
    "contrib/space-compute/pkg/transport/agent.go",
    'type LeaseRequest struct {\n\tNamespace  string     `json:"namespace"`\n\tAssignment Assignment `json:"assignment"`\n}',
    '''type LeaseRequest struct {
\tNamespace            string                              `json:"namespace"`
\tAssignment           Assignment                          `json:"assignment"`
\tPreviousLease        *spacev1.SpaceExecutionLease        `json:"previousLease,omitempty"`
\tPreviousObservations []spacev1.SpaceExecutionObservation `json:"previousObservations,omitempty"`
}''',
)
replace_once(
    "contrib/space-compute/pkg/transport/agent.go",
    '\tPrivateKey        ed25519.PrivateKey\n\tQueue             *DiskQueue',
    '\tPrivateKey        ed25519.PrivateKey\n\tPeerKeys          PeerKeys\n\tStateDir          string\n\tQueue             *DiskQueue',
)
replace_once(
    "contrib/space-compute/pkg/transport/agent.go",
    '\tif a.Queue == nil || a.Store == nil || a.Assembler == nil {\n\t\treturn fmt.Errorf("queue, store and assembler are required")\n\t}',
    '\tif a.Queue == nil || a.Store == nil || a.Assembler == nil || a.PeerKeys == nil || strings.TrimSpace(a.StateDir) == "" {\n\t\treturn fmt.Errorf("queue, store, assembler, peer keys and persistent state directory are required")\n\t}',
)

replace_between(
    "contrib/space-compute/pkg/transport/agent.go",
    'func (a *Agent) reconcileTransfers(ctx context.Context) error {',
    'func (a *Agent) reconcileAssignments(ctx context.Context) error {',
    r'''func (a *Agent) reconcileTransfers(ctx context.Context) error {
\tintents, err := a.Store.ListTransferIntents(ctx)
\tif err != nil {
\t\treturn err
\t}
\treceipts, err := a.Store.ListTransferReceipts(ctx)
\tif err != nil {
\t\treturn err
\t}
\tnow := a.now()
\tfor _, intent := range intents {
\t\tif intent == nil || !intent.Spec.ExpiresAt.After(now) {
\t\t\tcontinue
\t\t}
\t\tif err := spacev1.ValidateTransferIntent(intent, agentClock{now}); err != nil {
\t\t\treturn fmt.Errorf("transfer intent %s: %w", intent.Name, err)
\t\t}
\t\tif hasTransferReceipt(intent, receipts) {
\t\t\tcontinue
\t\t}
\t\tif intent.Spec.Coordinator == a.Local && intent.Spec.Source != a.Local {
\t\t\traw, err := json.Marshal(intent)
\t\t\tif err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t\tid := "transfer-intent-" + intent.Name
\t\t\te := NewEnvelope(id, TransferIntentKind, a.Local, intent.Spec.Source, intent.Spec.MissionUID, intent.Spec.PlanID, intent.Spec.Attempt, 1, now, intent.Spec.ExpiresAt.Time, raw)
\t\t\tif err := e.Sign(a.PrivateKey); err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t\tif err := a.Queue.Enqueue(e); err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t}
\t\tif intent.Spec.Source != a.Local {
\t\t\tcontinue
\t\t}
\t\tif now.Before(intent.Spec.Window.Start.Time.Add(-a.Limits.MaximumClockSkew)) || !intent.Spec.Window.End.After(now.Add(-a.Limits.MaximumClockSkew)) {
\t\t\tcontinue
\t\t}
\t\tchunks, err := ReadChunks(intent, a.DataRoot, a.MaxChunkBytes)
\t\tif err != nil {
\t\t\treturn fmt.Errorf("transfer %s: %w", intent.Name, err)
\t\t}
\t\tfor _, chunk := range chunks {
\t\t\tsequence := int64(chunk.ChunkIndex) + 1
\t\t\tqueued, err := a.Queue.Contains(intent.Name, sequence)
\t\t\tif err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t\tif queued {
\t\t\t\tcontinue
\t\t\t}
\t\t\traw, err := json.Marshal(chunk)
\t\t\tif err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t\te := NewEnvelope(intent.Name, TransferChunkKind, a.Local, intent.Spec.Destination, intent.Spec.MissionUID, intent.Spec.PlanID, intent.Spec.Attempt, sequence, now, intent.Spec.ExpiresAt.Time, raw)
\t\t\tif err := e.Sign(a.PrivateKey); err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t\tif err := a.Queue.Enqueue(e); err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t}
\t}
\treturn nil
}''',
)

replace_between(
    "contrib/space-compute/pkg/transport/agent.go",
    'func (a *Agent) reconcileAssignments(ctx context.Context) error {',
    'func (a *Agent) reconcileRemoteAssignments(ctx context.Context) error {',
    r'''func (a *Agent) reconcileAssignments(ctx context.Context) error {
\tassignments, err := a.Store.ListAssignments(ctx)
\tif err != nil {
\t\treturn err
\t}
\tleases, err := a.Store.ListExecutionLeases(ctx)
\tif err != nil {
\t\treturn err
\t}
\tobservations, err := a.Store.ListExecutionObservations(ctx)
\tif err != nil {
\t\treturn err
\t}
\tnow := a.now()
\tfor _, assignment := range assignments {
\t\tif assignment.Mission == nil || assignment.Placement == nil || !assignment.Placement.Spec.ExpiresAt.After(now) {
\t\t\tcontinue
\t\t}
\t\tp := assignment.Placement
\t\tm := assignment.Mission
\t\tlease, _ := spaceexecution.LatestLeaseForAttempt(leases, string(m.UID), p.Spec.PlanID, p.Spec.Attempt, now)
\t\tif lease != nil {
\t\t\tcontinue
\t\t}
\t\tvar previous *spacev1.SpaceExecutionLease
\t\tvar proof []spacev1.SpaceExecutionObservation
\t\tif p.Spec.Attempt > 1 {
\t\t\tprevious, proof, err = priorFenceProof(m, p, leases, observations, now)
\t\t\tif err != nil || previous == nil {
\t\t\t\t// Fail closed: a replacement attempt is not even requested until the
\t\t\t\t// coordinator has trusted old-attempt evidence.
\t\t\t\tcontinue
\t\t\t}
\t\t}
\t\tminEpoch := int64(0)
\t\tif previous != nil {
\t\t\tminEpoch = previous.Spec.Fence.LeaseEpoch
\t\t}
\t\tif p.Spec.Target == a.Local {
\t\t\tif _, _, err := a.issueLease(ctx, assignment, a.Local, minEpoch); err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t\tcontinue
\t\t}
\t\trequest := LeaseRequest{Namespace: m.Namespace, Assignment: assignment, PreviousLease: previous, PreviousObservations: proof}
\t\traw, err := json.Marshal(request)
\t\tif err != nil {
\t\t\treturn err
\t\t}
\t\tid := "lease-request-" + p.Spec.PlanID + fmt.Sprintf("-%d", p.Spec.Attempt)
\t\te := NewEnvelope(id, LeaseRequestKind, a.Local, p.Spec.Target, string(m.UID), p.Spec.PlanID, p.Spec.Attempt, 1, now, p.Spec.ExpiresAt.Time, raw)
\t\tif err := e.Sign(a.PrivateKey); err != nil {
\t\t\treturn err
\t\t}
\t\tif err := a.Queue.Enqueue(e); err != nil {
\t\t\treturn err
\t\t}
\t}
\treturn nil
}''',
)

replace_between(
    "contrib/space-compute/pkg/transport/agent.go",
    'func (a *Agent) reconcileRemoteAssignments(ctx context.Context) error {',
    'func (a *Agent) reconcileHeartbeats(ctx context.Context) error {',
    r'''func (a *Agent) reconcileRemoteAssignments(ctx context.Context) error {
\tassignments, err := a.Store.ListRemoteAssignments(ctx)
\tif err != nil {
\t\treturn err
\t}
\tleases, err := a.Store.ListExecutionLeases(ctx)
\tif err != nil {
\t\treturn err
\t}
\treceipts, err := a.Store.ListTransferReceipts(ctx)
\tif err != nil {
\t\treturn err
\t}
\tobservations, err := a.Store.ListExecutionObservations(ctx)
\tif err != nil {
\t\treturn err
\t}
\tnow := a.now()
\tfor _, assignment := range assignments {
\t\tif assignment.Mission == nil || assignment.Placement == nil || assignment.Placement.Spec.Target != a.Local {
\t\t\tcontinue
\t\t}
\t\tm, p := assignment.Mission, assignment.Placement
\t\tlease := latestLeaseForAttemptAny(leases, string(m.UID), p.Spec.PlanID, p.Spec.Attempt)
\t\tif lease == nil {
\t\t\tcontinue
\t\t}
\t\tif latestMissionEpoch(leases, string(m.UID)) > lease.Spec.Fence.LeaseEpoch {
\t\t\tif err := a.fenceRemoteExecution(ctx, assignment, lease, "superseded lease epoch", observations); err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t\tcontinue
\t\t}
\t\tterminal := terminalObservation(lease, observations, now)
\t\tif terminal != nil {
\t\t\tif terminal.Spec.Phase == spacev1.ExecutionObservationStopped {
\t\t\t\tif err := a.fenceRemoteExecution(ctx, assignment, lease, "trusted stop", observations); err != nil {
\t\t\t\t\treturn err
\t\t\t\t}
\t\t\t}
\t\t\tcontinue
\t\t}
\t\tconfirmed, err := a.leaseConfirmed(lease)
\t\tif err != nil {
\t\t\treturn err
\t\t}
\t\tskew := time.Duration(lease.Spec.MaximumClockSkewSeconds) * time.Second
\t\tsafety := skew + 2*time.Second
\t\tif !confirmed || spaceexecution.ValidateLease(lease, now) != nil || !lease.Spec.Fence.ExpiresAt.Time.After(now.Add(safety)) {
\t\t\tif err := a.fenceRemoteExecution(ctx, assignment, lease, "lease confirmation/expiry fence", observations); err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t\tcontinue
\t\t}
\t\tif err := spaceexecution.CanDispatch(m, p, lease, receipts, now); err != nil {
\t\t\tcontinue
\t\t}
\t\tif a.Executor == nil {
\t\t\treturn fmt.Errorf("remote assignment ready but executor is unavailable")
\t\t}
\t\tif err := a.Executor.EnsureExecution(ctx, m, p, lease); err != nil {
\t\t\treturn err
\t\t}
\t}
\treturn nil
}''',
)

replace_between(
    "contrib/space-compute/pkg/transport/agent.go",
    'func (a *Agent) reconcileHeartbeats(ctx context.Context) error {',
    'func (a *Agent) HandleEnvelope(ctx context.Context, e *Envelope) error {',
    r'''func (a *Agent) reconcileHeartbeats(ctx context.Context) error {
\tleases, err := a.Store.ListExecutionLeases(ctx)
\tif err != nil {
\t\treturn err
\t}
\tlocalAssignments, err := a.Store.ListAssignments(ctx)
\tif err != nil {
\t\treturn err
\t}
\tremoteAssignments, err := a.Store.ListRemoteAssignments(ctx)
\tif err != nil {
\t\treturn err
\t}
\tobservations, err := a.Store.ListExecutionObservations(ctx)
\tif err != nil {
\t\treturn err
\t}
\tnow := a.now()
\tfor _, lease := range leases {
\t\tif lease == nil || lease.Spec.Source != a.Local || latestMissionEpoch(leases, lease.Spec.Fence.MissionUID) != lease.Spec.Fence.LeaseEpoch {
\t\t\tcontinue
\t\t}
\t\tassignment := assignmentForLease(lease, localAssignments, remoteAssignments)
\t\tif assignment == nil || assignment.Placement == nil || !assignment.Placement.Spec.ExpiresAt.After(now) {
\t\t\tcontinue
\t\t}
\t\tterminal := terminalObservation(lease, observations, now)
\t\tif terminal != nil {
\t\t\tswitch terminal.Spec.Phase {
\t\t\tcase spacev1.ExecutionObservationStopped, spacev1.ExecutionObservationFailed:
\t\t\t\tcontinue
\t\t\tcase spacev1.ExecutionObservationCompleted:
\t\t\t\tif assignment.Mission == nil || !assignment.Mission.Spec.ResultReturnRequired {
\t\t\t\t\tcontinue
\t\t\t\t}
\t\t\t}
\t\t}
\t\tif lease.Spec.Destination != a.Local {
\t\t\tconfirmed, err := a.leaseConfirmed(lease)
\t\t\tif err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t\tif !confirmed {
\t\t\t\tcontinue
\t\t\t}
\t\t}
\t\tnextHeartbeat := lease.Spec.HeartbeatAt.Time.Add(a.LeaseTTL / 2)
\t\tif now.Before(nextHeartbeat) {
\t\t\tcontinue
\t\t}
\t\tnextExpiry := lease.Spec.Fence.ExpiresAt.Time.Add(a.LeaseTTL / 2)
\t\tif nextExpiry.After(assignment.Placement.Spec.ExpiresAt.Time) {
\t\t\tnextExpiry = assignment.Placement.Spec.ExpiresAt.Time
\t\t}
\t\tif !nextExpiry.After(nextHeartbeat) || !nextExpiry.After(now.Add(time.Duration(lease.Spec.MaximumClockSkewSeconds)*time.Second)) {
\t\t\tcontinue
\t\t}
\t\tnext := lease.DeepCopy()
\t\tnext.Spec.Provenance.Sequence++
\t\tnext.Spec.Provenance.PreviousDigest = lease.Spec.Provenance.Digest
\t\tnext.Spec.HeartbeatAt = metav1.NewTime(nextHeartbeat.UTC())
\t\tnext.Spec.Fence.ExpiresAt = metav1.NewTime(nextExpiry.UTC())
\t\tif err := a.signLease(next); err != nil {
\t\t\treturn err
\t\t}
\t\tif next.Spec.Destination == a.Local {
\t\t\tif err := a.Store.UpsertExecutionLease(ctx, next); err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t\tcontinue
\t\t}
\t\t// Two-phase renewal: the remote execution domain does not persist the
\t\t// extended expiry until the coordinator echoes a LeaseAck.
\t\tif err := a.enqueueReporterObject(next.Spec.Destination, "spaceexecutionleases", next, next.Spec.Fence.MissionUID, next.Spec.Fence.PlanID, next.Spec.Fence.Attempt, next.Spec.Provenance.Sequence, next.Spec.Fence.ExpiresAt.Time); err != nil {
\t\t\treturn err
\t\t}
\t}
\treturn nil
}''',
)

replace_between(
    "contrib/space-compute/pkg/transport/agent.go",
    'func (a *Agent) HandleEnvelope(ctx context.Context, e *Envelope) error {',
    'func (a *Agent) acceptLeaseRequest(ctx context.Context, e *Envelope, r *LeaseRequest) error {',
    r'''func (a *Agent) HandleEnvelope(ctx context.Context, e *Envelope) error {
\tif e == nil {
\t\treturn fmt.Errorf("envelope required")
\t}
\tswitch e.Kind {
\tcase TransferIntentKind:
\t\tvar intent spacev1.SpaceTransferIntent
\t\tif err := json.Unmarshal(e.Payload, &intent); err != nil {
\t\t\treturn err
\t\t}
\t\tif intent.Spec.Source != a.Local || intent.Spec.Coordinator != e.Source || intent.Spec.MissionUID != e.MissionUID || intent.Spec.PlanID != e.PlanID || intent.Spec.Attempt != e.Attempt {
\t\t\treturn fmt.Errorf("transfer intent envelope metadata mismatch")
\t\t}
\t\tif err := spacev1.ValidateTransferIntent(&intent, agentClock{a.now()}); err != nil {
\t\t\treturn err
\t\t}
\t\treturn a.Store.UpsertTransferIntent(ctx, &intent)
\tcase TransferChunkKind:
\t\tvar chunk TransferChunk
\t\tif err := json.Unmarshal(e.Payload, &chunk); err != nil {
\t\t\treturn err
\t\t}
\t\tif chunk.Source != e.Source || chunk.Destination != e.Destination || chunk.MissionUID != e.MissionUID || chunk.PlanID != e.PlanID || chunk.Attempt != e.Attempt {
\t\t\treturn fmt.Errorf("transfer chunk metadata does not match envelope")
\t\t}
\t\tcomplete, ack, err := a.Assembler.Accept(chunk)
\t\tif err != nil {
\t\t\treturn err
\t\t}
\t\tif complete {
\t\t\treturn a.enqueueAck(ack)
\t\t}
\t\treturn nil
\tcase TransferAckKind:
\t\tvar ack TransferAck
\t\tif err := json.Unmarshal(e.Payload, &ack); err != nil {
\t\t\treturn err
\t\t}
\t\treturn a.acceptAck(ctx, e, &ack)
\tcase LeaseRequestKind:
\t\tvar request LeaseRequest
\t\tif err := json.Unmarshal(e.Payload, &request); err != nil {
\t\t\treturn err
\t\t}
\t\treturn a.acceptLeaseRequest(ctx, e, &request)
\tcase LeaseGrantKind:
\t\tvar grant LeaseGrant
\t\tif err := json.Unmarshal(e.Payload, &grant); err != nil {
\t\t\treturn err
\t\t}
\t\treturn a.acceptLeaseGrant(ctx, e, &grant)
\tcase LeaseAckKind:
\t\tvar ack LeaseAck
\t\tif err := json.Unmarshal(e.Payload, &ack); err != nil {
\t\t\treturn err
\t\t}
\t\treturn a.acceptLeaseAck(ctx, e, &ack)
\tcase ReporterObjectKind:
\t\tvar object ReporterObject
\t\tif err := json.Unmarshal(e.Payload, &object); err != nil {
\t\t\treturn err
\t\t}
\t\treturn a.acceptReporterObject(ctx, e, &object)
\tdefault:
\t\treturn fmt.Errorf("unsupported envelope kind %q", e.Kind)
\t}
}''',
)

replace_between(
    "contrib/space-compute/pkg/transport/agent.go",
    'func (a *Agent) acceptLeaseRequest(ctx context.Context, e *Envelope, r *LeaseRequest) error {',
    'func (a *Agent) issueLease(ctx context.Context, assignment Assignment, destination spacev1.DomainReference)',
    r'''func (a *Agent) acceptLeaseRequest(ctx context.Context, e *Envelope, r *LeaseRequest) error {
\tif r.Assignment.Mission == nil || r.Assignment.Placement == nil {
\t\treturn fmt.Errorf("lease request assignment is required")
\t}
\tm, p := r.Assignment.Mission, r.Assignment.Placement
\tif p.Spec.Target != a.Local || string(m.UID) != e.MissionUID || p.Spec.PlanID != e.PlanID || p.Spec.Attempt != e.Attempt {
\t\treturn fmt.Errorf("lease request does not target local domain/assignment")
\t}
\tif err := spacev1.ValidateMission(m, spacev1.RealClock{}); err != nil {
\t\treturn err
\t}
\tif err := spacev1.ValidatePlacement(p, m); err != nil {
\t\treturn err
\t}
\tminEpoch := int64(0)
\tif p.Spec.Attempt > 1 {
\t\tif r.PreviousLease == nil || r.PreviousLease.Spec.Fence.MissionUID != string(m.UID) || r.PreviousLease.Spec.Fence.Attempt != p.Spec.Attempt-1 {
\t\t\treturn fmt.Errorf("replacement lease request lacks exact previous-attempt fence proof")
\t\t}
\t\tif err := a.verifyPriorFenceEvidence(m, r.PreviousLease, r.PreviousObservations); err != nil {
\t\t\treturn err
\t\t}
\t\tminEpoch = r.PreviousLease.Spec.Fence.LeaseEpoch
\t}
\tif err := a.Store.SaveRemoteAssignment(ctx, r.Assignment); err != nil {
\t\treturn err
\t}
\tlease, token, err := a.issueLease(ctx, r.Assignment, e.Source, minEpoch)
\tif err != nil {
\t\treturn err
\t}
\treturn a.enqueueLeaseGrant(r.Namespace, lease, token)
}''',
)

# The previous replacement intentionally leaves the old issueLease signature as
# the end marker. Replace the complete function through enqueueLeaseGrant.
replace_between(
    "contrib/space-compute/pkg/transport/agent.go",
    'func (a *Agent) issueLease(ctx context.Context, assignment Assignment, destination spacev1.DomainReference)',
    'func (a *Agent) enqueueLeaseGrant(namespace string, lease *spacev1.SpaceExecutionLease, token string) error {',
    r'''func (a *Agent) issueLease(ctx context.Context, assignment Assignment, destination spacev1.DomainReference, minimumEpoch int64) (*spacev1.SpaceExecutionLease, string, error) {
\tm, p := assignment.Mission, assignment.Placement
\tleases, err := a.Store.ListExecutionLeases(ctx)
\tif err != nil {
\t\treturn nil, "", err
\t}
\tfor _, existing := range leases {
\t\tif existing == nil || existing.Spec.Source != a.Local {
\t\t\tcontinue
\t\t}
\t\tf := existing.Spec.Fence
\t\tif f.MissionUID == string(m.UID) && f.PlanID == p.Spec.PlanID && f.Attempt == p.Spec.Attempt {
\t\t\tif f.LeaseEpoch <= minimumEpoch || spaceexecution.ValidateLease(existing, a.now()) != nil {
\t\t\t\treturn nil, "", fmt.Errorf("existing lease for attempt is expired/stale; a new attempt is required")
\t\t\t}
\t\t\ttoken, err := a.Store.GetFenceToken(ctx, m.Namespace, f)
\t\t\treturn existing, token, err
\t\t}
\t}
\tmaxEpoch := minimumEpoch
\tfor _, l := range leases {
\t\tif l != nil && l.Spec.Fence.MissionUID == string(m.UID) && l.Spec.Fence.LeaseEpoch > maxEpoch {
\t\t\tmaxEpoch = l.Spec.Fence.LeaseEpoch
\t\t}
\t}
\ttoken, hash, err := spaceexecution.NewFenceToken()
\tif err != nil {
\t\treturn nil, "", err
\t}
\tf := spacev1.ExecutionFence{MissionUID: string(m.UID), PlanID: p.Spec.PlanID, Attempt: p.Spec.Attempt, LeaseEpoch: maxEpoch + 1, TokenHash: hash, ExpiresAt: metav1.NewTime(a.now().Add(a.LeaseTTL))}
\tlease := &spacev1.SpaceExecutionLease{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceExecutionLease"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.ExecutionLeaseName(f.MissionUID, f.PlanID, f.Attempt, f.LeaseEpoch)}, Spec: spacev1.SpaceExecutionLeaseSpec{Source: a.Local, Destination: destination, Fence: f, HeartbeatAt: metav1.NewTime(a.now()), MaximumClockSkewSeconds: int64(a.Limits.MaximumClockSkew / time.Second), Provenance: a.baseProvenance(1)}}
\tif err := a.signLease(lease); err != nil {
\t\treturn nil, "", err
\t}
\tif err := a.Store.PutFenceToken(ctx, m.Namespace, f, token); err != nil {
\t\treturn nil, "", err
\t}
\tif err := a.Store.UpsertExecutionLease(ctx, lease); err != nil {
\t\treturn nil, "", err
\t}
\tif destination == a.Local {
\t\tif err := a.markLeaseConfirmed(lease); err != nil {
\t\t\treturn nil, "", err
\t\t}
\t}
\treturn lease, token, nil
}''',
)

replace_between(
    "contrib/space-compute/pkg/transport/agent.go",
    'func (a *Agent) acceptLeaseGrant(ctx context.Context, e *Envelope, g *LeaseGrant) error {',
    'func (a *Agent) enqueueAck(ack *TransferAck) error {',
    r'''func (a *Agent) acceptLeaseGrant(ctx context.Context, e *Envelope, g *LeaseGrant) error {
\tl := &g.Lease
\tif l.Spec.Source != e.Source || l.Spec.Destination != a.Local || l.Spec.Fence.MissionUID != e.MissionUID || l.Spec.Fence.PlanID != e.PlanID || l.Spec.Fence.Attempt != e.Attempt {
\t\treturn fmt.Errorf("lease grant metadata mismatch")
\t}
\thash, err := spaceexecution.TokenHash(g.Token)
\tif err != nil || hash != l.Spec.Fence.TokenHash {
\t\treturn fmt.Errorf("lease grant token hash mismatch")
\t}
\tleases, err := a.Store.ListExecutionLeases(ctx)
\tif err != nil {
\t\treturn err
\t}
\tif err := validateIncomingLease(leases, l, a.now()); err != nil {
\t\treturn err
\t}
\tif err := a.Store.PutFenceToken(ctx, g.Namespace, l.Spec.Fence, g.Token); err != nil {
\t\treturn err
\t}
\tif err := a.Store.UpsertRemoteReporterObject(ctx, "spaceexecutionleases", mustJSON(l)); err != nil {
\t\treturn err
\t}
\treturn a.enqueueLeaseAck(l.Spec.Source, l)
}''',
)

replace_between(
    "contrib/space-compute/pkg/transport/agent.go",
    'func (a *Agent) acceptAck(ctx context.Context, e *Envelope, ack *TransferAck) error {',
    'func (a *Agent) ReportExecution(ctx context.Context, namespace string, report spaceexecution.Report) error {',
    r'''func (a *Agent) acceptAck(ctx context.Context, e *Envelope, ack *TransferAck) error {
\tintent, err := a.Store.GetTransferIntent(ctx, ack.IntentName)
\tif err != nil {
\t\treturn err
\t}
\ts := intent.Spec
\tif s.TransferID != ack.TransferID || s.MissionUID != ack.MissionUID || s.PlanID != ack.PlanID || s.Attempt != ack.Attempt || s.Purpose != ack.Purpose || s.Source != ack.Destination || s.Destination != ack.Source || s.DataID != ack.DataID || s.Bytes != ack.Bytes || s.PayloadDigest != ack.PayloadDigest || s.LeaseEpoch != ack.LeaseEpoch || s.TokenHash != ack.TokenHash {
\t\treturn fmt.Errorf("transfer ACK does not exactly match local intent")
\t}
\treceipt := &spacev1.SpaceTransferReceipt{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceTransferReceipt"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.TransferReceiptName(s.Source, s.Destination, s.MissionUID, s.PlanID, s.TransferID)}, Spec: spacev1.SpaceTransferReceiptSpec{TransferID: s.TransferID, MissionUID: s.MissionUID, PlanID: s.PlanID, Attempt: s.Attempt, Source: s.Source, Destination: s.Destination, DataID: s.DataID, Bytes: s.Bytes, PayloadDigest: s.PayloadDigest, StartedAt: metav1.NewTime(ack.StartedAt), CompletedAt: metav1.NewTime(ack.CompletedAt), Provenance: a.baseProvenance(1)}}
\tif err := a.signTransferReceipt(receipt); err != nil {
\t\treturn err
\t}
\tif err := a.Store.UpsertTransferReceipt(ctx, receipt); err != nil {
\t\treturn err
\t}
\tfor _, destination := range uniqueDomains(s.Destination, s.Coordinator) {
\t\tif destination == a.Local {
\t\t\tcontinue
\t\t}
\t\tif err := a.enqueueReporterObject(destination, "spacetransferreceipts", receipt, s.MissionUID, s.PlanID, s.Attempt, 1, intent.Spec.ExpiresAt.Time); err != nil {
\t\t\treturn err
\t\t}
\t}
\tif s.Purpose == spacev1.TransferPurposeResult {
\t\tresult := &spacev1.SpaceResultReceipt{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceResultReceipt"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.ResultReceiptName(s.Source, s.Destination, s.MissionUID, s.PlanID, spacev1.ResultTransferID(s.Attempt))}, Spec: spacev1.SpaceResultReceiptSpec{ResultID: spacev1.ResultTransferID(s.Attempt), MissionUID: s.MissionUID, PlanID: s.PlanID, Attempt: s.Attempt, Source: s.Source, Destination: s.Destination, Bytes: s.Bytes, PayloadDigest: s.PayloadDigest, LeaseEpoch: s.LeaseEpoch, TokenHash: s.TokenHash, CompletedAt: metav1.NewTime(ack.CompletedAt), Provenance: a.baseProvenance(1)}}
\t\tif err := a.signResultReceipt(result); err != nil {
\t\t\treturn err
\t\t}
\t\tif err := a.Store.UpsertResultReceipt(ctx, result); err != nil {
\t\t\treturn err
\t\t}
\t\tfor _, destination := range uniqueDomains(s.Destination, s.Coordinator) {
\t\t\tif destination == a.Local {
\t\t\t\tcontinue
\t\t\t}
\t\t\tif err := a.enqueueReporterObject(destination, "spaceresultreceipts", result, s.MissionUID, s.PlanID, s.Attempt, 1, intent.Spec.ExpiresAt.Time); err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t}
\t}
\treturn nil
}''',
)

replace_between(
    "contrib/space-compute/pkg/transport/agent.go",
    'func (a *Agent) ReportExecution(ctx context.Context, namespace string, report spaceexecution.Report) error {',
    'func (a *Agent) enqueueReporterObject(destination spacev1.DomainReference, resource string, object any, missionUID, planID string, attempt int32, sequence int64, expiry time.Time) error {',
    r'''func (a *Agent) ReportExecution(ctx context.Context, namespace string, report spaceexecution.Report) error {
\tleases, err := a.Store.ListExecutionLeases(ctx)
\tif err != nil {
\t\treturn err
\t}
\tif latestMissionEpoch(leases, report.MissionUID) != report.LeaseEpoch {
\t\treturn fmt.Errorf("execution report uses a superseded lease epoch")
\t}
\tvar lease *spacev1.SpaceExecutionLease
\tfor _, candidate := range leases {
\t\tif candidate != nil && candidate.Spec.Fence.MissionUID == report.MissionUID && candidate.Spec.Fence.PlanID == report.PlanID && candidate.Spec.Fence.Attempt == report.Attempt && candidate.Spec.Fence.LeaseEpoch == report.LeaseEpoch {
\t\t\tlease = candidate
\t\t\tbreak
\t\t}
\t}
\tif lease == nil {
\t\treturn fmt.Errorf("execution lease not found")
\t}
\tif lease.Spec.Source != a.Local {
\t\treturn fmt.Errorf("only local-domain execution may report through this agent")
\t}
\tif err := spaceexecution.ValidateReport(report, lease, a.now()); err != nil {
\t\treturn err
\t}
\tlocalAssignments, err := a.Store.ListAssignments(ctx)
\tif err != nil {
\t\treturn err
\t}
\tremoteAssignments, err := a.Store.ListRemoteAssignments(ctx)
\tif err != nil {
\t\treturn err
\t}
\tassignment := assignmentForLease(lease, localAssignments, remoteAssignments)
\tif assignment == nil || assignment.Mission == nil || assignment.Placement == nil || assignment.Placement.Spec.Target != a.Local {
\t\treturn fmt.Errorf("execution assignment not found for fence")
\t}
\tobservations, err := a.Store.ListExecutionObservations(ctx)
\tif err != nil {
\t\treturn err
\t}
\tif terminalObservation(lease, observations, a.now()) != nil {
\t\treturn fmt.Errorf("execution fence is terminal; further token reports are rejected")
\t}
\tid := strings.ToLower(string(report.Phase)) + fmt.Sprintf("-%d-%d", report.LeaseEpoch, a.now().UnixNano())
\tif len(id) > 63 {
\t\tid = id[:63]
\t}
\tobs := &spacev1.SpaceExecutionObservation{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceExecutionObservation"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.ExecutionObservationName(a.Local, lease.Spec.Destination, report.MissionUID, report.PlanID, id)}, Spec: spacev1.SpaceExecutionObservationSpec{ObservationID: id, MissionUID: report.MissionUID, PlanID: report.PlanID, Attempt: report.Attempt, LeaseEpoch: report.LeaseEpoch, TokenHash: lease.Spec.Fence.TokenHash, Source: a.Local, Destination: lease.Spec.Destination, Phase: report.Phase, CheckpointID: report.CheckpointID, ObservedAt: metav1.NewTime(a.now()), Provenance: a.baseProvenance(1)}}
\tif err := a.signObservation(obs); err != nil {
\t\treturn err
\t}
\tif err := a.Store.UpsertExecutionObservation(ctx, obs); err != nil {
\t\treturn err
\t}
\tif obs.Spec.Destination != a.Local {
\t\tif err := a.enqueueReporterObject(obs.Spec.Destination, "spaceexecutionobservations", obs, report.MissionUID, report.PlanID, report.Attempt, 1, assignment.Placement.Spec.ExpiresAt.Time); err != nil {
\t\t\treturn err
\t\t}
\t}
\tif report.Phase != spacev1.ExecutionObservationCompleted {
\t\treturn nil
\t}
\tif !assignment.Mission.Spec.ResultReturnRequired {
\t\treturn nil
\t}
\tif report.ResultDataID == "" {
\t\treturn fmt.Errorf("completed execution requires resultDataID when result return is required")
\t}
\tif assignment.Placement.Spec.ResultTransfer == nil {
\t\treturn fmt.Errorf("result return is required but placement has no result transfer")
\t}
\tpath, err := DataPath(a.DataRoot, report.ResultDataID)
\tif err != nil {
\t\treturn err
\t}
\traw, err := os.ReadFile(path)
\tif err != nil {
\t\treturn err
\t}
\tif assignment.Mission.Spec.OutputSizeBytes > 0 && int64(len(raw)) != assignment.Mission.Spec.OutputSizeBytes {
\t\treturn fmt.Errorf("result size does not match mission outputSizeBytes")
\t}
\tsum := sha256Bytes(raw)
\tdigest := hex.EncodeToString(sum[:])
\ttransfer := *assignment.Placement.Spec.ResultTransfer
\ttransfer.DataID = report.ResultDataID
\ttransfer.Source = a.Local
\ttransfer.Bytes = int64(len(raw))
\tcoordinator := lease.Spec.Destination
\tif transfer.Destination == a.Local {
\t\tresult := &spacev1.SpaceResultReceipt{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceResultReceipt"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.ResultReceiptName(a.Local, a.Local, report.MissionUID, report.PlanID, spacev1.ResultTransferID(report.Attempt))}, Spec: spacev1.SpaceResultReceiptSpec{ResultID: spacev1.ResultTransferID(report.Attempt), MissionUID: report.MissionUID, PlanID: report.PlanID, Attempt: report.Attempt, Source: a.Local, Destination: a.Local, Bytes: int64(len(raw)), PayloadDigest: digest, LeaseEpoch: lease.Spec.Fence.LeaseEpoch, TokenHash: lease.Spec.Fence.TokenHash, CompletedAt: metav1.NewTime(a.now()), Provenance: a.baseProvenance(1)}}
\t\tif err := a.signResultReceipt(result); err != nil {
\t\t\treturn err
\t\t}
\t\tif err := a.Store.UpsertResultReceipt(ctx, result); err != nil {
\t\t\treturn err
\t\t}
\t\tif coordinator != a.Local {
\t\t\treturn a.enqueueReporterObject(coordinator, "spaceresultreceipts", result, report.MissionUID, report.PlanID, report.Attempt, 1, assignment.Placement.Spec.ExpiresAt.Time)
\t\t}
\t\treturn nil
\t}
\ttransferID := spacev1.ResultTransferID(report.Attempt)
\tintent := &spacev1.SpaceTransferIntent{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceTransferIntent"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.TransferIntentName(a.Local, transfer.Destination, report.MissionUID, report.PlanID, transferID)}, Spec: spacev1.SpaceTransferIntentSpec{TransferID: transferID, MissionUID: report.MissionUID, PlanID: report.PlanID, Attempt: report.Attempt, Purpose: spacev1.TransferPurposeResult, Coordinator: coordinator, Source: a.Local, Destination: transfer.Destination, DataID: report.ResultDataID, Bytes: int64(len(raw)), PayloadDigest: digest, LeaseEpoch: lease.Spec.Fence.LeaseEpoch, TokenHash: lease.Spec.Fence.TokenHash, Window: transfer, ExpiresAt: assignment.Placement.Spec.ExpiresAt}}
\treturn a.Store.UpsertTransferIntent(ctx, intent)
}''',
)

replace_once(
    "contrib/space-compute/pkg/transport/agent.go",
    '\tid := resource + "-" + objectName(object)\n\te := NewEnvelope(id, ReporterObjectKind, a.Local, destination, missionUID, planID, attempt, sequence, a.now(), expiry, payload)',
    '\tid := reporterEnvelopeID(resource, objectName(object), destination)\n\te := NewEnvelope(id, ReporterObjectKind, a.Local, destination, missionUID, planID, attempt, sequence, a.now(), expiry, payload)',
)

# ---- Workload dispatch/evidence ---------------------------------------------------
replace_once(
    "contrib/space-compute/pkg/workload/controller.go",
    '\treceipts, err := c.Evidence.ListTransferReceipts(ctx)\n',
    '\tif len(placement.Spec.InputTransfers) > 0 && c.LocalDomain == nil {\n\t\treturn c.wait(ctx, mission, placement, spacev1.PlacementTransferPending, "TransferCoordinatorUnavailable", "local domain identity/transfer agent is not configured")\n\t}\n\treceipts, err := c.Evidence.ListTransferReceipts(ctx)\n',
)
replace_once(
    "contrib/space-compute/pkg/workload/controller.go",
    'Spec: spacev1.SpaceTransferIntentSpec{TransferID: transferID, MissionUID: string(mission.UID), PlanID: placement.Spec.PlanID, Attempt: placement.Spec.Attempt, Source: epoch.Source, Destination: epoch.Destination, DataID: epoch.DataID, Bytes: epoch.Bytes, PayloadDigest: payloadDigest, Window: epoch, ExpiresAt: placement.Spec.ExpiresAt}',
    'Spec: spacev1.SpaceTransferIntentSpec{TransferID: transferID, MissionUID: string(mission.UID), PlanID: placement.Spec.PlanID, Attempt: placement.Spec.Attempt, Purpose: spacev1.TransferPurposeInput, Coordinator: *c.LocalDomain, Source: epoch.Source, Destination: epoch.Destination, DataID: epoch.DataID, Bytes: epoch.Bytes, PayloadDigest: payloadDigest, Window: epoch, ExpiresAt: placement.Spec.ExpiresAt}',
)

replace_between(
    "contrib/space-compute/pkg/workload/controller.go",
    'func (c *Controller) ReconcileTrustedEvidence(ctx context.Context, mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent) (bool, error) {',
    'func matchingTransferReceipt(intent *spacev1.SpaceTransferIntent, receipts []*spacev1.SpaceTransferReceipt) bool {',
    r'''func (c *Controller) ReconcileTrustedEvidence(ctx context.Context, mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent) (bool, error) {
\tif c.Evidence == nil {
\t\treturn false, nil
\t}
\tnow := c.clock().Now().UTC()
\tleases, err := c.Evidence.ListExecutionLeases(ctx)
\tif err != nil {
\t\treturn false, err
\t}
\tlease, err := spaceexecution.LatestLeaseForAttempt(leases, string(mission.UID), placement.Spec.PlanID, placement.Spec.Attempt, now)
\tif err != nil || lease == nil {
\t\treturn false, err
\t}
\tobservations, err := c.Evidence.ListExecutionObservations(ctx)
\tif err != nil {
\t\treturn false, err
\t}
\tchangedAny := false
\t// Signed remote terminal evidence drives the same local durable state machine
\t// as a locally observed Pod. Legacy Pod annotations are never consulted here.
\tfor _, o := range observations {
\t\tif o == nil || spaceexecution.ValidateObservationAgainstLease(o, lease, now) != nil {
\t\t\tcontinue
\t\t}
\t\tphase := ""
\t\tswitch o.Spec.Phase {
\t\tcase spacev1.ExecutionObservationFailed:
\t\t\tphase = "failed"
\t\tcase spacev1.ExecutionObservationCompleted:
\t\t\tif mission.Spec.ResultReturnRequired {
\t\t\t\tphase = "return-pending"
\t\t\t} else {
\t\t\t\tphase = "completed"
\t\t\t}
\t\tcase spacev1.ExecutionObservationCheckpointed:
\t\t\tif placement.Status.Phase == spacev1.PlacementReplanning && mission.Spec.Checkpoint.Checkpointable {
\t\t\t\tphase = "checkpointed"
\t\t\t}
\t\t}
\t\tif phase == "" {
\t\t\tcontinue
\t\t}
\t\tobs := spacev1.ExecutionObservation{Sequence: placement.Status.LastObservationSequence + 1, Attempt: placement.Spec.Attempt, PodUID: remoteFenceUID(lease), Phase: phase, ObservedAt: o.Spec.ObservedAt, CheckpointID: o.Spec.CheckpointID}
\t\tchanged, err := planner.ApplyExecutionObservation(placement, mission, obs, c.clock())
\t\tif err != nil {
\t\t\treturn false, err
\t\t}
\t\tif changed {
\t\t\tchangedAny = true
\t\t\tif err := c.Store.UpdatePlacementStatus(ctx, placement); err != nil {
\t\t\t\treturn false, err
\t\t\t}
\t\t}
\t}
\tif placement.Status.Phase == spacev1.PlacementReturnPending && mission.Spec.ResultReturnRequired {
\t\treceipts, err := c.Evidence.ListResultReceipts(ctx)
\t\tif err != nil {
\t\t\treturn false, err
\t\t}
\t\tfor _, r := range receipts {
\t\t\tif spaceexecution.ValidateResultAgainstLease(r, lease, now) != nil {
\t\t\t\tcontinue
\t\t\t}
\t\t\tif placement.Spec.ResultTransfer != nil && r.Spec.Destination != placement.Spec.ResultTransfer.Destination {
\t\t\t\tcontinue
\t\t\t}
\t\t\tobs := spacev1.ExecutionObservation{Sequence: placement.Status.LastObservationSequence + 1, Attempt: placement.Spec.Attempt, PodUID: remoteFenceUID(lease), Phase: "completed", ObservedAt: r.Spec.CompletedAt}
\t\t\tchanged, err := planner.ApplyExecutionObservation(placement, mission, obs, c.clock())
\t\t\tif err != nil {
\t\t\t\treturn false, err
\t\t\t}
\t\t\tif changed {
\t\t\t\tplacement.Status.ResultReturned = true
\t\t\t\tif err := c.Store.UpdatePlacementStatus(ctx, placement); err != nil {
\t\t\t\t\treturn false, err
\t\t\t\t}
\t\t\t\treturn true, nil
\t\t\t}
\t\t}
\t}
\treturn changedAny, nil
}

func remoteFenceUID(lease *spacev1.SpaceExecutionLease) string {
\tif lease == nil {
\t\treturn "remote"
\t}
\treturn fmt.Sprintf("remote-%s-%d", lease.Spec.Source.Name, lease.Spec.Fence.LeaseEpoch)
}''',
)

# ---- New transport hardening helpers ---------------------------------------------
hardening_go = r'''package transport

import (
\t"context"
\t"crypto/ed25519"
\t"crypto/sha256"
\t"encoding/base64"
\t"encoding/hex"
\t"encoding/json"
\t"fmt"
\t"os"
\t"path/filepath"
\t"sort"
\t"strings"
\t"time"

\tmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
\t"k8s.io/apimachinery/pkg/runtime"

\tspacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
\tspaceexecution "github.com/k3s-io/k3s/contrib/space-compute/pkg/execution"
)

type LeaseAck struct {
\tLease spacev1.SpaceExecutionLease `json:"lease"`
}

type fenceExecutor interface {
\tFenceExecution(context.Context, *spacev1.SpaceMission, *spacev1.SpacePlacementIntent, string) (bool, error)
}

type agentClock struct{ now time.Time }
func (c agentClock) Now() time.Time { return c.now }

type leaseConfirmation struct {
\tSequence int64  `json:"sequence"`
\tDigest   string `json:"digest"`
}

func (a *Agent) confirmationPath(lease *spacev1.SpaceExecutionLease) string {
\treturn filepath.Join(a.StateDir, "lease-confirmations", lease.Name+".json")
}

func (a *Agent) markLeaseConfirmed(lease *spacev1.SpaceExecutionLease) error {
\tif lease == nil {
\t\treturn fmt.Errorf("lease is required")
\t}
\treturn writeAtomic(a.confirmationPath(lease), leaseConfirmation{Sequence: lease.Spec.Provenance.Sequence, Digest: lease.Spec.Provenance.Digest})
}

func (a *Agent) leaseConfirmed(lease *spacev1.SpaceExecutionLease) (bool, error) {
\tif lease == nil {
\t\treturn false, nil
\t}
\tif lease.Spec.Destination == a.Local {
\t\treturn true, nil
\t}
\tvar confirmation leaseConfirmation
\tif err := readJSON(a.confirmationPath(lease), &confirmation); err != nil {
\t\tif os.IsNotExist(err) {
\t\t\treturn false, nil
\t\t}
\t\treturn false, err
\t}
\treturn confirmation.Sequence >= lease.Spec.Provenance.Sequence && confirmation.Digest == lease.Spec.Provenance.Digest, nil
}

func latestMissionEpoch(leases []*spacev1.SpaceExecutionLease, missionUID string) int64 {
\tvar max int64
\tfor _, lease := range leases {
\t\tif lease != nil && lease.Spec.Fence.MissionUID == missionUID && lease.Spec.Fence.LeaseEpoch > max {
\t\t\tmax = lease.Spec.Fence.LeaseEpoch
\t\t}
\t}
\treturn max
}

func latestLeaseForAttemptAny(leases []*spacev1.SpaceExecutionLease, missionUID, planID string, attempt int32) *spacev1.SpaceExecutionLease {
\tvar best *spacev1.SpaceExecutionLease
\tfor _, lease := range leases {
\t\tif lease == nil {
\t\t\tcontinue
\t\t}
\t\tf := lease.Spec.Fence
\t\tif f.MissionUID == missionUID && f.PlanID == planID && f.Attempt == attempt && (best == nil || f.LeaseEpoch > best.Spec.Fence.LeaseEpoch) {
\t\t\tbest = lease
\t\t}
\t}
\treturn best
}

func latestLeaseForMissionAttemptAny(leases []*spacev1.SpaceExecutionLease, missionUID string, attempt int32) *spacev1.SpaceExecutionLease {
\tvar best *spacev1.SpaceExecutionLease
\tfor _, lease := range leases {
\t\tif lease == nil || lease.Spec.Fence.MissionUID != missionUID || lease.Spec.Fence.Attempt != attempt {
\t\t\tcontinue
\t\t}
\t\tif best == nil || lease.Spec.Fence.LeaseEpoch > best.Spec.Fence.LeaseEpoch {
\t\t\tbest = lease
\t\t}
\t}
\treturn best
}

func priorFenceProof(mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent, leases []*spacev1.SpaceExecutionLease, observations []*spacev1.SpaceExecutionObservation, now time.Time) (*spacev1.SpaceExecutionLease, []spacev1.SpaceExecutionObservation, error) {
\tprevious := latestLeaseForMissionAttemptAny(leases, string(mission.UID), placement.Spec.Attempt-1)
\tif previous == nil {
\t\treturn nil, nil, fmt.Errorf("previous attempt has no trusted execution lease")
\t}
\tproof := make([]spacev1.SpaceExecutionObservation, 0, 2)
\tfor _, observation := range observations {
\t\tif observation != nil && spaceexecution.ValidateObservationAgainstLease(observation, previous, now) == nil {
\t\t\tproof = append(proof, *observation.DeepCopy())
\t\t}
\t}
\tif err := spaceexecution.CanStartAttempt(mission, previous, observations, now); err != nil {
\t\treturn nil, nil, err
\t}
\treturn previous.DeepCopy(), proof, nil
}

func (a *Agent) peerPublicKey(domain spacev1.DomainReference) (ed25519.PublicKey, error) {
\tif domain == a.Local {
\t\tkey, ok := a.PrivateKey.Public().(ed25519.PublicKey)
\t\tif !ok {
\t\t\treturn nil, fmt.Errorf("local signing public key is not Ed25519")
\t\t}
\t\treturn key, nil
\t}
\tif a.PeerKeys == nil {
\t\treturn nil, fmt.Errorf("peer key registry unavailable")
\t}
\treturn a.PeerKeys.PublicKey(domain)
}

func (a *Agent) verifyReporterEvidence(object runtime.Object, provenance spacev1.Provenance, source spacev1.DomainReference) error {
\tdigest, err := spacev1.ReporterDigest(object)
\tif err != nil {
\t\treturn err
\t}
\tif digest != provenance.Digest {
\t\treturn fmt.Errorf("reporter evidence digest mismatch")
\t}
\tdigestBytes, err := hex.DecodeString(digest)
\tif err != nil {
\t\treturn err
\t}
\tsig, err := base64.StdEncoding.DecodeString(provenance.Signature)
\tif err != nil {
\t\treturn err
\t}
\tpublicKey, err := a.peerPublicKey(source)
\tif err != nil {
\t\treturn err
\t}
\tif !ed25519.Verify(publicKey, digestBytes, sig) {
\t\treturn fmt.Errorf("reporter evidence signature verification failed")
\t}
\treturn nil
}

func (a *Agent) verifyPriorFenceEvidence(mission *spacev1.SpaceMission, previous *spacev1.SpaceExecutionLease, values []spacev1.SpaceExecutionObservation) error {
\tif previous == nil {
\t\treturn fmt.Errorf("previous lease required")
\t}
\tif err := spacev1.ValidateExecutionLease(previous, agentClock{a.now()}); err != nil {
\t\treturn err
\t}
\tif err := a.verifyReporterEvidence(previous, previous.Spec.Provenance, previous.Spec.Source); err != nil {
\t\treturn fmt.Errorf("verify previous lease: %w", err)
\t}
\tobservations := make([]*spacev1.SpaceExecutionObservation, 0, len(values))
\tfor i := range values {
\t\tvalue := values[i].DeepCopy()
\t\tif err := spacev1.ValidateExecutionObservation(value, agentClock{a.now()}); err != nil {
\t\t\treturn err
\t\t}
\t\tif err := a.verifyReporterEvidence(value, value.Spec.Provenance, value.Spec.Source); err != nil {
\t\t\treturn fmt.Errorf("verify previous observation: %w", err)
\t\t}
\t\tobservations = append(observations, value)
\t}
\treturn spaceexecution.CanStartAttempt(mission, previous, observations, a.now())
}

func validateIncomingLease(values []*spacev1.SpaceExecutionLease, next *spacev1.SpaceExecutionLease, now time.Time) error {
\tif err := spaceexecution.ValidateLease(next, now); err != nil {
\t\treturn err
\t}
\tvar latest *spacev1.SpaceExecutionLease
\tvar same *spacev1.SpaceExecutionLease
\tfor _, value := range values {
\t\tif value == nil || value.Spec.Fence.MissionUID != next.Spec.Fence.MissionUID {
\t\t\tcontinue
\t\t}
\t\tif latest == nil || value.Spec.Fence.LeaseEpoch > latest.Spec.Fence.LeaseEpoch {
\t\t\tlatest = value
\t\t}
\t\tif value.Name == next.Name {
\t\t\tsame = value
\t\t}
\t}
\tif latest == nil {
\t\treturn nil
\t}
\tif next.Spec.Fence.LeaseEpoch < latest.Spec.Fence.LeaseEpoch {
\t\treturn fmt.Errorf("lease epoch %d is stale; observed %d", next.Spec.Fence.LeaseEpoch, latest.Spec.Fence.LeaseEpoch)
\t}
\tif next.Spec.Fence.LeaseEpoch > latest.Spec.Fence.LeaseEpoch {
\t\treturn spaceexecution.ValidateLeaseAdvance(latest, next, now)
\t}
\tif same == nil {
\t\treturn fmt.Errorf("conflicting lease shares current epoch %d", next.Spec.Fence.LeaseEpoch)
\t}
\tif same.Spec.Provenance.Sequence == next.Spec.Provenance.Sequence && same.Spec.Provenance.Digest == next.Spec.Provenance.Digest {
\t\treturn nil
\t}
\treturn spaceexecution.ValidateLeaseAdvance(same, next, now)
}

func (a *Agent) enqueueLeaseAck(destination spacev1.DomainReference, lease *spacev1.SpaceExecutionLease) error {
\traw, err := json.Marshal(LeaseAck{Lease: *lease.DeepCopy()})
\tif err != nil {
\t\treturn err
\t}
\texpiry := lease.Spec.Fence.ExpiresAt.Time.Add(a.Limits.MaximumClockSkew)
\tif !expiry.After(a.now()) {
\t\texpiry = a.now().Add(a.Limits.MaximumClockSkew + time.Second)
\t}
\te := NewEnvelope("lease-ack-"+lease.Name, LeaseAckKind, a.Local, destination, lease.Spec.Fence.MissionUID, lease.Spec.Fence.PlanID, lease.Spec.Fence.Attempt, lease.Spec.Provenance.Sequence, a.now(), expiry, raw)
\tif err := e.Sign(a.PrivateKey); err != nil {
\t\treturn err
\t}
\treturn a.Queue.Enqueue(e)
}

func (a *Agent) acceptLeaseAck(ctx context.Context, e *Envelope, ack *LeaseAck) error {
\tlease := &ack.Lease
\tif lease.Spec.Source != a.Local || lease.Spec.Destination != e.Source || lease.Spec.Fence.MissionUID != e.MissionUID || lease.Spec.Fence.PlanID != e.PlanID || lease.Spec.Fence.Attempt != e.Attempt || lease.Spec.Provenance.Sequence != e.Sequence {
\t\treturn fmt.Errorf("lease ack metadata mismatch")
\t}
\tif err := a.verifyReporterEvidence(lease, lease.Spec.Provenance, a.Local); err != nil {
\t\treturn err
\t}
\tleases, err := a.Store.ListExecutionLeases(ctx)
\tif err != nil {
\t\treturn err
\t}
\tcurrent := latestLeaseForAttemptAny(leases, lease.Spec.Fence.MissionUID, lease.Spec.Fence.PlanID, lease.Spec.Fence.Attempt)
\tif current == nil {
\t\treturn fmt.Errorf("local lease for ack not found")
\t}
\tif current.Spec.Provenance.Sequence != lease.Spec.Provenance.Sequence || current.Spec.Provenance.Digest != lease.Spec.Provenance.Digest {
\t\tif err := validateIncomingLease(leases, lease, a.now()); err != nil {
\t\t\treturn err
\t\t}
\t\tif err := a.Store.UpsertExecutionLease(ctx, lease); err != nil {
\t\t\treturn err
\t\t}
\t}
\treturn a.markLeaseConfirmed(lease)
}

func (a *Agent) acceptReporterObject(ctx context.Context, e *Envelope, object *ReporterObject) error {
\tswitch object.Resource {
\tcase "spaceexecutionleases":
\t\tvar lease spacev1.SpaceExecutionLease
\t\tif err := json.Unmarshal(object.Object, &lease); err != nil {
\t\t\treturn err
\t\t}
\t\tif lease.Spec.Source != e.Source || lease.Spec.Destination != a.Local || lease.Spec.Fence.MissionUID != e.MissionUID || lease.Spec.Fence.PlanID != e.PlanID || lease.Spec.Fence.Attempt != e.Attempt || lease.Spec.Provenance.Sequence != e.Sequence {
\t\t\treturn fmt.Errorf("lease reporter envelope metadata mismatch")
\t\t}
\t\tleases, err := a.Store.ListExecutionLeases(ctx)
\t\tif err != nil {
\t\t\treturn err
\t\t}
\t\tif err := validateIncomingLease(leases, &lease, a.now()); err != nil {
\t\t\treturn err
\t\t}
\t\tif err := a.Store.UpsertRemoteReporterObject(ctx, object.Resource, object.Object); err != nil {
\t\t\treturn err
\t\t}
\t\treturn a.enqueueLeaseAck(lease.Spec.Source, &lease)
\tcase "spaceexecutionobservations":
\t\tvar observation spacev1.SpaceExecutionObservation
\t\tif err := json.Unmarshal(object.Object, &observation); err != nil {
\t\t\treturn err
\t\t}
\t\tleases, err := a.Store.ListExecutionLeases(ctx)
\t\tif err != nil {
\t\t\treturn err
\t\t}
\t\tif observation.Spec.LeaseEpoch != latestMissionEpoch(leases, observation.Spec.MissionUID) {
\t\t\treturn fmt.Errorf("execution observation uses stale lease epoch")
\t\t}
\t\tlease := latestLeaseForAttemptAny(leases, observation.Spec.MissionUID, observation.Spec.PlanID, observation.Spec.Attempt)
\t\tif lease == nil || spaceexecution.ValidateObservationAgainstLease(&observation, lease, a.now()) != nil {
\t\t\treturn fmt.Errorf("execution observation does not match current trusted fence")
\t\t}
\tcase "spaceresultreceipts":
\t\tvar receipt spacev1.SpaceResultReceipt
\t\tif err := json.Unmarshal(object.Object, &receipt); err != nil {
\t\t\treturn err
\t\t}
\t\tleases, err := a.Store.ListExecutionLeases(ctx)
\t\tif err != nil {
\t\t\treturn err
\t\t}
\t\tif receipt.Spec.LeaseEpoch != latestMissionEpoch(leases, receipt.Spec.MissionUID) {
\t\t\treturn fmt.Errorf("result receipt uses stale lease epoch")
\t\t}
\t\tlease := latestLeaseForAttemptAny(leases, receipt.Spec.MissionUID, receipt.Spec.PlanID, receipt.Spec.Attempt)
\t\tif lease == nil || spaceexecution.ValidateResultAgainstLease(&receipt, lease, a.now()) != nil {
\t\t\treturn fmt.Errorf("result receipt does not match current trusted fence")
\t\t}
\tcase "spacetransferreceipts":
\tdefault:
\t\treturn fmt.Errorf("remote reporter resource %q is not allowed", object.Resource)
\t}
\treturn a.Store.UpsertRemoteReporterObject(ctx, object.Resource, object.Object)
}

func hasTransferReceipt(intent *spacev1.SpaceTransferIntent, receipts []*spacev1.SpaceTransferReceipt) bool {
\tif intent == nil {
\t\treturn false
\t}
\tfor _, receipt := range receipts {
\t\tif receipt == nil {
\t\t\tcontinue
\t\t}
\t\ts := receipt.Spec
\t\ti := intent.Spec
\t\tif s.TransferID == i.TransferID && s.MissionUID == i.MissionUID && s.PlanID == i.PlanID && s.Attempt == i.Attempt && s.Source == i.Source && s.Destination == i.Destination && s.DataID == i.DataID && s.Bytes == i.Bytes && s.PayloadDigest == i.PayloadDigest {
\t\t\treturn true
\t\t}
\t}
\treturn false
}

func uniqueDomains(values ...spacev1.DomainReference) []spacev1.DomainReference {
\tseen := map[string]struct{}{}
\tout := make([]spacev1.DomainReference, 0, len(values))
\tfor _, value := range values {
\t\tkey := strings.ToLower(string(value.OrbitClass) + "/" + value.ClusterID + "/" + value.Name)
\t\tif _, ok := seen[key]; ok || value.Name == "" || value.ClusterID == "" {
\t\t\tcontinue
\t\t}
\t\tseen[key] = struct{}{}
\t\tout = append(out, value)
\t}
\treturn out
}

func reporterEnvelopeID(resource, name string, destination spacev1.DomainReference) string {
\tsum := sha256.Sum256([]byte(resource + "|" + name + "|" + strings.ToLower(string(destination.OrbitClass)+"/"+destination.ClusterID+"/"+destination.Name)))
\treturn "reporter-" + hex.EncodeToString(sum[:20])
}

func assignmentForLease(lease *spacev1.SpaceExecutionLease, groups ...[]Assignment) *Assignment {
\tif lease == nil {
\t\treturn nil
\t}
\tf := lease.Spec.Fence
\tfor _, values := range groups {
\t\tfor i := range values {
\t\t\ta := &values[i]
\t\t\tif a.Mission != nil && a.Placement != nil && string(a.Mission.UID) == f.MissionUID && a.Placement.Spec.PlanID == f.PlanID && a.Placement.Spec.Attempt == f.Attempt {
\t\t\t\treturn a
\t\t\t}
\t\t}
\t}
\treturn nil
}

func terminalObservation(lease *spacev1.SpaceExecutionLease, observations []*spacev1.SpaceExecutionObservation, now time.Time) *spacev1.SpaceExecutionObservation {
\tvalid := make([]*spacev1.SpaceExecutionObservation, 0, len(observations))
\tfor _, observation := range observations {
\t\tif observation == nil || spaceexecution.ValidateObservationAgainstLease(observation, lease, now) != nil {
\t\t\tcontinue
\t\t}
\t\tswitch observation.Spec.Phase {
\t\tcase spacev1.ExecutionObservationStopped, spacev1.ExecutionObservationCompleted, spacev1.ExecutionObservationFailed:
\t\t\tvalid = append(valid, observation)
\t\t}
\t}
\tif len(valid) == 0 {
\t\treturn nil
\t}
\tsort.Slice(valid, func(i, j int) bool { return valid[i].Spec.ObservedAt.After(valid[j].Spec.ObservedAt.Time) })
\treturn valid[0]
}

func (a *Agent) fenceRemoteExecution(ctx context.Context, assignment Assignment, lease *spacev1.SpaceExecutionLease, reason string, observations []*spacev1.SpaceExecutionObservation) error {
\tif terminal := terminalObservation(lease, observations, a.now()); terminal != nil && terminal.Spec.Phase == spacev1.ExecutionObservationStopped {
\t\treturn nil
\t}
\texecutor, ok := a.Executor.(fenceExecutor)
\tif !ok {
\t\treturn fmt.Errorf("executor does not implement execution fencing")
\t}
\tif _, err := executor.FenceExecution(ctx, assignment.Mission, assignment.Placement, reason); err != nil {
\t\treturn err
\t}
\tid := fmt.Sprintf("stopped-%d-%d", lease.Spec.Fence.LeaseEpoch, a.now().UnixNano())
\tif len(id) > 63 {
\t\tid = id[:63]
\t}
\tobservation := &spacev1.SpaceExecutionObservation{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceExecutionObservation"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.ExecutionObservationName(a.Local, lease.Spec.Destination, lease.Spec.Fence.MissionUID, lease.Spec.Fence.PlanID, id)}, Spec: spacev1.SpaceExecutionObservationSpec{ObservationID: id, MissionUID: lease.Spec.Fence.MissionUID, PlanID: lease.Spec.Fence.PlanID, Attempt: lease.Spec.Fence.Attempt, LeaseEpoch: lease.Spec.Fence.LeaseEpoch, TokenHash: lease.Spec.Fence.TokenHash, Source: a.Local, Destination: lease.Spec.Destination, Phase: spacev1.ExecutionObservationStopped, ObservedAt: metav1.NewTime(a.now()), Provenance: a.baseProvenance(1)}}
\tif err := a.signObservation(observation); err != nil {
\t\treturn err
\t}
\tif err := a.Store.UpsertExecutionObservation(ctx, observation); err != nil {
\t\treturn err
\t}
\tif observation.Spec.Destination != a.Local {
\t\texpiry := a.now().Add(a.Limits.DiskRetention)
\t\tif assignment.Placement != nil && assignment.Placement.Spec.ExpiresAt.After(a.now()) {
\t\t\texpiry = assignment.Placement.Spec.ExpiresAt.Time
\t\t}
\t\treturn a.enqueueReporterObject(observation.Spec.Destination, "spaceexecutionobservations", observation, observation.Spec.MissionUID, observation.Spec.PlanID, observation.Spec.Attempt, 1, expiry)
\t}
\treturn nil
}
'''
write("contrib/space-compute/pkg/transport/hardening.go", hardening_go)

# ---- Command/runtime wiring -------------------------------------------------------
replace_once(
    "cmd/space-compute-domain-agent/main.go",
    'agent := &spacetransport.Agent{Local: cfg.LocalDomain, ReporterPrincipal: cfg.ReporterPrincipal, PrivateKey: privateKey, Queue: queue, Store: store, Executor: &kubeExecutor{client: client}, Assembler: &spacetransport.FileAssembler{Root: cfg.DataRoot, MaxBytes: 1 << 40}, DataRoot: cfg.DataRoot, LeaseTTL: time.Duration(cfg.LeaseTTLSeconds) * time.Second, MaxChunkBytes: cfg.MaxChunkBytes, Limits: limits}',
    'agent := &spacetransport.Agent{Local: cfg.LocalDomain, ReporterPrincipal: cfg.ReporterPrincipal, PrivateKey: privateKey, PeerKeys: peers, StateDir: cfg.StateDir, Queue: queue, Store: store, Executor: &kubeExecutor{client: client}, Assembler: &spacetransport.FileAssembler{Root: cfg.DataRoot, MaxBytes: 1 << 40}, DataRoot: cfg.DataRoot, LeaseTTL: time.Duration(cfg.LeaseTTLSeconds) * time.Second, MaxChunkBytes: cfg.MaxChunkBytes, Limits: limits}',
)
replace_once(
    "cmd/space-compute-domain-agent/main.go",
    'go func() { errCh <- serveReport(ctx, cfg.ReportAddress, agent) }()',
    'go func() { errCh <- serveReport(ctx, cfg.ReportAddress, spacetransport.ServerOnlyTLSConfig(cert), agent) }()',
)
replace_once(
    "cmd/space-compute-domain-agent/main.go",
    'func serveReport(ctx context.Context, address string, agent *spacetransport.Agent) error {',
    'func serveReport(ctx context.Context, address string, tlsConfig *tls.Config, agent *spacetransport.Agent) error {',
)
replace_once(
    "cmd/space-compute-domain-agent/main.go",
    '\treturn servePlain(ctx, address, mux)\n}\nfunc serveHealth',
    '\treturn serveEnvelope(ctx, address, tlsConfig, mux)\n}\nfunc serveHealth',
)
replace_once(
    "contrib/space-compute/pkg/transport/http.go",
    'func ServerTLSConfig(cert tls.Certificate, clientCAs *x509.CertPool) *tls.Config {\n\treturn &tls.Config{\n\t\tMinVersion:   tls.VersionTLS13,\n\t\tCertificates: []tls.Certificate{cert},\n\t\tClientAuth:   tls.RequireAndVerifyClientCert,\n\t\tClientCAs:    clientCAs,\n\t}\n}',
    'func ServerTLSConfig(cert tls.Certificate, clientCAs *x509.CertPool) *tls.Config {\n\treturn &tls.Config{\n\t\tMinVersion:   tls.VersionTLS13,\n\t\tCertificates: []tls.Certificate{cert},\n\t\tClientAuth:   tls.RequireAndVerifyClientCert,\n\t\tClientCAs:    clientCAs,\n\t}\n}\n\n// ServerOnlyTLSConfig protects the local execution report endpoint. The fence\n// token authenticates the workload report; cross-domain transport still always\n// uses ServerTLSConfig and mutual certificate authentication.\nfunc ServerOnlyTLSConfig(cert tls.Certificate) *tls.Config {\n\treturn &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}}\n}',
)

# The production executor can actively fence a remote Pod. Local deletion is not
# treated as proof by itself; the domain agent signs a Stopped observation only
# after this operation returns.
replace_once(
    "cmd/space-compute-domain-agent/store.go",
    '\t_, err = pods.Create(ctx, pod, metav1.CreateOptions{})\n\treturn err\n}\n\nvar _ spacetransport.AgentStore',
    '''\t_, err = pods.Create(ctx, pod, metav1.CreateOptions{})
\treturn err
}

func (e *kubeExecutor) FenceExecution(ctx context.Context, m *spacev1.SpaceMission, p *spacev1.SpacePlacementIntent, reason string) (bool, error) {
\tif m == nil || p == nil {
\t\treturn false, fmt.Errorf("mission and placement are required")
\t}
\tname := spaceworkload.AttemptPodName(m.Name, p.Spec.Attempt)
\tpods := e.client.CoreV1().Pods(m.Namespace)
\tcurrent, err := pods.Get(ctx, name, metav1.GetOptions{})
\tif apierrors.IsNotFound(err) {
\t\treturn false, nil
\t}
\tif err != nil {
\t\treturn false, err
\t}
\tif current.Labels[spacev1.LabelPlacementID] != p.Spec.PlanID {
\t\treturn false, fmt.Errorf("refusing to fence Pod owned by another plan")
\t}
\tzero := int64(0)
\tif err := pods.Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil && !apierrors.IsNotFound(err) {
\t\treturn false, fmt.Errorf("fence execution (%s): %w", reason, err)
\t}
\treturn true, nil
}

var _ spacetransport.AgentStore''',
)

print("stage5 hardening source patch applied")
