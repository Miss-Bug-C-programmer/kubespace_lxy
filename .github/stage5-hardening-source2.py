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


def replace_between(path, start, end, new):
    text = read(path)
    i = text.find(start)
    if i < 0:
        raise SystemExit(f"{path}: start marker missing {start!r}")
    j = text.find(end, i + len(start))
    if j < 0:
        raise SystemExit(f"{path}: end marker missing {end!r}")
    write(path, text[:i] + new.rstrip() + "\n\n" + text[j:])


# Execution-lease clock skew is a fencing safety parameter and is intentionally
# independent from the wider transport-envelope timestamp tolerance.
replace_once(
    "cmd/space-compute-domain-agent/config.go",
    '\tLeaseTTLSeconds         int64                   `json:"leaseTTLSeconds"`\n\tMaxChunkBytes',
    '\tLeaseTTLSeconds         int64                   `json:"leaseTTLSeconds"`\n\tLeaseClockSkewSeconds   int64                   `json:"leaseClockSkewSeconds"`\n\tMaxChunkBytes',
)
replace_once(
    "cmd/space-compute-domain-agent/config.go",
    '\tif cfg.LeaseTTLSeconds == 0 {\n\t\tcfg.LeaseTTLSeconds = 120\n\t}\n',
    '\tif cfg.LeaseTTLSeconds == 0 {\n\t\tcfg.LeaseTTLSeconds = 120\n\t}\n\tif cfg.LeaseClockSkewSeconds == 0 {\n\t\tcfg.LeaseClockSkewSeconds = 2\n\t}\n',
)
replace_once(
    "cmd/space-compute-domain-agent/config.go",
    '\tif len(cfg.Peers) > 64 {\n\t\treturn cfg, fmt.Errorf("peer count exceeds 64")\n\t}\n',
    '\tif len(cfg.Peers) > 64 {\n\t\treturn cfg, fmt.Errorf("peer count exceeds 64")\n\t}\n\tif cfg.LeaseClockSkewSeconds < 0 || cfg.LeaseClockSkewSeconds > 30 || 4*cfg.LeaseClockSkewSeconds >= cfg.LeaseTTLSeconds {\n\t\treturn cfg, fmt.Errorf("leaseClockSkewSeconds must be 0..30 and strictly less than one quarter of leaseTTLSeconds")\n\t}\n',
)

replace_once(
    "contrib/space-compute/pkg/transport/agent.go",
    '\tLeaseTTL          time.Duration\n\tMaxChunkBytes',
    '\tLeaseTTL          time.Duration\n\tLeaseClockSkew    time.Duration\n\tMaxChunkBytes',
)
replace_once(
    "contrib/space-compute/pkg/transport/agent.go",
    '\tif a.LeaseTTL < 30*time.Second || a.LeaseTTL > 24*time.Hour {\n\t\treturn fmt.Errorf("lease TTL out of bounds")\n\t}\n',
    '\tif a.LeaseTTL < 30*time.Second || a.LeaseTTL > 24*time.Hour {\n\t\treturn fmt.Errorf("lease TTL out of bounds")\n\t}\n\tif a.LeaseClockSkew < 0 || a.LeaseClockSkew > 30*time.Second || 4*a.LeaseClockSkew >= a.LeaseTTL {\n\t\treturn fmt.Errorf("lease clock skew must be non-negative, at most 30s and below one quarter of lease TTL")\n\t}\n',
)
replace_once(
    "cmd/space-compute-domain-agent/main.go",
    'LeaseTTL: time.Duration(cfg.LeaseTTLSeconds) * time.Second, MaxChunkBytes:',
    'LeaseTTL: time.Duration(cfg.LeaseTTLSeconds) * time.Second, LeaseClockSkew: time.Duration(cfg.LeaseClockSkewSeconds) * time.Second, MaxChunkBytes:',
)
replace_once(
    "contrib/space-compute/pkg/transport/agent.go",
    'MaximumClockSkewSeconds: int64(a.Limits.MaximumClockSkew / time.Second)',
    'MaximumClockSkewSeconds: int64(a.LeaseClockSkew / time.Second)',
)

# Route desired transfer state to both participating domains. The wire copy has
# Kubernetes server metadata stripped so it can be safely upserted into a peer API.
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
\t\tif intent.Spec.Purpose == spacev1.TransferPurposeInput && intent.Spec.Coordinator == a.Local {
\t\t\tfor _, destination := range uniqueDomains(intent.Spec.Source, intent.Spec.Destination) {
\t\t\t\tif destination == a.Local {
\t\t\t\t\tcontinue
\t\t\t\t}
\t\t\t\tif err := a.enqueueTransferIntent(intent, destination); err != nil {
\t\t\t\t\treturn err
\t\t\t\t}
\t\t\t}
\t\t}
\t\tif intent.Spec.Purpose == spacev1.TransferPurposeResult && intent.Spec.Source == a.Local && intent.Spec.Destination != a.Local {
\t\t\tif err := a.enqueueTransferIntent(intent, intent.Spec.Destination); err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t}
\t\tif intent.Spec.Source != a.Local {
\t\t\tcontinue
\t\t}
\t\t// Planner transfer windows are already clock-skew/safety-margin adjusted.
\t\t// Do not turn NotBefore or wall-clock arrival into an assumption of success.
\t\tif now.Before(intent.Spec.Window.Start.Time) || !intent.Spec.Window.End.After(now) {
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

# Receiver requires the durable transfer intent before accepting any payload byte.
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
\t\tif (a.Local != intent.Spec.Source && a.Local != intent.Spec.Destination) || intent.Spec.MissionUID != e.MissionUID || intent.Spec.PlanID != e.PlanID || intent.Spec.Attempt != e.Attempt {
\t\t\treturn fmt.Errorf("transfer intent envelope metadata mismatch")
\t\t}
\t\tauthorized := e.Source == intent.Spec.Coordinator
\t\tif intent.Spec.Purpose == spacev1.TransferPurposeResult && e.Source == intent.Spec.Source {
\t\t\tauthorized = true
\t\t}
\t\tif !authorized {
\t\t\treturn fmt.Errorf("transfer intent sender is not coordinator/source authority")
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
\t\tintent, err := a.Store.GetTransferIntent(ctx, chunk.IntentName)
\t\tif err != nil {
\t\t\treturn fmt.Errorf("transfer intent is not durable at receiver: %w", err)
\t\t}
\t\tif err := validateChunkAgainstIntent(&chunk, intent, a.Local, a.now()); err != nil {
\t\t\treturn err
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

# Reports are made crash/retry safe: result desired state is persisted first,
# outbound terminal observation is queued durably second, local observation last.
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
\tlease := latestLeaseForAttemptAny(leases, report.MissionUID, report.PlanID, report.Attempt)
\tif lease == nil || lease.Spec.Fence.LeaseEpoch != report.LeaseEpoch {
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
\tif existing := matchingReportObservation(report, lease, observations); existing != nil {
\t\tif err := a.ensureResultReturn(ctx, *assignment, lease, report); err != nil {
\t\t\treturn err
\t\t}
\t\tif existing.Spec.Destination != a.Local {
\t\t\treturn a.enqueueReporterObject(existing.Spec.Destination, "spaceexecutionobservations", existing, report.MissionUID, report.PlanID, report.Attempt, existing.Spec.Provenance.Sequence, assignment.Placement.Spec.ExpiresAt.Time)
\t\t}
\t\treturn nil
\t}
\tif terminalObservation(lease, observations, a.now()) != nil {
\t\treturn fmt.Errorf("execution fence is terminal; further token reports are rejected")
\t}
\tif err := a.ensureResultReturn(ctx, *assignment, lease, report); err != nil {
\t\treturn err
\t}
\tid := reportObservationID(report, a.now())
\tobs := &spacev1.SpaceExecutionObservation{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceExecutionObservation"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.ExecutionObservationName(a.Local, lease.Spec.Destination, report.MissionUID, report.PlanID, id)}, Spec: spacev1.SpaceExecutionObservationSpec{ObservationID: id, MissionUID: report.MissionUID, PlanID: report.PlanID, Attempt: report.Attempt, LeaseEpoch: report.LeaseEpoch, TokenHash: lease.Spec.Fence.TokenHash, Source: a.Local, Destination: lease.Spec.Destination, Phase: report.Phase, CheckpointID: report.CheckpointID, ObservedAt: metav1.NewTime(a.now()), Provenance: a.baseProvenance(1)}}
\tif err := a.signObservation(obs); err != nil {
\t\treturn err
\t}
\tif obs.Spec.Destination != a.Local {
\t\tif err := a.enqueueReporterObject(obs.Spec.Destination, "spaceexecutionobservations", obs, report.MissionUID, report.PlanID, report.Attempt, 1, assignment.Placement.Spec.ExpiresAt.Time); err != nil {
\t\t\treturn err
\t\t}
\t}
\treturn a.Store.UpsertExecutionObservation(ctx, obs)
}''',
)

# Consume only the newest signed remote terminal observation for the current fence.
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
\tif placement.Status.Phase == spacev1.PlacementReplanning && mission.Spec.Checkpoint.Checkpointable {
\t\tvar checkpoint *spacev1.SpaceExecutionObservation
\t\tfor _, o := range observations {
\t\t\tif o == nil || o.Spec.Phase != spacev1.ExecutionObservationCheckpointed || spaceexecution.ValidateObservationAgainstLease(o, lease, now) != nil {
\t\t\t\tcontinue
\t\t\t}
\t\t\tif checkpoint == nil || o.Spec.ObservedAt.After(checkpoint.Spec.ObservedAt.Time) {
\t\t\t\tcheckpoint = o
\t\t\t}
\t\t}
\t\tif checkpoint != nil {
\t\t\tobs := spacev1.ExecutionObservation{Sequence: placement.Status.LastObservationSequence + 1, Attempt: placement.Spec.Attempt, PodUID: remoteFenceUID(lease), Phase: "checkpointed", ObservedAt: checkpoint.Spec.ObservedAt, CheckpointID: checkpoint.Spec.CheckpointID}
\t\t\tchanged, err := planner.ApplyExecutionObservation(placement, mission, obs, c.clock())
\t\t\tif err != nil {
\t\t\t\treturn false, err
\t\t\t}
\t\t\tif changed {
\t\t\t\treturn true, c.Store.UpdatePlacementStatus(ctx, placement)
\t\t\t}
\t\t}
\t}
\tif placement.Status.Phase != spacev1.PlacementReturnPending && placement.Status.Phase != spacev1.PlacementCompleted && placement.Status.Phase != spacev1.PlacementFailed {
\t\tvar terminal *spacev1.SpaceExecutionObservation
\t\tfor _, o := range observations {
\t\t\tif o == nil || (o.Spec.Phase != spacev1.ExecutionObservationCompleted && o.Spec.Phase != spacev1.ExecutionObservationFailed) || spaceexecution.ValidateObservationAgainstLease(o, lease, now) != nil {
\t\t\t\tcontinue
\t\t\t}
\t\t\tif terminal == nil || o.Spec.ObservedAt.After(terminal.Spec.ObservedAt.Time) {
\t\t\t\tterminal = o
\t\t\t}
\t\t}
\t\tif terminal != nil {
\t\t\tphase := "failed"
\t\t\tif terminal.Spec.Phase == spacev1.ExecutionObservationCompleted {
\t\t\t\tphase = "completed"
\t\t\t\tif mission.Spec.ResultReturnRequired {
\t\t\t\t\tphase = "return-pending"
\t\t\t\t}
\t\t\t}
\t\t\tobs := spacev1.ExecutionObservation{Sequence: placement.Status.LastObservationSequence + 1, Attempt: placement.Spec.Attempt, PodUID: remoteFenceUID(lease), Phase: phase, ObservedAt: terminal.Spec.ObservedAt}
\t\t\tchanged, err := planner.ApplyExecutionObservation(placement, mission, obs, c.clock())
\t\t\tif err != nil {
\t\t\t\treturn false, err
\t\t\t}
\t\t\tif changed {
\t\t\t\tif err := c.Store.UpdatePlacementStatus(ctx, placement); err != nil {
\t\t\t\t\treturn false, err
\t\t\t\t}
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
\t\t\t\treturn true, c.Store.UpdatePlacementStatus(ctx, placement)
\t\t\t}
\t\t}
\t}
\treturn false, nil
}

func remoteFenceUID(lease *spacev1.SpaceExecutionLease) string {
\tif lease == nil {
\t\treturn "remote"
\t}
\treturn fmt.Sprintf("remote-%s-%d", lease.Spec.Source.Name, lease.Spec.Fence.LeaseEpoch)
}''',
)

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
\t"k8s.io/apimachinery/pkg/types"

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

func (a *Agent) enqueueTransferIntent(intent *spacev1.SpaceTransferIntent, destination spacev1.DomainReference) error {
\tcopy := intent.DeepCopy()
\tcopy.ResourceVersion = ""
\tcopy.UID = types.UID("")
\tcopy.Generation = 0
\tcopy.CreationTimestamp = metav1.Time{}
\tcopy.DeletionTimestamp = nil
\tcopy.DeletionGracePeriodSeconds = nil
\tcopy.ManagedFields = nil
\tcopy.OwnerReferences = nil
\tcopy.Finalizers = nil
\traw, err := json.Marshal(copy)
\tif err != nil {
\t\treturn err
\t}
\tid := transferIntentEnvelopeID(copy.Name, destination)
\te := NewEnvelope(id, TransferIntentKind, a.Local, destination, copy.Spec.MissionUID, copy.Spec.PlanID, copy.Spec.Attempt, 1, a.now(), copy.Spec.ExpiresAt.Time, raw)
\tif err := e.Sign(a.PrivateKey); err != nil {
\t\treturn err
\t}
\treturn a.Queue.Enqueue(e)
}

func transferIntentEnvelopeID(name string, destination spacev1.DomainReference) string {
\tsum := sha256.Sum256([]byte("transfer-intent|" + name + "|" + strings.ToLower(string(destination.OrbitClass)+"/"+destination.ClusterID+"/"+destination.Name)))
\treturn "transfer-intent-" + hex.EncodeToString(sum[:20])
}

func validateChunkAgainstIntent(chunk *TransferChunk, intent *spacev1.SpaceTransferIntent, local spacev1.DomainReference, now time.Time) error {
\tif chunk == nil || intent == nil {
\t\treturn fmt.Errorf("chunk and transfer intent are required")
\t}
\ti := intent.Spec
\tif i.Destination != local || i.TransferID != chunk.TransferID || i.MissionUID != chunk.MissionUID || i.PlanID != chunk.PlanID || i.Attempt != chunk.Attempt || i.Purpose != chunk.Purpose || i.Source != chunk.Source || i.Destination != chunk.Destination || i.DataID != chunk.DataID || i.Bytes != chunk.TotalBytes || i.PayloadDigest != chunk.PayloadDigest || i.LeaseEpoch != chunk.LeaseEpoch || i.TokenHash != chunk.TokenHash {
\t\treturn fmt.Errorf("transfer chunk does not match durable transfer intent")
\t}
\tif now.Before(i.Window.Start.Time) || !i.Window.End.After(now) || !i.ExpiresAt.After(now) {
\t\treturn fmt.Errorf("transfer chunk is outside the trusted transfer window")
\t}
\tif chunk.StartedAt.Before(i.Window.Start.Time) || chunk.StartedAt.After(now.Add(time.Second)) {
\t\treturn fmt.Errorf("transfer chunk start time is outside trusted bounds")
\t}
\treturn nil
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

func matchingReportObservation(report spaceexecution.Report, lease *spacev1.SpaceExecutionLease, observations []*spacev1.SpaceExecutionObservation) *spacev1.SpaceExecutionObservation {
\tfor _, observation := range observations {
\t\tif observation == nil {
\t\t\tcontinue
\t\t}
\t\ts := observation.Spec
\t\tif s.MissionUID == report.MissionUID && s.PlanID == report.PlanID && s.Attempt == report.Attempt && s.LeaseEpoch == report.LeaseEpoch && s.TokenHash == lease.Spec.Fence.TokenHash && s.Phase == report.Phase && s.CheckpointID == report.CheckpointID {
\t\t\treturn observation
\t\t}
\t}
\treturn nil
}

func reportObservationID(report spaceexecution.Report, now time.Time) string {
\tphase := strings.ToLower(string(report.Phase))
\tif report.Phase == spacev1.ExecutionObservationHeartbeat {
\t\treturn fmt.Sprintf("heartbeat-%d-%d", report.LeaseEpoch, now.UnixNano())
\t}
\textra := report.CheckpointID
\tif extra != "" {
\t\tsum := sha256.Sum256([]byte(extra))
\t\textra = "-" + hex.EncodeToString(sum[:4])
\t}
\tid := fmt.Sprintf("report-%s-%d%s", phase, report.LeaseEpoch, extra)
\tif len(id) > 63 {
\t\tid = id[:63]
\t}
\treturn id
}

func (a *Agent) ensureResultReturn(ctx context.Context, assignment Assignment, lease *spacev1.SpaceExecutionLease, report spaceexecution.Report) error {
\tif report.Phase != spacev1.ExecutionObservationCompleted || assignment.Mission == nil || !assignment.Mission.Spec.ResultReturnRequired {
\t\treturn nil
\t}
\tif report.ResultDataID == "" {
\t\treturn fmt.Errorf("completed execution requires resultDataID when result return is required")
\t}
\tif assignment.Placement == nil || assignment.Placement.Spec.ResultTransfer == nil {
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
\tdigestBytes := sha256.Sum256(raw)
\tdigest := hex.EncodeToString(digestBytes[:])
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
}

func (a *Agent) fenceRemoteExecution(ctx context.Context, assignment Assignment, lease *spacev1.SpaceExecutionLease, reason string, observations []*spacev1.SpaceExecutionObservation) error {
\tif terminal := terminalObservation(lease, observations, a.now()); terminal != nil && terminal.Spec.Phase == spacev1.ExecutionObservationStopped {
\t\treturn nil
\t}
\texecutor, ok := a.Executor.(fenceExecutor)
\tif !ok {
\t\treturn fmt.Errorf("executor does not implement execution fencing")
\t}
\tdeleted, err := executor.FenceExecution(ctx, assignment.Mission, assignment.Placement, reason)
\tif err != nil {
\t\treturn err
\t}
\tif !deleted {
\t\treturn nil
\t}
\tid := fmt.Sprintf("stopped-%d-%d", lease.Spec.Fence.LeaseEpoch, a.now().UnixNano())
\tif len(id) > 63 {
\t\tid = id[:63]
\t}
\tobservation := &spacev1.SpaceExecutionObservation{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceExecutionObservation"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.ExecutionObservationName(a.Local, lease.Spec.Destination, lease.Spec.Fence.MissionUID, lease.Spec.Fence.PlanID, id)}, Spec: spacev1.SpaceExecutionObservationSpec{ObservationID: id, MissionUID: lease.Spec.Fence.MissionUID, PlanID: lease.Spec.Fence.PlanID, Attempt: lease.Spec.Fence.Attempt, LeaseEpoch: lease.Spec.Fence.LeaseEpoch, TokenHash: lease.Spec.Fence.TokenHash, Source: a.Local, Destination: lease.Spec.Destination, Phase: spacev1.ExecutionObservationStopped, ObservedAt: metav1.NewTime(a.now()), Provenance: a.baseProvenance(1)}}
\tif err := a.signObservation(observation); err != nil {
\t\treturn err
\t}
\tif observation.Spec.Destination != a.Local {
\t\texpiry := a.now().Add(a.Limits.DiskRetention)
\t\tif assignment.Placement != nil && assignment.Placement.Spec.ExpiresAt.After(a.now()) {
\t\t\texpiry = assignment.Placement.Spec.ExpiresAt.Time
\t\t}
\t\tif err := a.enqueueReporterObject(observation.Spec.Destination, "spaceexecutionobservations", observation, observation.Spec.MissionUID, observation.Spec.PlanID, observation.Spec.Attempt, 1, expiry); err != nil {
\t\t\treturn err
\t\t}
\t}
\treturn a.Store.UpsertExecutionObservation(ctx, observation)
}
'''
write("contrib/space-compute/pkg/transport/hardening.go", hardening_go)

print("stage5 protocol edge hardening applied")
