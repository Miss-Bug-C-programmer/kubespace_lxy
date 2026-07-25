#!/usr/bin/env python3
from pathlib import Path


def replace_once(path, old, new):
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: marker count {count} for {old[:100]!r}")
    p.write_text(text.replace(old, new, 1))


workload_test = "contrib/space-compute/pkg/workload/controller_test.go"
replace_once(
    workload_test,
    'controller := &Controller{Store: store, Evidence: evidence, Clock: &mutableClock{now: now}}\n',
    'coordinator := source\n\tcontroller := &Controller{Store: store, Evidence: evidence, Clock: &mutableClock{now: now}, LocalDomain: &coordinator}\n',
)

replace_once(
    workload_test,
    '''\tmission.Spec.Inputs = []spacev1.DataObject{{ID: "sensor", SizeBytes: 1, Locations: []string{"ground-a"}}}\n\tplacement.Spec.InputTransfers = []spacev1.TransferEpoch{{DataID: "sensor", Source: spacev1.DomainReference{Name: "ground-a", ClusterID: "g", OrbitClass: spacev1.OrbitGround}, Destination: placement.Spec.Target, Start: metav1.NewTime(now), End: metav1.NewTime(now.Add(time.Minute)), Bytes: 1}}\n\t_, err := (&Controller{Store: &memoryStore{}, Evidence: newMemoryEvidence(), Clock: &mutableClock{now: now}}).ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate)\n''',
    '''\tmission.Spec.Inputs = []spacev1.DataObject{{ID: "sensor", SizeBytes: 1, Locations: []string{"ground-a"}}}\n\tsource := spacev1.DomainReference{Name: "ground-a", ClusterID: "g", OrbitClass: spacev1.OrbitGround}\n\tplacement.Spec.InputTransfers = []spacev1.TransferEpoch{{DataID: "sensor", Source: source, Destination: placement.Spec.Target, Start: metav1.NewTime(now), End: metav1.NewTime(now.Add(time.Minute)), Bytes: 1}}\n\tcoordinator := spacev1.DomainReference{Name: "ground-control", ClusterID: "ground-control", OrbitClass: spacev1.OrbitGround}\n\t_, err := (&Controller{Store: &memoryStore{}, Evidence: newMemoryEvidence(), Clock: &mutableClock{now: now}, LocalDomain: &coordinator}).ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate)\n''',
)

replace_once(
    workload_test,
    '''func validResultReceipt(m *spacev1.SpaceMission, p *spacev1.SpacePlacementIntent, lease *spacev1.SpaceExecutionLease, at time.Time) *spacev1.SpaceResultReceipt {\n\tr := &spacev1.SpaceResultReceipt{Spec: spacev1.SpaceResultReceiptSpec{ResultID: "result-one", MissionUID: string(m.UID), PlanID: p.Spec.PlanID, Attempt: p.Spec.Attempt, Source: lease.Spec.Source, Destination: lease.Spec.Destination, Bytes: 1, PayloadDigest: strings.Repeat("d", 64), LeaseEpoch: lease.Spec.Fence.LeaseEpoch, TokenHash: lease.Spec.Fence.TokenHash, CompletedAt: metav1.NewTime(at), Provenance: testProvenance(1)}}\n\tr.Name = spacev1.ResultReceiptName(r.Spec.Source, r.Spec.Destination, r.Spec.MissionUID, r.Spec.PlanID, r.Spec.ResultID)\n\treturn r\n}\n''',
    '''func validResultReceipt(m *spacev1.SpaceMission, p *spacev1.SpacePlacementIntent, lease *spacev1.SpaceExecutionLease, at time.Time) *spacev1.SpaceResultReceipt {\n\tdestination := lease.Spec.Destination\n\tif p.Spec.ResultTransfer != nil {\n\t\tdestination = p.Spec.ResultTransfer.Destination\n\t}\n\tr := &spacev1.SpaceResultReceipt{Spec: spacev1.SpaceResultReceiptSpec{ResultID: "result-one", MissionUID: string(m.UID), PlanID: p.Spec.PlanID, Attempt: p.Spec.Attempt, Source: lease.Spec.Source, Destination: destination, Bytes: 1, PayloadDigest: strings.Repeat("d", 64), LeaseEpoch: lease.Spec.Fence.LeaseEpoch, TokenHash: lease.Spec.Fence.TokenHash, CompletedAt: metav1.NewTime(at), Provenance: testProvenance(1)}}\n\tr.Name = spacev1.ResultReceiptName(r.Spec.Source, r.Spec.Destination, r.Spec.MissionUID, r.Spec.PlanID, r.Spec.ResultID)\n\treturn r\n}\n''',
)

insert_anchor = 'func TestDeterministicAttemptFenceRejectsDifferentPlan(t *testing.T) {'
text = Path(workload_test).read_text()
if text.count(insert_anchor) != 1:
    raise SystemExit("workload missing-coordinator test anchor mismatch")
missing_test = r'''func TestMissingTransferCoordinatorStaysPendingWithoutPod(t *testing.T) {
\tnow := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
\tmission, placement := dispatchFixture(now)
\tdigest := strings.Repeat("a", 64)
\tsource := spacev1.DomainReference{Name: "ground-a", ClusterID: "ground", OrbitClass: spacev1.OrbitGround}
\tmission.Spec.Inputs = []spacev1.DataObject{{ID: "sensor", SizeBytes: 1, Locations: []string{"ground-a"}, PayloadDigest: digest}}
\tplacement.Spec.InputTransfers = []spacev1.TransferEpoch{{DataID: "sensor", Source: source, Destination: placement.Spec.Target, Start: metav1.NewTime(now), End: metav1.NewTime(now.Add(time.Minute)), Bytes: 1}}
\tplacement.Spec.NotBefore = metav1.NewTime(now)
\tplacement.Spec.ComputeStart = metav1.NewTime(now)
\tstore := &memoryStore{}
\tevidence := newMemoryEvidence()
\tc := &Controller{Store: store, Evidence: evidence, Clock: &mutableClock{now: now}}
\tif _, err := c.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate); err != nil {
\t\tt.Fatalf("missing coordinator should wait fail-closed: %v", err)
\t}
\tif placement.Status.Phase != spacev1.PlacementTransferPending || store.creates != 0 || len(evidence.intents) != 0 {
\t\tt.Fatalf("phase=%s creates=%d intents=%d", placement.Status.Phase, store.creates, len(evidence.intents))
\t}
}

'''
text = text.replace(insert_anchor, missing_test + insert_anchor, 1)
Path(workload_test).write_text(text)

transport_test = r'''package transport

import (
\t"strings"
\t"testing"
\t"time"

\tmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

\tspacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

func hardeningLease(now time.Time, attempt int32, epoch int64, tokenByte string) *spacev1.SpaceExecutionLease {
\tsrc, dst := domains()
\tf := spacev1.ExecutionFence{MissionUID: "mission-uid", PlanID: "plan-one", Attempt: attempt, LeaseEpoch: epoch, TokenHash: strings.Repeat(tokenByte, 64), ExpiresAt: metav1.NewTime(now.Add(5 * time.Minute))}
\treturn &spacev1.SpaceExecutionLease{ObjectMeta: metav1.ObjectMeta{Name: spacev1.ExecutionLeaseName(f.MissionUID, f.PlanID, f.Attempt, f.LeaseEpoch)}, Spec: spacev1.SpaceExecutionLeaseSpec{Source: src, Destination: dst, Fence: f, HeartbeatAt: metav1.NewTime(now), MaximumClockSkewSeconds: 1, Provenance: spacev1.Provenance{ReporterID: "reporter", Source: "agent", Digest: strings.Repeat("a", 64), Sequence: 1}}}
}

func TestIncomingLeaseEpochIsMonotonic(t *testing.T) {
\tnow := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
\told := hardeningLease(now, 1, 1, "b")
\tnext := hardeningLease(now.Add(time.Second), 2, 2, "c")
\tif err := validateIncomingLease([]*spacev1.SpaceExecutionLease{old}, next, now.Add(time.Second)); err != nil {
\t\tt.Fatalf("higher epoch rejected: %v", err)
\t}
\tif err := validateIncomingLease([]*spacev1.SpaceExecutionLease{next}, old, now.Add(time.Second)); err == nil {
\t\tt.Fatal("stale lower lease epoch accepted")
\t}
\tconflict := next.DeepCopy()
\tconflict.Spec.Fence.TokenHash = strings.Repeat("d", 64)
\tconflict.Spec.Provenance.Sequence = 2
\tconflict.Spec.Provenance.Digest = strings.Repeat("e", 64)
\tconflict.Spec.HeartbeatAt = metav1.NewTime(now.Add(2 * time.Second))
\tconflict.Spec.Fence.ExpiresAt = metav1.NewTime(now.Add(6 * time.Minute))
\tif err := validateIncomingLease([]*spacev1.SpaceExecutionLease{next}, conflict, now.Add(2*time.Second)); err == nil {
\t\tt.Fatal("same epoch with changed token accepted")
\t}
}

func TestLeaseConfirmationPersistsExactSequenceAndDigest(t *testing.T) {
\tnow := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
\tlease := hardeningLease(now, 1, 1, "b")
\ta := &Agent{StateDir: t.TempDir()}
\tif err := a.markLeaseConfirmed(lease); err != nil {
\t\tt.Fatal(err)
\t}
\tconfirmed, err := a.leaseConfirmed(lease)
\tif err != nil || !confirmed {
\t\tt.Fatalf("persisted confirmation confirmed=%v err=%v", confirmed, err)
\t}
\tadvanced := lease.DeepCopy()
\tadvanced.Spec.Provenance.Sequence++
\tadvanced.Spec.Provenance.Digest = strings.Repeat("f", 64)
\tconfirmed, err = a.leaseConfirmed(advanced)
\tif err != nil || confirmed {
\t\tt.Fatalf("unacknowledged renewal confirmed=%v err=%v", confirmed, err)
\t}
}

func TestChunkMustMatchDurableIntentExactly(t *testing.T) {
\tnow := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
\tsrc, dst := domains()
\tintent := &spacev1.SpaceTransferIntent{Spec: spacev1.SpaceTransferIntentSpec{TransferID: "transfer-one", MissionUID: "mission-uid", PlanID: "plan-one", Attempt: 1, Purpose: spacev1.TransferPurposeInput, Coordinator: src, Source: src, Destination: dst, DataID: "sensor", Bytes: 4, PayloadDigest: strings.Repeat("a", 64), Window: spacev1.TransferEpoch{DataID: "sensor", Source: src, Destination: dst, Start: metav1.NewTime(now.Add(-time.Minute)), End: metav1.NewTime(now.Add(time.Minute)), Bytes: 4}, ExpiresAt: metav1.NewTime(now.Add(2 * time.Minute))}}
\tchunk := &TransferChunk{IntentName: "intent", TransferID: intent.Spec.TransferID, MissionUID: intent.Spec.MissionUID, PlanID: intent.Spec.PlanID, Attempt: intent.Spec.Attempt, Purpose: intent.Spec.Purpose, Source: src, Destination: dst, DataID: "sensor", TotalBytes: 4, PayloadDigest: intent.Spec.PayloadDigest, ChunkIndex: 0, ChunkCount: 1, StartedAt: now, Data: []byte("data")}
\tif err := validateChunkAgainstIntent(chunk, intent, dst, now); err != nil {
\t\tt.Fatalf("matching chunk rejected: %v", err)
\t}
\tchunk.PayloadDigest = strings.Repeat("b", 64)
\tif err := validateChunkAgainstIntent(chunk, intent, dst, now); err == nil {
\t\tt.Fatal("chunk not matching durable intent accepted")
\t}
}
'''
Path("contrib/space-compute/pkg/transport/hardening_test.go").write_text(transport_test)
print("stage5 hardening regression tests staged")
