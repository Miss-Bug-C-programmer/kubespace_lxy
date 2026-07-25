package transport

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

func hardeningLease(now time.Time, attempt int32, epoch int64, tokenByte string) *spacev1.SpaceExecutionLease {
	src, dst := domains()
	f := spacev1.ExecutionFence{MissionUID: "mission-uid", PlanID: "plan-one", Attempt: attempt, LeaseEpoch: epoch, TokenHash: strings.Repeat(tokenByte, 64), ExpiresAt: metav1.NewTime(now.Add(5 * time.Minute))}
	return &spacev1.SpaceExecutionLease{ObjectMeta: metav1.ObjectMeta{Name: spacev1.ExecutionLeaseName(f.MissionUID, f.PlanID, f.Attempt, f.LeaseEpoch)}, Spec: spacev1.SpaceExecutionLeaseSpec{Source: src, Destination: dst, Fence: f, HeartbeatAt: metav1.NewTime(now), MaximumClockSkewSeconds: 1, Provenance: spacev1.Provenance{ReporterID: "reporter", Source: "agent", Digest: strings.Repeat("a", 64), Sequence: 1}}}
}

func TestIncomingLeaseEpochIsMonotonic(t *testing.T) {
	now := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
	old := hardeningLease(now, 1, 1, "b")
	next := hardeningLease(now.Add(time.Second), 2, 2, "c")
	if err := validateIncomingLease([]*spacev1.SpaceExecutionLease{old}, next, now.Add(time.Second)); err != nil {
		t.Fatalf("higher epoch rejected: %v", err)
	}
	if err := validateIncomingLease([]*spacev1.SpaceExecutionLease{next}, old, now.Add(time.Second)); err == nil {
		t.Fatal("stale lower lease epoch accepted")
	}
	conflict := next.DeepCopy()
	conflict.Spec.Fence.TokenHash = strings.Repeat("d", 64)
	conflict.Spec.Provenance.Sequence = 2
	conflict.Spec.Provenance.Digest = strings.Repeat("e", 64)
	conflict.Spec.HeartbeatAt = metav1.NewTime(now.Add(2 * time.Second))
	conflict.Spec.Fence.ExpiresAt = metav1.NewTime(now.Add(6 * time.Minute))
	if err := validateIncomingLease([]*spacev1.SpaceExecutionLease{next}, conflict, now.Add(2*time.Second)); err == nil {
		t.Fatal("same epoch with changed token accepted")
	}
}

func TestLeaseConfirmationPersistsExactSequenceAndDigest(t *testing.T) {
	now := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
	lease := hardeningLease(now, 1, 1, "b")
	a := &Agent{StateDir: t.TempDir()}
	if err := a.markLeaseConfirmed(lease); err != nil {
		t.Fatal(err)
	}
	confirmed, err := a.leaseConfirmed(lease)
	if err != nil || !confirmed {
		t.Fatalf("persisted confirmation confirmed=%v err=%v", confirmed, err)
	}
	advanced := lease.DeepCopy()
	advanced.Spec.Provenance.Sequence++
	advanced.Spec.Provenance.Digest = strings.Repeat("f", 64)
	confirmed, err = a.leaseConfirmed(advanced)
	if err != nil || confirmed {
		t.Fatalf("unacknowledged renewal confirmed=%v err=%v", confirmed, err)
	}
}

func TestChunkMustMatchDurableIntentExactly(t *testing.T) {
	now := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
	src, dst := domains()
	intent := &spacev1.SpaceTransferIntent{Spec: spacev1.SpaceTransferIntentSpec{TransferID: "transfer-one", MissionUID: "mission-uid", PlanID: "plan-one", Attempt: 1, Purpose: spacev1.TransferPurposeInput, Coordinator: src, Source: src, Destination: dst, DataID: "sensor", Bytes: 4, PayloadDigest: strings.Repeat("a", 64), Window: spacev1.TransferEpoch{DataID: "sensor", Source: src, Destination: dst, Start: metav1.NewTime(now.Add(-time.Minute)), End: metav1.NewTime(now.Add(time.Minute)), Bytes: 4}, ExpiresAt: metav1.NewTime(now.Add(2 * time.Minute))}}
	chunk := &TransferChunk{IntentName: "intent", TransferID: intent.Spec.TransferID, MissionUID: intent.Spec.MissionUID, PlanID: intent.Spec.PlanID, Attempt: intent.Spec.Attempt, Purpose: intent.Spec.Purpose, Source: src, Destination: dst, DataID: "sensor", TotalBytes: 4, PayloadDigest: intent.Spec.PayloadDigest, ChunkIndex: 0, ChunkCount: 1, StartedAt: now, Data: []byte("data")}
	if err := validateChunkAgainstIntent(chunk, intent, dst, now); err != nil {
		t.Fatalf("matching chunk rejected: %v", err)
	}
	chunk.PayloadDigest = strings.Repeat("b", 64)
	if err := validateChunkAgainstIntent(chunk, intent, dst, now); err == nil {
		t.Fatal("chunk not matching durable intent accepted")
	}
}
