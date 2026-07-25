package execution

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

func TestLeaseAdvanceRejectsOldEpochAndTokenReuse(t *testing.T) {
	now := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
	m, p, l := fenceFixture(now)
	same := l.DeepCopy()
	same.Spec.HeartbeatAt = metav1.NewTime(now.Add(time.Minute))
	same.Spec.Fence.ExpiresAt = metav1.NewTime(now.Add(6 * time.Minute))
	same.Spec.Fence.TokenHash = strings.Repeat("c", 64)
	if err := ValidateLeaseAdvance(l, same, now.Add(time.Minute)); err == nil {
		t.Fatal("same epoch token change accepted")
	}
	next := l.DeepCopy()
	next.Spec.Fence.Attempt = 2
	next.Spec.Fence.LeaseEpoch = 2
	next.Spec.Fence.ExpiresAt = metav1.NewTime(now.Add(10 * time.Minute))
	next.Spec.HeartbeatAt = metav1.NewTime(now.Add(time.Minute))
	if err := ValidateLeaseAdvance(l, next, now.Add(time.Minute)); err == nil {
		t.Fatal("higher epoch reused old token")
	}
	_, _ = m, p
}

func TestNonCheckpointablePartitionNeverDuplicatesOnExpiryAlone(t *testing.T) {
	now := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
	m, _, l := fenceFixture(now)
	m.Spec.Checkpoint.Checkpointable = false
	l.Spec.Fence.ExpiresAt = metav1.NewTime(now.Add(-time.Minute))
	if err := CanStartAttempt(m, l, nil, now); err == nil {
		t.Fatal("expired non-checkpointable attempt duplicated without trusted stop")
	}
	stop := validObs(l, spacev1.ExecutionObservationStopped, "", now.Add(-2*time.Minute))
	if err := CanStartAttempt(m, l, []*spacev1.SpaceExecutionObservation{stop}, now); err != nil {
		t.Fatalf("trusted stop rejected: %v", err)
	}
}

func TestCheckpointableMigrationRequiresSignedCheckpointThenStopOrExpiry(t *testing.T) {
	now := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
	m, _, l := fenceFixture(now)
	if err := CanStartAttempt(m, l, nil, now); err == nil {
		t.Fatal("migration without checkpoint accepted")
	}
	checkpoint := validObs(l, spacev1.ExecutionObservationCheckpointed, "cp-1", now)
	if err := CanStartAttempt(m, l, []*spacev1.SpaceExecutionObservation{checkpoint}, now); err == nil {
		t.Fatal("live checkpointed lease migrated without stop/expiry")
	}
	l.Spec.Fence.ExpiresAt = metav1.NewTime(now.Add(-time.Minute))
	checkpoint.Spec.ObservedAt = metav1.NewTime(now.Add(-2 * time.Minute))
	if err := CanStartAttempt(m, l, []*spacev1.SpaceExecutionObservation{checkpoint}, now); err != nil {
		t.Fatalf("expired checkpointed lease should migrate: %v", err)
	}
}

func TestOldTokenReportRejected(t *testing.T) {
	now := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
	_, _, l := fenceFixture(now)
	token, hash, err := NewFenceToken()
	if err != nil {
		t.Fatal(err)
	}
	l.Spec.Fence.TokenHash = hash
	if err := ValidateReport(Report{MissionUID: l.Spec.Fence.MissionUID, PlanID: l.Spec.Fence.PlanID, Attempt: 1, LeaseEpoch: 1, Token: token, Phase: spacev1.ExecutionObservationHeartbeat}, l, now); err != nil {
		t.Fatalf("current token rejected: %v", err)
	}
	oldToken, _, _ := NewFenceToken()
	if err := ValidateReport(Report{MissionUID: l.Spec.Fence.MissionUID, PlanID: l.Spec.Fence.PlanID, Attempt: 1, LeaseEpoch: 1, Token: oldToken, Phase: spacev1.ExecutionObservationHeartbeat}, l, now); err == nil {
		t.Fatal("old token heartbeat accepted")
	}
}

func TestCanDispatchRequiresTransferReceiptComputeTimeAndLease(t *testing.T) {
	now := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
	m, p, l := fenceFixture(now)
	digest := strings.Repeat("d", 64)
	src := spacev1.DomainReference{Name: "ground", ClusterID: "ground", OrbitClass: spacev1.OrbitGround}
	m.Spec.Inputs = []spacev1.DataObject{{ID: "sensor", SizeBytes: 10, Locations: []string{"ground"}, PayloadDigest: digest}}
	p.Spec.InputTransfers = []spacev1.TransferEpoch{{DataID: "sensor", Source: src, Destination: p.Spec.Target, Start: metav1.NewTime(now.Add(-time.Minute)), End: metav1.NewTime(now), Bytes: 10}}
	p.Spec.ComputeStart = metav1.NewTime(now.Add(time.Minute))
	p.Spec.NotBefore = p.Spec.ComputeStart
	if err := CanDispatch(m, p, l, nil, now); err == nil {
		t.Fatal("dispatch before compute start accepted")
	}
	p.Spec.ComputeStart = metav1.NewTime(now)
	p.Spec.NotBefore = p.Spec.ComputeStart
	if err := CanDispatch(m, p, l, nil, now); err == nil {
		t.Fatal("dispatch without transfer receipt accepted")
	}
	r := &spacev1.SpaceTransferReceipt{Spec: spacev1.SpaceTransferReceiptSpec{TransferID: spacev1.InputTransferID(0, "sensor"), MissionUID: string(m.UID), PlanID: p.Spec.PlanID, Attempt: p.Spec.Attempt, Source: src, Destination: p.Spec.Target, DataID: "sensor", Bytes: 10, PayloadDigest: digest, StartedAt: metav1.NewTime(now.Add(-time.Minute)), CompletedAt: metav1.NewTime(now), Provenance: prov()}}
	if err := CanDispatch(m, p, l, []*spacev1.SpaceTransferReceipt{r}, now); err != nil {
		t.Fatalf("complete dispatch evidence rejected: %v", err)
	}
}

func fenceFixture(now time.Time) (*spacev1.SpaceMission, *spacev1.SpacePlacementIntent, *spacev1.SpaceExecutionLease) {
	m := &spacev1.SpaceMission{ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "missions", UID: types.UID("mission-uid")}, Spec: spacev1.SpaceMissionSpec{MissionClass: "science", Priority: 1, StatePolicy: spacev1.PolicyStrict, RequiredCapabilities: []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1}}, Deadline: metav1.NewTime(now.Add(time.Hour)), ExpectedDurationSeconds: 30, MaximumDurationSeconds: 60, DurationUncertaintySecs: 10, SafetyMarginSeconds: 5, MaximumClockSkewSeconds: 1, Retry: spacev1.RetryPolicy{MaxAttempts: 3, AllowMigration: true, MaxConcurrentExecutions: 1}, Checkpoint: spacev1.CheckpointPolicy{Checkpointable: true}}}
	target := spacev1.DomainReference{Name: "leo", ClusterID: "leo", OrbitClass: spacev1.OrbitLEO}
	p := &spacev1.SpacePlacementIntent{Spec: spacev1.SpacePlacementIntentSpec{MissionRef: coreRef(m), PlanID: "plan-one", Attempt: 1, Target: target, NotBefore: metav1.NewTime(now), ComputeStart: metav1.NewTime(now), ComputeEnd: metav1.NewTime(now.Add(time.Minute)), ExpiresAt: metav1.NewTime(now.Add(20 * time.Minute)), MaterialInputDigest: "x", SnapshotSequences: map[string]int64{}}}
	f := spacev1.ExecutionFence{MissionUID: string(m.UID), PlanID: p.Spec.PlanID, Attempt: 1, LeaseEpoch: 1, TokenHash: strings.Repeat("b", 64), ExpiresAt: metav1.NewTime(now.Add(5 * time.Minute))}
	l := &spacev1.SpaceExecutionLease{ObjectMeta: metav1.ObjectMeta{Name: spacev1.ExecutionLeaseName(f.MissionUID, f.PlanID, 1, 1)}, Spec: spacev1.SpaceExecutionLeaseSpec{Source: target, Destination: target, Fence: f, HeartbeatAt: metav1.NewTime(now), MaximumClockSkewSeconds: 1, Provenance: prov()}}
	return m, p, l
}
func coreRef(m *spacev1.SpaceMission) corev1.ObjectReference {
	return corev1.ObjectReference{Namespace: m.Namespace, Name: m.Name, UID: m.UID}
}
func prov() spacev1.Provenance {
	return spacev1.Provenance{ReporterID: "reporter", Source: "agent", Digest: strings.Repeat("a", 64), Sequence: 1}
}
func validObs(l *spacev1.SpaceExecutionLease, phase spacev1.ExecutionObservationPhase, checkpoint string, at time.Time) *spacev1.SpaceExecutionObservation {
	f := l.Spec.Fence
	o := &spacev1.SpaceExecutionObservation{Spec: spacev1.SpaceExecutionObservationSpec{ObservationID: "obs", MissionUID: f.MissionUID, PlanID: f.PlanID, Attempt: f.Attempt, LeaseEpoch: f.LeaseEpoch, TokenHash: f.TokenHash, Source: l.Spec.Source, Destination: l.Spec.Destination, Phase: phase, CheckpointID: checkpoint, ObservedAt: metav1.NewTime(at), Provenance: prov()}}
	o.Name = spacev1.ExecutionObservationName(o.Spec.Source, o.Spec.Destination, o.Spec.MissionUID, o.Spec.PlanID, o.Spec.ObservationID)
	return o
}
