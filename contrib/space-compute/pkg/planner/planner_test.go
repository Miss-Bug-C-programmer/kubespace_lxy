package planner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

type testClock struct{ now time.Time }

func (f testClock) Now() time.Time { return f.now }

func TestLEOGEOAndGroundDeterministicDecision(t *testing.T) {
	now, mission, summaries, links := scenario()
	first, err := Plan(mission, summaries, links, testClock{now})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Plan(mission.DeepCopy(), reverseSummaries(summaries), reverseLinks(links), testClock{now})
	if err != nil {
		t.Fatal(err)
	}
	if first.Placement.Spec.Target.Name != "leo-a" {
		t.Fatalf("target = %s, want leo-a; rejected=%+v", first.Placement.Spec.Target.Name, first.Rejected)
	}
	assertDecisionGolden(t, first.Placement)
	if first.Placement.Spec.MaterialInputDigest != second.Placement.Spec.MaterialInputDigest || first.Placement.Spec.PlanID != second.Placement.Spec.PlanID || first.Placement.Spec.Score != second.Placement.Spec.Score {
		t.Fatalf("replay changed decision: first=%+v second=%+v", first.Placement.Spec, second.Placement.Spec)
	}
	if len(first.Placement.Spec.Explanations) < 8 || first.Placement.Spec.Explanations[0].Code != "capabilities_satisfied" {
		t.Fatalf("explanations = %+v", first.Placement.Spec.Explanations)
	}
	// GEO has twice the compute rating, but its later contacts make total
	// completion worse; the nearer LEO plan wins.
	if !first.Placement.Spec.ComputeEnd.Time.Before(links[2].Spec.Windows[0].Start.Time) {
		t.Fatal("LEO plan did not complete compute before the late GEO contact")
	}
}

func TestRecordedLinkInputsMatchDeterministicScenario(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "golden", "recorded-links.json"))
	if err != nil {
		t.Fatal(err)
	}
	var recorded spacev1.SpaceLinkSnapshotList
	if err := json.Unmarshal(raw, &recorded); err != nil {
		t.Fatal(err)
	}
	_, _, _, links := scenario()
	if len(recorded.Items) != len(links) {
		t.Fatalf("recorded links=%d scenario=%d", len(recorded.Items), len(links))
	}
	for index := range links {
		recordedSpec, recordedErr := json.Marshal(recorded.Items[index].Spec)
		scenarioSpec, scenarioErr := json.Marshal(links[index].Spec)
		if recordedErr != nil || scenarioErr != nil {
			t.Fatalf("marshal recorded link %d: recorded=%v scenario=%v", index, recordedErr, scenarioErr)
		}
		if recorded.Items[index].Name != links[index].Name || !bytes.Equal(recordedSpec, scenarioSpec) {
			t.Fatalf("recorded link %d drifted: recorded=%+v scenario=%+v", index, recorded.Items[index], links[index])
		}
	}
}

func assertDecisionGolden(t *testing.T, placement *spacev1.SpacePlacementIntent) {
	t.Helper()
	var expected struct {
		TargetDomain, TargetOrbitClass, ComputeStart, ComputeEnd, ResultEnd string
		Score                                                               spacev1.DecisionScore `json:"score"`
		ExplanationCodes                                                    []string              `json:"explanationCodes"`
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "golden", "expected-decision.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &expected); err != nil {
		t.Fatal(err)
	}
	codes := make([]string, len(placement.Spec.Explanations))
	for i := range placement.Spec.Explanations {
		codes[i] = placement.Spec.Explanations[i].Code
	}
	if placement.Spec.Target.Name != expected.TargetDomain || string(placement.Spec.Target.OrbitClass) != expected.TargetOrbitClass || placement.Spec.ComputeStart.Time.Format(time.RFC3339) != expected.ComputeStart || placement.Spec.ComputeEnd.Time.Format(time.RFC3339) != expected.ComputeEnd || placement.Spec.ResultTransfer.End.Time.Format(time.RFC3339) != expected.ResultEnd || placement.Spec.Score != expected.Score || !reflect.DeepEqual(codes, expected.ExplanationCodes) {
		t.Fatalf("decision does not match golden: placement=%+v codes=%v expected=%+v", placement.Spec, codes, expected)
	}
}

func TestInputWindowFitAndReturnDeadlineFailures(t *testing.T) {
	now, mission, summaries, links := scenario()
	leoOnly := summaries[:1]
	short := links[0].DeepCopy()
	short.Spec.Windows[0].End = metav1.NewTime(short.Spec.Windows[0].Start.Add(10 * time.Second))
	if _, err := Plan(mission, leoOnly, []*spacev1.SpaceLinkSnapshot{short, links[1]}, testClock{now}); err == nil || !strings.Contains(err.Error(), "input_transfer_window_missing") {
		t.Fatalf("short input window error = %v", err)
	}

	returnLate := mission.DeepCopy()
	returnLate.Spec.Deadline = metav1.NewTime(now.Add(15 * time.Minute))
	if _, err := Plan(returnLate, leoOnly, links[:2], testClock{now}); err == nil || !strings.Contains(err.Error(), "result_return_window_missing") {
		t.Fatalf("late return error = %v", err)
	}
}

func TestRejectedControllerGenerationCannotEnterPlanning(t *testing.T) {
	now, mission, summaries, links := scenario()
	rejectedLink := links[0].DeepCopy()
	rejectedLink.Generation = 2
	rejectedLink.Status.ObservedGeneration = 2
	rejectedLink.Status.AcceptedSequence = 1
	rejectedLink.Status.Conditions = []metav1.Condition{{Type: ConditionLinkValidated, Status: metav1.ConditionFalse, Reason: "RejectedObservation", ObservedGeneration: 2}}
	if _, err := Plan(mission, summaries[:1], []*spacev1.SpaceLinkSnapshot{rejectedLink, links[1]}, testClock{now}); err == nil || !strings.Contains(err.Error(), "input_transfer_window_missing") {
		t.Fatalf("rejected link generation entered planning: %v", err)
	}

	rejectedSummary := summaries[0].DeepCopy()
	rejectedSummary.Generation = 2
	rejectedSummary.Status.ObservedGeneration = 2
	rejectedSummary.Status.Conditions = []metav1.Condition{{Type: "Validated", Status: metav1.ConditionFalse, Reason: "RejectedSummary", ObservedGeneration: 2}}
	if _, err := Plan(mission, []*spacev1.SpaceDomainResourceSummary{rejectedSummary}, links[:2], testClock{now}); err == nil || !strings.Contains(err.Error(), "resource_snapshot_unaccepted") {
		t.Fatalf("rejected resource generation entered planning: %v", err)
	}
}

func TestEnergyPolicyRejectsOrPenalizes(t *testing.T) {
	now, mission, summaries, links := scenario()
	low := summaries[0].DeepCopy()
	low.Spec.EnergyHeadroomMilli = 100
	low.Spec.MinimumEnergyMilli = 300
	if _, err := Plan(mission, []*spacev1.SpaceDomainResourceSummary{low}, links[:2], testClock{now}); err == nil || !strings.Contains(err.Error(), "energy_below_minimum") {
		t.Fatalf("strict low-energy error = %v", err)
	}
	degraded := mission.DeepCopy()
	degraded.Spec.StatePolicy = spacev1.PolicyDegraded
	decision, err := Plan(degraded, []*spacev1.SpaceDomainResourceSummary{low}, links[:2], testClock{now})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Placement.Spec.Score.EnergyThermal >= 50 {
		t.Fatalf("degraded energy score = %d, want penalty", decision.Placement.Spec.Score.EnergyThermal)
	}
}

func TestPartitionCheckpointAndNonCheckpointableFailure(t *testing.T) {
	now, mission, summaries, links := scenario()
	decision, err := Plan(mission, summaries[:1], links[:2], testClock{now})
	if err != nil {
		t.Fatal(err)
	}
	placement := decision.Placement
	placement.Status.Phase = spacev1.PlacementRunning
	if err := HandleLinkPartition(placement, mission, testClock{now}); err != nil {
		t.Fatal(err)
	}
	if placement.Status.Phase != spacev1.PlacementReplanning {
		t.Fatalf("checkpointable phase = %s", placement.Status.Phase)
	}
	observation := spacev1.ExecutionObservation{Sequence: 1, Attempt: placement.Spec.Attempt, Phase: "checkpointed", CheckpointID: "checkpoint-1", ObservedAt: metav1.NewTime(now)}
	if changed, err := ApplyExecutionObservation(placement, mission, observation, testClock{now}); err != nil || !changed || placement.Status.Phase != spacev1.PlacementCheckpointed {
		t.Fatalf("checkpoint observation changed=%v err=%v phase=%s", changed, err, placement.Status.Phase)
	}
	if changed, err := ApplyExecutionObservation(placement, mission, observation, testClock{now}); err != nil || changed {
		t.Fatalf("duplicate changed=%v err=%v", changed, err)
	}

	nonCheckpointable := mission.DeepCopy()
	nonCheckpointable.Spec.Checkpoint = spacev1.CheckpointPolicy{}
	nonCheckpointable.Spec.Retry.AllowMigration = false
	failed := decision.Placement.DeepCopy()
	failed.Status.Phase = spacev1.PlacementRunning
	if err := HandleLinkPartition(failed, nonCheckpointable, testClock{now}); err != nil || failed.Status.Phase != spacev1.PlacementFailed {
		t.Fatalf("non-checkpointable partition err=%v phase=%s", err, failed.Status.Phase)
	}
}

func TestLinkHistoryIsBoundedAndRecordsRejectedObservation(t *testing.T) {
	now, _, _, links := scenario()
	previous := links[0].DeepCopy()
	previous.Spec.HistoryLimit = 2
	status, err := ReconcileLinkStatus(previous, nil, testClock{now})
	if err != nil {
		t.Fatal(err)
	}
	previous.Status = status
	bad := previous.DeepCopy()
	bad.Generation = 2
	bad.Spec.Provenance.Sequence = 2
	bad.Spec.ObservedAt = metav1.NewTime(now.Add(time.Second))
	bad.Spec.ValidUntil = metav1.NewTime(now.Add(3 * time.Hour))
	bad.Spec.Windows[0].End = bad.Spec.Windows[0].Start
	status, err = ReconcileLinkStatus(bad, previous, testClock{now})
	if err == nil {
		t.Fatal("invalid observation accepted")
	}
	if len(status.History) != 2 || status.History[1].Accepted {
		t.Fatalf("history = %+v", status.History)
	}
}

func TestControllerRestartDuplicateIntentAndCrossDomainFence(t *testing.T) {
	now, mission, summaries, links := scenario()
	repository := &memoryRepository{mission: mission, summaries: summaries, links: links}
	controller := &Controller{Repository: repository, Clock: testClock{now}}
	if _, err := controller.Reconcile(context.Background(), MissionKey{Namespace: mission.Namespace, Name: mission.Name}); err != nil {
		t.Fatal(err)
	}
	if repository.applyCount != 1 {
		t.Fatalf("apply count = %d", repository.applyCount)
	}
	// New controller instance represents restart; the same intent is idempotent.
	if _, err := (&Controller{Repository: repository, Clock: testClock{now}}).Reconcile(context.Background(), MissionKey{Namespace: mission.Namespace, Name: mission.Name}); err != nil {
		t.Fatal(err)
	}
	if repository.applyCount != 1 {
		t.Fatalf("restart duplicated placement, apply count=%d", repository.applyCount)
	}

	repository.placement.Status.Phase = spacev1.PlacementRunning
	repository.links[0] = repository.links[0].DeepCopy()
	repository.links[0].Spec.Provenance.Sequence++
	repository.links[0].Spec.Provenance.Digest = strings.Repeat("d", 64)
	repository.links[0].Spec.ObservedAt = metav1.NewTime(now.Add(time.Minute))
	repository.links[0].Spec.ValidUntil = metav1.NewTime(now.Add(3 * time.Hour))
	repository.links[0].Spec.Windows[0].BandwidthBitsPerSec += 1
	if _, err := controller.Reconcile(context.Background(), MissionKey{Namespace: mission.Namespace, Name: mission.Name}); err != nil {
		t.Fatal(err)
	}
	if repository.applyCount != 1 || repository.placement.Status.Phase != spacev1.PlacementReplanning {
		t.Fatalf("running attempt was duplicated: apply=%d phase=%s", repository.applyCount, repository.placement.Status.Phase)
	}
}

func scenario() (time.Time, *spacev1.SpaceMission, []*spacev1.SpaceDomainResourceSummary, []*spacev1.SpaceLinkSnapshot) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	mission := &spacev1.SpaceMission{ObjectMeta: metav1.ObjectMeta{Name: "earth-observation", Namespace: "missions", UID: types.UID("mission-uid"), Generation: 1}, Spec: spacev1.SpaceMissionSpec{MissionClass: "earth-observation", Priority: 900, StatePolicy: spacev1.PolicyStrict,
		RequiredCapabilities: []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1, Architecture: "space-cuda"}}, RequiredSoftware: map[string]string{"runtime.spacecompute.k3s.io/cuda": "12.4"},
		Inputs: []spacev1.DataObject{{ID: "sensor-frame", SizeBytes: 600_000_000, Locations: []string{"ground-a"}}}, OutputSizeBytes: 10_000_000, ResultDestinations: []string{"ground-a"}, ResultReturnRequired: true,
		Deadline: metav1.NewTime(now.Add(2 * time.Hour)), ExpectedDurationSeconds: 300, MaximumDurationSeconds: 600, DurationUncertaintySecs: 100, SafetyMarginSeconds: 10, MaximumClockSkewSeconds: 2,
		Retry: spacev1.RetryPolicy{MaxAttempts: 3, AllowMigration: true, MaxConcurrentExecutions: 1}, Checkpoint: spacev1.CheckpointPolicy{Checkpointable: true, MinimumIntervalSecs: 60, MaximumStateBytes: 1_000_000},
		WorkloadTemplate: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "processor", Image: "example.invalid/processor:v1"}}}}}}
	leo := summary("leo-a", "leo-a-cluster", spacev1.OrbitLEO, now, 1000, 800, 800)
	geo := summary("geo-a", "geo-a-cluster", spacev1.OrbitGEO, now, 2000, 900, 900)
	links := []*spacev1.SpaceLinkSnapshot{
		link("ground-leo", "ground-a", spacev1.OrbitGround, "leo-a", spacev1.OrbitLEO, now, now.Add(time.Minute), now.Add(15*time.Minute), 100_000_000, 1),
		link("leo-ground", "leo-a", spacev1.OrbitLEO, "ground-a", spacev1.OrbitGround, now, now.Add(20*time.Minute), now.Add(35*time.Minute), 100_000_000, 1),
		link("ground-geo", "ground-a", spacev1.OrbitGround, "geo-a", spacev1.OrbitGEO, now, now.Add(25*time.Minute), now.Add(40*time.Minute), 1_000_000_000, 1),
		link("geo-ground", "geo-a", spacev1.OrbitGEO, "ground-a", spacev1.OrbitGround, now, now.Add(50*time.Minute), now.Add(65*time.Minute), 1_000_000_000, 1),
	}
	return now, mission, []*spacev1.SpaceDomainResourceSummary{leo, geo}, links
}

func summary(name, cluster string, orbit spacev1.OrbitClass, now time.Time, compute int64, energy, thermal int32) *spacev1.SpaceDomainResourceSummary {
	return &spacev1.SpaceDomainResourceSummary{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: spacev1.SpaceDomainResourceSummarySpec{Domain: spacev1.DomainReference{Name: name, ClusterID: cluster, OrbitClass: orbit}, ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(3 * time.Hour)), Provenance: provenance(1), Devices: []spacev1.DeviceCapacity{{Class: "gpu", Count: 2, Architectures: []string{"space-cuda"}, Precision: []string{"fp16"}, ComputeMilli: compute, FragmentationMilli: 800}}, Software: map[string]string{"runtime.spacecompute.k3s.io/cuda": "12.4"}, QueueDelaySeconds: 5, EnergyHeadroomMilli: energy, ThermalHeadroomMilli: thermal, ResilienceMilli: 850, MinimumEnergyMilli: 300, MinimumThermalMilli: 300, MaximumSnapshotAgeSecs: 60, ExporterSnapshotDigest: strings.Repeat("b", 64)}}
}

func link(name, source string, sourceOrbit spacev1.OrbitClass, destination string, destinationOrbit spacev1.OrbitClass, now, start, end time.Time, bandwidth int64, sequence int64) *spacev1.SpaceLinkSnapshot {
	return &spacev1.SpaceLinkSnapshot{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: spacev1.SpaceLinkSnapshotSpec{Source: spacev1.DomainReference{Name: source, ClusterID: source + "-cluster", OrbitClass: sourceOrbit}, Destination: spacev1.DomainReference{Name: destination, ClusterID: destination + "-cluster", OrbitClass: destinationOrbit}, ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(3 * time.Hour)), MaximumClockSkewSeconds: 2, MinimumUpdateSeconds: 10, HistoryLimit: 16, Provenance: provenance(sequence), Windows: []spacev1.ContactWindow{{ID: name + "-window", Start: metav1.NewTime(start), End: metav1.NewTime(end), BandwidthBitsPerSec: bandwidth, RTTMicroseconds: 20_000, LossPartsPerMillion: 100, ErrorPartsPerMillion: 10, StabilityMilli: 900, ConfidenceMilli: 900, Predicted: true}}}}
}
func provenance(sequence int64) spacev1.Provenance {
	return spacev1.Provenance{ReporterID: "authenticated-reporter", Source: "recorded-contact-product", Digest: strings.Repeat("a", 64), Sequence: sequence}
}
func reverseSummaries(in []*spacev1.SpaceDomainResourceSummary) []*spacev1.SpaceDomainResourceSummary {
	out := append([]*spacev1.SpaceDomainResourceSummary(nil), in...)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}
func reverseLinks(in []*spacev1.SpaceLinkSnapshot) []*spacev1.SpaceLinkSnapshot {
	out := append([]*spacev1.SpaceLinkSnapshot(nil), in...)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

type memoryRepository struct {
	mission    *spacev1.SpaceMission
	summaries  []*spacev1.SpaceDomainResourceSummary
	links      []*spacev1.SpaceLinkSnapshot
	placement  *spacev1.SpacePlacementIntent
	applyCount int
}

func (r *memoryRepository) GetMission(context.Context, MissionKey) (*spacev1.SpaceMission, error) {
	if r.mission == nil {
		return nil, ErrNotFound
	}
	return r.mission.DeepCopy(), nil
}
func (r *memoryRepository) ListResourceSummaries(context.Context) ([]*spacev1.SpaceDomainResourceSummary, error) {
	return r.summaries, nil
}
func (r *memoryRepository) ListLinkSnapshots(context.Context) ([]*spacev1.SpaceLinkSnapshot, error) {
	return r.links, nil
}
func (r *memoryRepository) GetPlacement(context.Context, MissionKey) (*spacev1.SpacePlacementIntent, error) {
	if r.placement == nil {
		return nil, ErrNotFound
	}
	return r.placement.DeepCopy(), nil
}
func (r *memoryRepository) ApplyPlacement(_ context.Context, value *spacev1.SpacePlacementIntent, expected string) (bool, error) {
	if r.placement != nil && r.placement.Spec.PlanID != expected {
		return false, errors.New("optimistic plan conflict")
	}
	if r.placement != nil && r.placement.Spec.PlanID == value.Spec.PlanID {
		return false, nil
	}
	r.placement = value.DeepCopy()
	r.applyCount++
	return true, nil
}
func (r *memoryRepository) UpdatePlacementStatus(_ context.Context, value *spacev1.SpacePlacementIntent) error {
	r.placement.Status = value.Status
	return nil
}
func (r *memoryRepository) UpdateMissionStatus(_ context.Context, value *spacev1.SpaceMission) error {
	r.mission.Status = value.Status
	return nil
}
func (*memoryRepository) Event(context.Context, MissionKey, string, string, string) {}
