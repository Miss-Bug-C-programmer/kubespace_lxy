package planner

import (
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

func TestPlannerScaleDeterminismAndLatencyBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("scale qualification is excluded only by explicit -short")
	}
	for _, count := range []int{100, 1000, 5000} {
		t.Run(fmt.Sprintf("domains-%d", count), func(t *testing.T) {
			now, mission, summaries := planningDataset(count, spacev1.PolicyStrict)
			start := time.Now()
			first, err := Plan(mission, summaries, nil, testClock{now})
			elapsed := time.Since(start)
			if err != nil {
				t.Fatal(err)
			}
			second, err := Plan(mission.DeepCopy(), reverseSummaries(summaries), nil, testClock{now})
			if err != nil {
				t.Fatal(err)
			}
			if first.Placement.Spec.PlanID != second.Placement.Spec.PlanID || first.Placement.Spec.Target != second.Placement.Spec.Target || first.Placement.Spec.Score != second.Placement.Spec.Score {
				t.Fatalf("scale replay changed decision: first=%+v second=%+v", first.Placement.Spec, second.Placement.Spec)
			}
			// This is a deliberately tolerant CPU-only regression ceiling, not a
			// claimed production p99. It catches algorithmic/runaway regressions
			// while the benchmark report supplies host-specific measurements.
			budget := 2*time.Second + time.Duration(count/1000)*2*time.Second
			if elapsed > budget {
				t.Fatalf("planning %d domains took %s, budget %s", count, elapsed, budget)
			}
			t.Logf("domains=%d planning=%s target=%s", count, elapsed, first.Placement.Spec.Target.Name)
		})
	}
}

func BenchmarkMissionPlanningDatasets(b *testing.B) {
	for _, count := range []int{100, 1000, 5000} {
		for _, policy := range []spacev1.StatePolicy{spacev1.PolicyStrict, spacev1.PolicyDegraded, spacev1.PolicyBestEffort} {
			b.Run(fmt.Sprintf("%d/%s", count, policy), func(b *testing.B) {
				now, mission, summaries := planningDataset(count, policy)
				b.ReportMetric(float64(count), "domains/op")
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := Plan(mission, summaries, nil, testClock{now}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func planningDataset(count int, policy spacev1.StatePolicy) (time.Time, *spacev1.SpaceMission, []*spacev1.SpaceDomainResourceSummary) {
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	mission := &spacev1.SpaceMission{
		ObjectMeta: metav1.ObjectMeta{Name: "scale-mission", Namespace: "missions", UID: types.UID("scale-mission-uid"), Generation: 1},
		Spec: spacev1.SpaceMissionSpec{
			MissionClass: "batch", Priority: 500, StatePolicy: policy,
			RequiredCapabilities: []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1, Architecture: "iluvatar"}},
			Deadline:             metav1.NewTime(now.Add(2 * time.Hour)), ExpectedDurationSeconds: 60, MaximumDurationSeconds: 120,
			DurationUncertaintySecs: 30, SafetyMarginSeconds: 5, MaximumClockSkewSeconds: 1,
			Retry:            spacev1.RetryPolicy{MaxAttempts: 3, AllowMigration: true, MaxConcurrentExecutions: 1},
			Checkpoint:       spacev1.CheckpointPolicy{Checkpointable: true, MinimumIntervalSecs: 30, MaximumStateBytes: 1 << 20},
			WorkloadTemplate: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "example.invalid/worker:v1"}}}},
		},
	}
	summaries := make([]*spacev1.SpaceDomainResourceSummary, count)
	for index := range summaries {
		name := fmt.Sprintf("domain-%05d", index)
		compute := int64(1000 + index%5*100)
		summaries[index] = &spacev1.SpaceDomainResourceSummary{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: spacev1.SpaceDomainResourceSummarySpec{
				Domain:     spacev1.DomainReference{Name: name, ClusterID: name + "-cluster", OrbitClass: spacev1.OrbitGround},
				ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(3 * time.Hour)), Provenance: provenance(int64(index + 1)),
				Devices:           []spacev1.DeviceCapacity{{Class: "gpu", Count: int64(1 + index%4), Architectures: []string{"iluvatar"}, ComputeMilli: compute, FragmentationMilli: int32(500 + index%500)}},
				QueueDelaySeconds: int64(index % 30), EnergyHeadroomMilli: int32(600 + index%400), ThermalHeadroomMilli: int32(650 + index%350),
				ResilienceMilli: int32(700 + index%300), MinimumEnergyMilli: 200, MinimumThermalMilli: 200,
				MaximumSnapshotAgeSecs: 60, ExporterSnapshotDigest: fmt.Sprintf("%064x", index+1),
			},
		}
	}
	return now, mission, summaries
}
