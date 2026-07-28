package planner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

func TestPhase10PreparedMaterialDigestMatchesLegacyEncoding(t *testing.T) {
	now, mission, summaries := planningDataset(100, spacev1.PolicyStrict)
	prepared, err := PreparePlanningInputs(summaries, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := materialDigestPrepared(mission, prepared)
	if err != nil {
		t.Fatal(err)
	}
	want, err := legacyMaterialDigestForPhase10Test(mission, summaries, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("prepared digest=%s legacy=%s", got, want)
	}
	first, err := PlanPrepared(mission, prepared, testClock{now})
	if err != nil {
		t.Fatal(err)
	}
	second, err := PlanPrepared(mission.DeepCopy(), prepared, testClock{now})
	if err != nil {
		t.Fatal(err)
	}
	if first.Placement.Spec.MaterialInputDigest != second.Placement.Spec.MaterialInputDigest || first.Placement.Spec.PlanID != second.Placement.Spec.PlanID {
		t.Fatal("reused prepared inputs changed deterministic decision")
	}
}

func legacyMaterialDigestForPhase10Test(mission *spacev1.SpaceMission, summaries []*spacev1.SpaceDomainResourceSummary, links []*spacev1.SpaceLinkSnapshot) (string, error) {
	type material struct {
		Mission           spacev1.SpaceMissionSpec                 `json:"mission"`
		MissionGeneration int64                                    `json:"missionGeneration"`
		Resources         []spacev1.SpaceDomainResourceSummarySpec `json:"resources"`
		Links             []spacev1.SpaceLinkSnapshotSpec          `json:"links"`
	}
	normalizedMission, err := normalizedMissionSpecForDigest(mission.Spec)
	if err != nil {
		return "", err
	}
	value := material{Mission: normalizedMission, MissionGeneration: mission.Generation}
	for _, summary := range summaries {
		if summary != nil {
			value.Resources = append(value.Resources, normalizedResourceSummarySpecForDigest(summary.Spec))
		}
	}
	sort.SliceStable(value.Resources, func(i, j int) bool {
		return fullDomainKey(value.Resources[i].Domain) < fullDomainKey(value.Resources[j].Domain)
	})
	sortedLinks := append([]*spacev1.SpaceLinkSnapshot(nil), links...)
	sort.SliceStable(sortedLinks, func(i, j int) bool {
		if sortedLinks[i] == nil {
			return false
		}
		if sortedLinks[j] == nil {
			return true
		}
		left := directedDomainKey(sortedLinks[i].Spec.Source, sortedLinks[i].Spec.Destination) + "\x00" + sortedLinks[i].Name
		right := directedDomainKey(sortedLinks[j].Spec.Source, sortedLinks[j].Spec.Destination) + "\x00" + sortedLinks[j].Name
		return left < right
	})
	for _, link := range sortedLinks {
		if link != nil {
			value.Links = append(value.Links, normalizedLinkSpecForDigest(link.Spec))
		}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func BenchmarkPreparedMissionPlanningDatasets(b *testing.B) {
	for _, count := range []int{100, 1000, 5000} {
		b.Run(benchmarkCountName(count), func(b *testing.B) {
			now, mission, summaries := planningDataset(count, spacev1.PolicyStrict)
			prepared, err := PreparePlanningInputs(summaries, nil)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(count), "domains/op")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := PlanPrepared(mission, prepared, testClock{now}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchmarkCountName(count int) string {
	if count == 100 {
		return "100"
	}
	if count == 1000 {
		return "1000"
	}
	return "5000"
}

func TestPhase10ControllerReusesUnchangedDurablePlanWithoutDomainScan(t *testing.T) {
	now, mission, _ := planningDataset(1, spacev1.PolicyStrict)
	mission.Generation = 7
	mission.Status = spacev1.SpaceMissionStatus{
		ObservedGeneration: 7,
		Phase:              spacev1.MissionPlanned,
		PlacementName:      "mission-placement",
		PlanID:             "plan-current",
		LastDecisionDigest: "material-current",
	}
	repository := &memoryRepository{mission: mission}
	repository.placement = &spacev1.SpacePlacementIntent{
		ObjectMeta: metav1.ObjectMeta{Name: "mission-placement", Namespace: mission.Namespace},
		Spec: spacev1.SpacePlacementIntentSpec{
			MissionRef:          corev1.ObjectReference{Namespace: mission.Namespace, Name: mission.Name, UID: mission.UID},
			PlanID:              "plan-current",
			MaterialInputDigest: "material-current",
			PlanningInputDigest: "test-input-digest",
			ExpiresAt:           metav1.NewTime(now.Add(time.Hour)),
			ComputeEnd:          metav1.NewTime(now.Add(30 * time.Minute)),
		},
		Status: spacev1.SpacePlacementIntentStatus{Phase: spacev1.PlacementPending},
	}
	// memoryRepository intentionally exposes no summaries. If Reconcile falls
	// through to Plan/PlanPrepared, this case returns NoFeasiblePlan. A successful
	// reconcile therefore proves the unchanged durable-plan fast path avoids the
	// domain scan entirely.
	controller := &Controller{Repository: repository, Clock: testClock{now}}
	result, err := controller.Reconcile(context.Background(), MissionKey{Namespace: mission.Namespace, Name: mission.Name})
	if err != nil {
		t.Fatal(err)
	}
	if repository.applyCount != 0 {
		t.Fatalf("unchanged durable plan produced %d placement writes", repository.applyCount)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("requeueAfter=%v, want guarded expiry requeue", result.RequeueAfter)
	}
}
