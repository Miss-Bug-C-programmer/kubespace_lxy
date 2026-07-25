package workload

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

func TestBuildInputTransferIntentsIsPureAndTransportOwned(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	mission, placement := dispatchFixture(now)
	digest := strings.Repeat("a", 64)
	source := spacev1.DomainReference{Name: "ground-a", ClusterID: "ground", OrbitClass: spacev1.OrbitGround}
	coordinator := spacev1.DomainReference{Name: "ground-control", ClusterID: "control", OrbitClass: spacev1.OrbitGround}
	mission.Spec.Inputs = []spacev1.DataObject{{ID: "sensor", SizeBytes: 7, Locations: []string{"ground-a"}, PayloadDigest: digest}}
	placement.Spec.InputTransfers = []spacev1.TransferEpoch{{DataID: "sensor", Source: source, Destination: placement.Spec.Target, Start: metav1.NewTime(now), End: metav1.NewTime(now.Add(time.Minute)), Bytes: 7}}
	intents, err := BuildInputTransferIntents(mission, placement, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 1 {
		t.Fatalf("intents=%d", len(intents))
	}
	intent := intents[0]
	if intent.Spec.Coordinator != coordinator || intent.Spec.PayloadDigest != digest || intent.Spec.Attempt != placement.Spec.Attempt || intent.Name == "" {
		t.Fatalf("unexpected transfer intent: %#v", intent)
	}
}
