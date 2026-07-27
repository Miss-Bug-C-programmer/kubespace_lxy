package v1beta1

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestAddToSchemeRegistersCanonicalBetaGVKs(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		obj  runtime.Object
		kind string
	}{
		{&SpaceMission{}, "SpaceMission"},
		{&SpaceDomainResourceSummary{}, "SpaceDomainResourceSummary"},
		{&PhysicalDeviceInventory{}, "PhysicalDeviceInventory"},
		{&SpacePlacementIntent{}, "SpacePlacementIntent"},
	}
	for _, tc := range cases {
		gvks, _, err := scheme.ObjectKinds(tc.obj)
		if err != nil {
			t.Fatalf("%s ObjectKinds: %v", tc.kind, err)
		}
		want := schema.GroupVersionKind{Group: GroupName, Version: "v1beta1", Kind: tc.kind}
		found := false
		for _, got := range gvks {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s beta GVK missing from %v", tc.kind, gvks)
		}
	}
}
