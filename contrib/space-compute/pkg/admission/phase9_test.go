package admission

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

func TestPhase9ReporterAdmissionAcceptsBetaPhysicalInventory(t *testing.T) {
	now := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	principal := "system:serviceaccount:reporters:leo-a"
	publicKey, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	domain := spacev1.DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: spacev1.OrbitLEO}
	ref := spacev1.SecretReference{Namespace: "kube-system", Name: "space-compute-reporter-public-keys", Key: "leo-a"}
	binding := &spacev1.SpaceDomainReporterBinding{ObjectMeta: metav1.ObjectMeta{Name: spacev1.ReporterBindingName(principal)}, Spec: spacev1.SpaceDomainReporterBindingSpec{ReporterPrincipal: principal, Domain: domain, AllowedKinds: []string{"PhysicalDeviceInventory"}, PublicKeyRef: ref}}
	validator, err := NewValidator(&fakeTrustSource{bindings: map[string]*spacev1.SpaceDomainReporterBinding{principal: binding}, keys: map[string]ed25519.PublicKey{ref.Key: publicKey}}, fixedClock{now})
	if err != nil {
		t.Fatal(err)
	}
	inventory := &spacev1.PhysicalDeviceInventory{TypeMeta: metav1.TypeMeta{APIVersion: "spacecompute.k3s.io/v1beta1", Kind: "PhysicalDeviceInventory"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.PhysicalDeviceInventoryName(domain, "node-a")}, Spec: spacev1.PhysicalDeviceInventorySpec{
		Domain: domain, NodeName: "node-a", ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)), ConfidenceMilli: 900,
		Provenance: spacev1.Provenance{ReporterID: principal, Source: "device-agent", Sequence: 1},
		Devices:    []spacev1.PhysicalDevice{{StableDeviceID: "gpu-a", KubernetesResourceName: "nvidia.com/gpu", AllocationID: "dra/device-a", Class: "gpu", Vendor: "nvidia", Model: "h100", Architecture: "sm90", Topology: spacev1.DeviceTopology{PCIAddress: "0000:01:00.0"}, TotalMemoryBytes: 80 << 30, FreeMemoryBytes: 70 << 30, Health: "healthy", ConfidenceMilli: 900}},
	}}
	signReporterObject(t, inventory, privateKey)
	request := admissionRequest(t, admissionv1.Create, "physicaldeviceinventories", principal, inventory, nil)
	request.Resource.Version = "v1beta1"
	if err := validator.Validate(context.Background(), request); err != nil {
		t.Fatalf("beta inventory rejected: %v", err)
	}
}

func TestStorageMigratorNoopAdmissionIsNarrow(t *testing.T) {
	oldRaw := []byte(`{"apiVersion":"spacecompute.k3s.io/v1alpha1","kind":"SpaceMission","metadata":{"name":"m","namespace":"n","uid":"u","resourceVersion":"1"},"spec":{"workingMemoryBytes":1},"status":{"phase":"Planned"}}`)
	newRaw := []byte(`{"apiVersion":"spacecompute.k3s.io/v1beta1","kind":"SpaceMission","metadata":{"name":"m","namespace":"n","uid":"u","resourceVersion":"2"},"spec":{"workingMemoryBytes":1},"status":{"phase":"Planned"}}`)
	request := &admissionv1.AdmissionRequest{Operation: admissionv1.Update, UserInfo: authenticationv1.UserInfo{Username: StorageMigratorPrincipal}, Object: runtime.RawExtension{Raw: newRaw}, OldObject: runtime.RawExtension{Raw: oldRaw}}
	if !IsStorageMigrationNoop(request) {
		t.Fatal("version-only storage rewrite was not recognized")
	}
	var changed map[string]interface{}
	if err := json.Unmarshal(newRaw, &changed); err != nil {
		t.Fatal(err)
	}
	changed["spec"].(map[string]interface{})["workingMemoryBytes"] = float64(2)
	badRaw, _ := json.Marshal(changed)
	request.Object.Raw = badRaw
	if IsStorageMigrationNoop(request) {
		t.Fatal("migrator semantic spec mutation was incorrectly bypassed")
	}
	request.Object.Raw = newRaw
	request.UserInfo.Username = StorageMigratorPrincipal
	var metadataChanged map[string]interface{}
	if err := json.Unmarshal(newRaw, &metadataChanged); err != nil {
		t.Fatal(err)
	}
	metadataChanged["metadata"].(map[string]interface{})["labels"] = map[string]interface{}{"security.example/role": "changed"}
	metadataRaw, _ := json.Marshal(metadataChanged)
	request.Object.Raw = metadataRaw
	if IsStorageMigrationNoop(request) {
		t.Fatal("migrator metadata mutation was incorrectly bypassed")
	}
	request.Object.Raw = newRaw
	request.UserInfo.Username = "system:serviceaccount:kube-system:other"
	if IsStorageMigrationNoop(request) {
		t.Fatal("non-migrator principal received storage migration bypass")
	}
}

func TestReporterLimitAllowsNoopMigrationWithoutConsumingReporterQuota(t *testing.T) {
	next := &countingValidator{}
	guard, err := NewReporterLimitValidator(next, ReporterLimits{MaxLinkSnapshots: 1, MaxResourceSummaries: 1, MaxPhysicalDeviceInventories: 1, QPS: 1, Burst: 1, MaxTrackedPrincipals: 1}, staticReporterCounts{"physicaldeviceinventories": 1})
	if err != nil {
		t.Fatal(err)
	}
	oldRaw := []byte(`{"apiVersion":"spacecompute.k3s.io/v1alpha1","kind":"PhysicalDeviceInventory","metadata":{"name":"i","uid":"u"},"spec":{"x":1},"status":{}}`)
	newRaw := []byte(`{"apiVersion":"spacecompute.k3s.io/v1beta1","kind":"PhysicalDeviceInventory","metadata":{"name":"i","uid":"u"},"spec":{"x":1},"status":{}}`)
	req := &admissionv1.AdmissionRequest{Operation: admissionv1.Update, Resource: metav1.GroupVersionResource{Group: spacev1.GroupName, Version: "v1beta1", Resource: "physicaldeviceinventories"}, UserInfo: authenticationv1.UserInfo{Username: StorageMigratorPrincipal}, Object: runtime.RawExtension{Raw: newRaw}, OldObject: runtime.RawExtension{Raw: oldRaw}}
	if err := guard.Validate(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if next.calls != 1 {
		t.Fatalf("next calls=%d", next.calls)
	}

	create := &admissionv1.AdmissionRequest{Operation: admissionv1.Create, Resource: req.Resource, UserInfo: authenticationv1.UserInfo{Username: "reporter"}}
	if err := guard.Validate(context.Background(), create); err == nil || !strings.Contains(err.Error(), "PhysicalDeviceInventory quota") {
		t.Fatalf("inventory quota error=%v", err)
	}
}
