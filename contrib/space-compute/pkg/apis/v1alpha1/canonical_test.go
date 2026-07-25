package v1alpha1

import (
	"bytes"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCanonicalReporterDigestIsStableAndCoversSchedulingFields(t *testing.T) {
	now := time.Date(2026, 7, 24, 6, 0, 0, 123456789, time.FixedZone("offset", 8*60*60))
	source := DomainReference{Name: "ground-a", ClusterID: "ground-cluster", OrbitClass: OrbitGround}
	destination := DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: OrbitLEO}
	link := &SpaceLinkSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: LinkSnapshotName(source, destination)},
		Spec: SpaceLinkSnapshotSpec{
			Source: source, Destination: destination, ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)),
			MaximumClockSkewSeconds: 5, MinimumUpdateSeconds: 10, HistoryLimit: 8,
			Provenance: Provenance{ReporterID: "reporter", Source: "contact-product", Sequence: 2, PreviousDigest: strings.Repeat("a", 64), Digest: strings.Repeat("b", 64), Signature: "ignored"},
			Windows: []ContactWindow{
				{ID: "b", Start: metav1.NewTime(now.Add(20 * time.Minute)), End: metav1.NewTime(now.Add(30 * time.Minute)), BandwidthBitsPerSec: 20, RTTMicroseconds: 2, LossPartsPerMillion: 3, ErrorPartsPerMillion: 4, StabilityMilli: 500, ConfidenceMilli: 600, Predicted: true},
				{ID: "a", Start: metav1.NewTime(now.Add(10 * time.Minute)), End: metav1.NewTime(now.Add(15 * time.Minute)), BandwidthBitsPerSec: 10, RTTMicroseconds: 1, LossPartsPerMillion: 2, ErrorPartsPerMillion: 3, StabilityMilli: 700, ConfidenceMilli: 800, Predicted: false},
			},
		},
	}
	first, err := ReporterDigest(link)
	if err != nil {
		t.Fatal(err)
	}
	reordered := link.DeepCopy()
	reordered.Spec.Windows[0], reordered.Spec.Windows[1] = reordered.Spec.Windows[1], reordered.Spec.Windows[0]
	reordered.Spec.ObservedAt = metav1.NewTime(now.UTC())
	reordered.Spec.Provenance.Digest = strings.Repeat("c", 64)
	reordered.Spec.Provenance.Signature = "different-signature"
	second, err := ReporterDigest(reordered)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("canonical digest changed for equivalent order/time or excluded digest/signature: %s != %s", first, second)
	}

	stability := link.DeepCopy()
	stability.Spec.Windows[0].StabilityMilli++
	third, _ := ReporterDigest(stability)
	if third == first {
		t.Fatal("stability-only change did not change canonical reporter digest")
	}

	windowBefore := ContactWindowsDigest(link.Spec.Windows)
	windowAfter := ContactWindowsDigest(stability.Spec.Windows)
	if windowBefore == windowAfter {
		t.Fatal("stability-only change did not change contactWindowsDigest")
	}
}

func TestCanonicalResourceSummarySortsMapAndSetLikeFields(t *testing.T) {
	now := time.Date(2026, 7, 24, 6, 0, 0, 0, time.UTC)
	domain := DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: OrbitLEO}
	base := &SpaceDomainResourceSummary{
		ObjectMeta: metav1.ObjectMeta{Name: DomainResourceSummaryName(domain)},
		Spec: SpaceDomainResourceSummarySpec{
			Domain: domain, ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)),
			Provenance: Provenance{ReporterID: "reporter", Source: "exporter", Sequence: 1},
			Devices: []DeviceCapacity{
				{Class: "npu", Count: 1, Architectures: []string{"b", "a"}, Models: []string{"m2", "m1"}, Precision: []string{"int8", "fp16"}, ComputeMilli: 2000, FragmentationMilli: 400},
				{Class: "gpu", Count: 2, ComputeMilli: 1000, FragmentationMilli: 500},
			},
			Software:      map[string]string{"z.example/runtime": "2", "a.example/runtime": "1"},
			DataLocations: []string{"data-b", "data-a"}, QueueDelaySeconds: 2,
			EnergyHeadroomMilli: 800, ThermalHeadroomMilli: 700, ResilienceMilli: 900,
			MinimumEnergyMilli: 200, MinimumThermalMilli: 300, MaximumSnapshotAgeSecs: 60,
			ExporterSnapshotDigest: strings.Repeat("d", 64),
		},
	}
	first, _ := CanonicalReporterBytes(base)
	copy := base.DeepCopy()
	copy.Spec.Devices[0], copy.Spec.Devices[1] = copy.Spec.Devices[1], copy.Spec.Devices[0]
	copy.Spec.Devices[1].Architectures[0], copy.Spec.Devices[1].Architectures[1] = copy.Spec.Devices[1].Architectures[1], copy.Spec.Devices[1].Architectures[0]
	copy.Spec.DataLocations[0], copy.Spec.DataLocations[1] = copy.Spec.DataLocations[1], copy.Spec.DataLocations[0]
	copy.Spec.Software = map[string]string{"a.example/runtime": "1", "z.example/runtime": "2"}
	second, _ := CanonicalReporterBytes(copy)
	if !bytes.Equal(first, second) {
		t.Fatalf("canonical summary depends on map/set insertion order:\n%s\n---\n%s", first, second)
	}
}

func TestDerivedReporterObjectNamesBindFullDomainIdentity(t *testing.T) {
	a := DomainReference{Name: "node", ClusterID: "cluster-a", OrbitClass: OrbitLEO}
	b := DomainReference{Name: "node", ClusterID: "cluster-b", OrbitClass: OrbitLEO}
	if DomainResourceSummaryName(a) == DomainResourceSummaryName(b) {
		t.Fatal("summary name ignored cluster identity")
	}
	if LinkSnapshotName(a, b) == LinkSnapshotName(b, a) {
		t.Fatal("directed link name is not directional")
	}
	if ReporterBindingName("principal-a") == ReporterBindingName("principal-b") {
		t.Fatal("reporter binding names collided")
	}
}

func TestReporterBindingValidationUsesPrincipalDerivedNameAndImmutableDomain(t *testing.T) {
	principal := "system:serviceaccount:reporters:leo-a"
	domain := DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: OrbitLEO}
	binding := &SpaceDomainReporterBinding{
		ObjectMeta: metav1.ObjectMeta{Name: ReporterBindingName(principal)},
		Spec: SpaceDomainReporterBindingSpec{
			ReporterPrincipal: principal, Domain: domain,
			AllowedKinds: []string{"SpaceDomainResourceSummary"},
			PublicKeyRef: SecretReference{Namespace: "kube-system", Name: "space-compute-reporter-public-keys", Key: "leo-a"},
		},
	}
	if err := ValidateReporterBinding(binding, nil); err != nil {
		t.Fatalf("valid binding: %v", err)
	}
	badName := binding.DeepCopy()
	badName.Name = "reporter-arbitrary"
	if err := ValidateReporterBinding(badName, nil); err == nil || !strings.Contains(err.Error(), "metadata.name") {
		t.Fatalf("derived-name error=%v", err)
	}
	moved := binding.DeepCopy()
	moved.Spec.Domain.Name = "leo-b"
	moved.Name = ReporterBindingName(principal)
	if err := ValidateReporterBinding(moved, binding); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("domain immutability error=%v", err)
	}
}
