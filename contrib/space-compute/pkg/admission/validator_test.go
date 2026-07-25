package admission

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type fakeTrustSource struct {
	bindings map[string]*spacev1.SpaceDomainReporterBinding
	keys     map[string]ed25519.PublicKey
}

func (f *fakeTrustSource) Binding(_ context.Context, principal string) (*spacev1.SpaceDomainReporterBinding, error) {
	value := f.bindings[principal]
	if value == nil {
		return nil, fmt.Errorf("no trusted reporter binding for %q", principal)
	}
	return value.DeepCopy(), nil
}

func (f *fakeTrustSource) PublicKey(_ context.Context, ref spacev1.SecretReference) (ed25519.PublicKey, error) {
	value := f.keys[ref.Key]
	if value == nil {
		return nil, fmt.Errorf("public key %q unavailable", ref.Key)
	}
	return append(ed25519.PublicKey(nil), value...), nil
}

func TestValidatorAcceptsTrustedSignedLinkAndRejectsForgery(t *testing.T) {
	now := time.Date(2026, 7, 24, 6, 0, 0, 123456789, time.UTC)
	principal := "system:serviceaccount:reporters:ground-a"
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	source := spacev1.DomainReference{Name: "ground-a", ClusterID: "ground-cluster", OrbitClass: spacev1.OrbitGround}
	peer := spacev1.DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: spacev1.OrbitLEO}
	ref := spacev1.SecretReference{Namespace: "kube-system", Name: "space-compute-reporter-public-keys", Key: "ground-a"}
	binding := &spacev1.SpaceDomainReporterBinding{
		ObjectMeta: metav1.ObjectMeta{Name: spacev1.ReporterBindingName(principal)},
		Spec: spacev1.SpaceDomainReporterBindingSpec{
			ReporterPrincipal: principal, Domain: source,
			AllowedKinds: []string{"SpaceLinkSnapshot", "SpaceDomainResourceSummary", "SpaceTransferReceipt", "SpaceResultReceipt"},
			AllowedPeers: []spacev1.DomainReference{peer}, PublicKeyRef: ref,
		},
	}
	trust := &fakeTrustSource{bindings: map[string]*spacev1.SpaceDomainReporterBinding{principal: binding}, keys: map[string]ed25519.PublicKey{ref.Key: publicKey}}
	validator, err := NewValidator(trust, fixedClock{now})
	if err != nil {
		t.Fatal(err)
	}
	link := signedLink(t, now, principal, source, peer, privateKey)
	request := admissionRequest(t, admissionv1.Create, "spacelinksnapshots", principal, link, nil)
	if err := validator.Validate(context.Background(), request); err != nil {
		t.Fatalf("valid signed link rejected: %v", err)
	}

	for name, tc := range map[string]struct {
		mutate func(*spacev1.SpaceLinkSnapshot)
		want   string
	}{
		"digest": {func(v *spacev1.SpaceLinkSnapshot) { v.Spec.Windows[0].BandwidthBitsPerSec++ }, "digest mismatch"},
		"signature": {func(v *spacev1.SpaceLinkSnapshot) {
			v.Spec.Provenance.Signature = base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
		}, "signature verification failed"},
		"name":   {func(v *spacev1.SpaceLinkSnapshot) { v.Name = "forged-name" }, "not derived"},
		"domain": {func(v *spacev1.SpaceLinkSnapshot) { v.Spec.Source.Name = "ground-b" }, "bound domain"},
		"peer": {func(v *spacev1.SpaceLinkSnapshot) {
			v.Spec.Destination = spacev1.DomainReference{Name: "leo-b", ClusterID: "leo-b-cluster", OrbitClass: spacev1.OrbitLEO}
		}, "allowed peer"},
	} {
		t.Run(name, func(t *testing.T) {
			forged := link.DeepCopy()
			tc.mutate(forged)
			err := validator.Validate(context.Background(), admissionRequest(t, admissionv1.Create, "spacelinksnapshots", principal, forged, nil))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, want fragment %q", err, tc.want)
			}
		})
	}

	wrongPrincipal := admissionRequest(t, admissionv1.Create, "spacelinksnapshots", "system:serviceaccount:reporters:other", link, nil)
	if err := validator.Validate(context.Background(), wrongPrincipal); err == nil || !strings.Contains(err.Error(), "authenticated principal") {
		t.Fatalf("principal forgery error=%v", err)
	}
}

func TestValidatorEnforcesExactDigestChainIdentityAndStabilityUpdate(t *testing.T) {
	now := time.Date(2026, 7, 24, 6, 0, 0, 0, time.UTC)
	principal := "system:serviceaccount:reporters:ground-a"
	publicKey, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	source := spacev1.DomainReference{Name: "ground-a", ClusterID: "ground-cluster", OrbitClass: spacev1.OrbitGround}
	peer := spacev1.DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: spacev1.OrbitLEO}
	peerB := spacev1.DomainReference{Name: "leo-b", ClusterID: "leo-b-cluster", OrbitClass: spacev1.OrbitLEO}
	ref := spacev1.SecretReference{Namespace: "kube-system", Name: "space-compute-reporter-public-keys", Key: "ground-a"}
	binding := &spacev1.SpaceDomainReporterBinding{
		ObjectMeta: metav1.ObjectMeta{Name: spacev1.ReporterBindingName(principal)},
		Spec:       spacev1.SpaceDomainReporterBindingSpec{ReporterPrincipal: principal, Domain: source, AllowedKinds: []string{"SpaceLinkSnapshot"}, AllowedPeers: []spacev1.DomainReference{peer, peerB}, PublicKeyRef: ref},
	}
	validator, _ := NewValidator(&fakeTrustSource{bindings: map[string]*spacev1.SpaceDomainReporterBinding{principal: binding}, keys: map[string]ed25519.PublicKey{ref.Key: publicKey}}, fixedClock{now.Add(30 * time.Second)})
	old := signedLink(t, now, principal, source, peer, privateKey)
	updated := old.DeepCopy()
	updated.Generation = 2
	updated.Spec.Provenance.Sequence = 2
	updated.Spec.Provenance.PreviousDigest = old.Spec.Provenance.Digest
	updated.Spec.ObservedAt = metav1.NewTime(now.Add(20 * time.Second))
	updated.Spec.ValidUntil = metav1.NewTime(now.Add(time.Hour + 20*time.Second))
	updated.Spec.Windows[0].StabilityMilli--
	signReporterObject(t, updated, privateKey)

	request := admissionRequest(t, admissionv1.Update, "spacelinksnapshots", principal, updated, old)
	if err := validator.Validate(context.Background(), request); err != nil {
		t.Fatalf("valid chained stability update rejected: %v", err)
	}

	cases := map[string]struct {
		mutate func(*spacev1.SpaceLinkSnapshot)
		want   string
	}{
		"sequence-gap": {func(v *spacev1.SpaceLinkSnapshot) {
			v.Spec.Provenance.Sequence = 3
			signReporterObject(t, v, privateKey)
		}, "exactly one"},
		"previous-digest": {func(v *spacev1.SpaceLinkSnapshot) {
			v.Spec.Provenance.PreviousDigest = strings.Repeat("0", 64)
			signReporterObject(t, v, privateKey)
		}, "previousDigest"},
		"timestamp": {func(v *spacev1.SpaceLinkSnapshot) {
			v.Spec.ObservedAt = old.Spec.ObservedAt
			signReporterObject(t, v, privateKey)
		}, "timestamp"},
		"destination-switch": {func(v *spacev1.SpaceLinkSnapshot) {
			v.Spec.Destination.Name = "leo-b"
			v.Name = spacev1.LinkSnapshotName(v.Spec.Source, v.Spec.Destination)
			signReporterObject(t, v, privateKey)
		}, "stable identity"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			bad := updated.DeepCopy()
			tc.mutate(bad)
			err := validator.Validate(context.Background(), admissionRequest(t, admissionv1.Update, "spacelinksnapshots", principal, bad, old))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, want fragment %q", err, tc.want)
			}
		})
	}
}

func TestValidatorCoversSummaryAndReceiptKinds(t *testing.T) {
	now := time.Date(2026, 7, 24, 6, 0, 0, 0, time.UTC)
	principal := "system:serviceaccount:reporters:leo-a"
	publicKey, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	source := spacev1.DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: spacev1.OrbitLEO}
	peer := spacev1.DomainReference{Name: "ground-a", ClusterID: "ground-cluster", OrbitClass: spacev1.OrbitGround}
	ref := spacev1.SecretReference{Namespace: "kube-system", Name: "space-compute-reporter-public-keys", Key: "leo-a"}
	binding := &spacev1.SpaceDomainReporterBinding{
		ObjectMeta: metav1.ObjectMeta{Name: spacev1.ReporterBindingName(principal)},
		Spec: spacev1.SpaceDomainReporterBindingSpec{
			ReporterPrincipal: principal, Domain: source,
			AllowedKinds: []string{"SpaceDomainResourceSummary", "SpaceTransferReceipt", "SpaceResultReceipt"},
			AllowedPeers: []spacev1.DomainReference{peer}, PublicKeyRef: ref,
		},
	}
	validator, _ := NewValidator(&fakeTrustSource{bindings: map[string]*spacev1.SpaceDomainReporterBinding{principal: binding}, keys: map[string]ed25519.PublicKey{ref.Key: publicKey}}, fixedClock{now})

	summary := &spacev1.SpaceDomainResourceSummary{
		ObjectMeta: metav1.ObjectMeta{Name: spacev1.DomainResourceSummaryName(source)},
		Spec: spacev1.SpaceDomainResourceSummarySpec{
			Domain: source, ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)),
			Provenance: spacev1.Provenance{ReporterID: principal, Source: "exporter-summary", Sequence: 1},
			Devices:    []spacev1.DeviceCapacity{{Class: "gpu", Count: 1, Architectures: []string{"space-cuda"}, ComputeMilli: 1000}},
			Software:   map[string]string{"runtime.spacecompute.k3s.io/cuda": "12.4"}, DataLocations: []string{"frame-a"},
			EnergyHeadroomMilli: 800, ThermalHeadroomMilli: 700, ResilienceMilli: 900,
			MaximumSnapshotAgeSecs: 60, ExporterSnapshotDigest: strings.Repeat("b", 64),
		},
	}
	signReporterObject(t, summary, privateKey)
	if err := validator.Validate(context.Background(), admissionRequest(t, admissionv1.Create, "spacedomainresourcesummaries", principal, summary, nil)); err != nil {
		t.Fatalf("summary rejected: %v", err)
	}

	transfer := &spacev1.SpaceTransferReceipt{
		ObjectMeta: metav1.ObjectMeta{},
		Spec: spacev1.SpaceTransferReceiptSpec{
			TransferID: "transfer-a", MissionUID: "mission-uid", PlanID: "plan-a", Attempt: 1,
			Source: source, Destination: peer, DataID: "frame-a", Bytes: 4096, PayloadDigest: strings.Repeat("c", 64),
			StartedAt: metav1.NewTime(now.Add(-time.Minute)), CompletedAt: metav1.NewTime(now),
			Provenance: spacev1.Provenance{ReporterID: principal, Source: "transfer-agent", Sequence: 1},
		},
	}
	transfer.Name = spacev1.TransferReceiptName(source, peer, transfer.Spec.MissionUID, transfer.Spec.PlanID, transfer.Spec.TransferID)
	signReporterObject(t, transfer, privateKey)
	if err := validator.Validate(context.Background(), admissionRequest(t, admissionv1.Create, "spacetransferreceipts", principal, transfer, nil)); err != nil {
		t.Fatalf("transfer receipt rejected: %v", err)
	}

	result := &spacev1.SpaceResultReceipt{
		Spec: spacev1.SpaceResultReceiptSpec{
			ResultID: "result-a", MissionUID: "mission-uid", PlanID: "plan-a", Attempt: 1,
			Source: source, Destination: peer, Bytes: 1024, PayloadDigest: strings.Repeat("d", 64), CompletedAt: metav1.NewTime(now),
			Provenance: spacev1.Provenance{ReporterID: principal, Source: "result-agent", Sequence: 1},
		},
	}
	result.Name = spacev1.ResultReceiptName(source, peer, result.Spec.MissionUID, result.Spec.PlanID, result.Spec.ResultID)
	signReporterObject(t, result, privateKey)
	if err := validator.Validate(context.Background(), admissionRequest(t, admissionv1.Create, "spaceresultreceipts", principal, result, nil)); err != nil {
		t.Fatalf("result receipt rejected: %v", err)
	}
}

func TestAdmissionHTTPHandlerFailsClosed(t *testing.T) {
	now := time.Date(2026, 7, 24, 6, 0, 0, 0, time.UTC)
	principal := "system:serviceaccount:reporters:ground-a"
	publicKey, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	source := spacev1.DomainReference{Name: "ground-a", ClusterID: "ground-cluster", OrbitClass: spacev1.OrbitGround}
	peer := spacev1.DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: spacev1.OrbitLEO}
	ref := spacev1.SecretReference{Namespace: "kube-system", Name: "space-compute-reporter-public-keys", Key: "ground-a"}
	binding := &spacev1.SpaceDomainReporterBinding{ObjectMeta: metav1.ObjectMeta{Name: spacev1.ReporterBindingName(principal)}, Spec: spacev1.SpaceDomainReporterBindingSpec{ReporterPrincipal: principal, Domain: source, AllowedKinds: []string{"SpaceLinkSnapshot"}, AllowedPeers: []spacev1.DomainReference{peer}, PublicKeyRef: ref}}
	validator, _ := NewValidator(&fakeTrustSource{bindings: map[string]*spacev1.SpaceDomainReporterBinding{principal: binding}, keys: map[string]ed25519.PublicKey{ref.Key: publicKey}}, fixedClock{now})
	handler, _ := NewHandler(validator, 64<<10)
	link := signedLink(t, now, principal, source, peer, privateKey)
	review := admissionv1.AdmissionReview{TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"}, Request: admissionRequest(t, admissionv1.Create, "spacelinksnapshots", principal, link, nil)}
	raw, _ := json.Marshal(review)
	req := httptest.NewRequest(http.MethodPost, "/validate", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("http status=%d body=%s", rec.Code, rec.Body.String())
	}
	var response admissionv1.AdmissionReview
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Response == nil || !response.Response.Allowed || response.Response.UID != review.Request.UID {
		t.Fatalf("unexpected admission response: %+v", response.Response)
	}

	forged := link.DeepCopy()
	forged.Spec.Windows[0].StabilityMilli--
	review.Request = admissionRequest(t, admissionv1.Create, "spacelinksnapshots", principal, forged, nil)
	raw, _ = json.Marshal(review)
	req = httptest.NewRequest(http.MethodPost, "/validate", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Response == nil || response.Response.Allowed || !strings.Contains(response.Response.Result.Message, "digest mismatch") {
		t.Fatalf("forged payload was not denied: %+v", response.Response)
	}
}

func FuzzAdmissionReview(f *testing.F) {
	publicKey, _, _ := ed25519.GenerateKey(rand.Reader)
	principal := "system:serviceaccount:reporters:fuzz"
	domain := spacev1.DomainReference{Name: "ground-fuzz", ClusterID: "ground-fuzz-cluster", OrbitClass: spacev1.OrbitGround}
	ref := spacev1.SecretReference{Namespace: "kube-system", Name: "space-compute-reporter-public-keys", Key: "fuzz"}
	binding := &spacev1.SpaceDomainReporterBinding{
		ObjectMeta: metav1.ObjectMeta{Name: spacev1.ReporterBindingName(principal)},
		Spec:       spacev1.SpaceDomainReporterBindingSpec{ReporterPrincipal: principal, Domain: domain, AllowedKinds: []string{"SpaceDomainResourceSummary"}, PublicKeyRef: ref},
	}
	validator, _ := NewValidator(&fakeTrustSource{bindings: map[string]*spacev1.SpaceDomainReporterBinding{principal: binding}, keys: map[string]ed25519.PublicKey{ref.Key: publicKey}}, fixedClock{time.Date(2026, 7, 24, 6, 0, 0, 0, time.UTC)})
	handler, _ := NewHandler(validator, 64<<10)
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64<<10 {
			return
		}
		request := httptest.NewRequest(http.MethodPost, "/validate", bytes.NewReader(raw))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code < 200 || response.Code > 599 {
			t.Fatalf("invalid HTTP status %d", response.Code)
		}
	})
}

func signedLink(t *testing.T, now time.Time, principal string, source, destination spacev1.DomainReference, privateKey ed25519.PrivateKey) *spacev1.SpaceLinkSnapshot {
	t.Helper()
	value := &spacev1.SpaceLinkSnapshot{
		ObjectMeta: metav1.ObjectMeta{},
		Spec: spacev1.SpaceLinkSnapshotSpec{
			Source: source, Destination: destination, ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)),
			MaximumClockSkewSeconds: 5, MinimumUpdateSeconds: 10, HistoryLimit: 8,
			Provenance: spacev1.Provenance{ReporterID: principal, Source: "signed-contact-product", Sequence: 1},
			Windows:    []spacev1.ContactWindow{{ID: "contact-a", Start: metav1.NewTime(now.Add(time.Minute)), End: metav1.NewTime(now.Add(10 * time.Minute)), BandwidthBitsPerSec: 100_000_000, RTTMicroseconds: 20_000, LossPartsPerMillion: 100, ErrorPartsPerMillion: 10, StabilityMilli: 950, ConfidenceMilli: 900, Predicted: true}},
		},
	}
	value.Name = spacev1.LinkSnapshotName(source, destination)
	signReporterObject(t, value, privateKey)
	return value
}

func signReporterObject(t *testing.T, object runtime.Object, privateKey ed25519.PrivateKey) {
	t.Helper()
	provenance := objectProvenance(t, object)
	provenance.Digest = ""
	provenance.Signature = ""
	digest, err := spacev1.ReporterDigest(object)
	if err != nil {
		t.Fatal(err)
	}
	provenance.Digest = digest
	raw, _ := hex.DecodeString(digest)
	provenance.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, raw))
}

func objectProvenance(t *testing.T, object runtime.Object) *spacev1.Provenance {
	t.Helper()
	switch value := object.(type) {
	case *spacev1.SpaceLinkSnapshot:
		return &value.Spec.Provenance
	case *spacev1.SpaceDomainResourceSummary:
		return &value.Spec.Provenance
	case *spacev1.SpaceTransferReceipt:
		return &value.Spec.Provenance
	case *spacev1.SpaceResultReceipt:
		return &value.Spec.Provenance
	default:
		t.Fatalf("unsupported reporter object %T", object)
		return nil
	}
}

func admissionRequest(t *testing.T, operation admissionv1.Operation, resource, principal string, current, previous runtime.Object) *admissionv1.AdmissionRequest {
	t.Helper()
	raw, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	request := &admissionv1.AdmissionRequest{
		UID: types.UID("admission-test"), Operation: operation,
		Resource: metav1.GroupVersionResource{Group: spacev1.GroupName, Version: "v1alpha1", Resource: resource},
		UserInfo: authenticationUser(principal), Object: runtime.RawExtension{Raw: raw},
	}
	if previous != nil {
		oldRaw, err := json.Marshal(previous)
		if err != nil {
			t.Fatal(err)
		}
		request.OldObject = runtime.RawExtension{Raw: oldRaw}
	}
	return request
}

func authenticationUser(principal string) authenticationv1.UserInfo {
	return authenticationv1.UserInfo{Username: principal}
}
