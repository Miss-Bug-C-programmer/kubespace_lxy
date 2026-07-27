package conversion

import (
	"encoding/json"
	"reflect"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func TestAlphaBetaConversionRoundTripPreservesCanonicalAndUnknownFields(t *testing.T) {
	original := []byte(`{"apiVersion":"spacecompute.k3s.io/v1alpha1","kind":"SpaceMission","metadata":{"name":"science","namespace":"missions","labels":{"a":"b"}},"spec":{"missionClass":"science","workingMemoryBytes":4096,"workingStorageBytes":8192,"minimumBandwidthBitsPerSecond":1000000,"maximumRTTMicroseconds":50000,"maximumLossPartsPerMillion":100,"futureCanonical":{"nested":[1,"two",true],"largeInteger":1152921504606846975}},"status":{"phase":"Planned","futureStatus":{"sequence":9}},"unknownTop":{"kept":true}}`)
	beta, err := ConvertRaw(original, BetaVersion)
	if err != nil {
		t.Fatal(err)
	}
	assertAPIVersion(t, beta, BetaVersion)
	alpha, err := ConvertRaw(beta, AlphaVersion)
	if err != nil {
		t.Fatal(err)
	}
	assertAPIVersion(t, alpha, AlphaVersion)
	assertEquivalentExceptAPIVersion(t, original, alpha)
}

func TestBetaToAlphaPreservesNewPlacementAndInventoryFields(t *testing.T) {
	cases := [][]byte{
		[]byte(`{"apiVersion":"spacecompute.k3s.io/v1beta1","kind":"SpacePlacementIntent","metadata":{"name":"p","namespace":"n"},"spec":{"selectedCapabilitySetName":"fast","selectedCapabilities":[{"class":"gpu","quantity":2}],"selectedPhysicalDeviceConstraints":[{"class":"gpu","quantity":2,"stableDeviceIDs":["gpu-a","gpu-b"],"allocationIDs":["claim/a","claim/b"]}]},"status":{"transferState":"Completed","transferReceiptReferences":["r1"],"executionLeaseReference":"lease","fencingTokenHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","checkpointReceipt":"checkpoint","resultReceipt":"result","remoteAcknowledgementSequence":7}}`),
		[]byte(`{"apiVersion":"spacecompute.k3s.io/v1beta1","kind":"PhysicalDeviceInventory","metadata":{"name":"inventory"},"spec":{"domain":{"name":"leo-a","clusterID":"leo-cluster","orbitClass":"leo"},"observedAt":"2026-07-27T00:00:00Z","validUntil":"2026-07-27T01:00:00Z","confidenceMilli":900,"provenance":{"reporterID":"r","source":"exporter","digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sequence":1},"devices":[{"stableDeviceID":"gpu-a","kubernetesResourceName":"nvidia.com/gpu","allocationID":"driver/pool/device","class":"gpu","vendor":"nvidia","model":"x","architecture":"sm90","topology":{"numaNode":0,"socketID":"0","pciAddress":"0000:01:00.0"},"totalMemoryBytes":1024,"freeMemoryBytes":512,"health":"healthy","confidenceMilli":900}]}}`),
	}
	for _, raw := range cases {
		alpha, err := ConvertRaw(raw, AlphaVersion)
		if err != nil {
			t.Fatal(err)
		}
		beta, err := ConvertRaw(alpha, BetaVersion)
		if err != nil {
			t.Fatal(err)
		}
		assertEquivalentExceptAPIVersion(t, raw, beta)
	}
}

func TestConvertRawPreservesLargeIntegerExactly(t *testing.T) {
	original := []byte(`{"apiVersion":"spacecompute.k3s.io/v1beta1","kind":"PhysicalDeviceInventory","metadata":{"name":"inventory"},"spec":{"energyLikeFutureField":1152921504606846975}}`)
	alpha, err := ConvertRaw(original, AlphaVersion)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(alpha, &object); err != nil {
		t.Fatal(err)
	}
	var spec map[string]json.RawMessage
	if err := json.Unmarshal(object["spec"], &spec); err != nil {
		t.Fatal(err)
	}
	if got := string(spec["energyLikeFutureField"]); got != "1152921504606846975" {
		t.Fatalf("large integer changed during conversion: %s", got)
	}
}

func TestConversionReviewConvertsBatchAndRejectsForeignVersion(t *testing.T) {
	review := &apiextensionsv1.ConversionReview{Request: &apiextensionsv1.ConversionRequest{
		UID: types.UID("convert-1"), DesiredAPIVersion: BetaVersion,
		Objects: []runtime.RawExtension{{Raw: []byte(`{"apiVersion":"spacecompute.k3s.io/v1alpha1","kind":"SpaceDomainResourceSummary","metadata":{"name":"r"},"spec":{"future":1}}`)}},
	}}
	response := ConvertReview(review)
	if response.Response == nil || response.Response.UID != review.Request.UID || response.Response.Result.Status != metav1.StatusSuccess || len(response.Response.ConvertedObjects) != 1 {
		t.Fatalf("response=%+v", response.Response)
	}
	assertAPIVersion(t, response.Response.ConvertedObjects[0].Raw, BetaVersion)

	review.Request.DesiredAPIVersion = "example.invalid/v1"
	response = ConvertReview(review)
	if response.Response.Result.Status != metav1.StatusFailure || response.Response.Result.Code != 400 {
		t.Fatalf("foreign version response=%+v", response.Response)
	}
}

func FuzzConvertRawPreservesJSON(f *testing.F) {
	f.Add("future", "value")
	f.Add("field", "")
	f.Fuzz(func(t *testing.T, key, value string) {
		if key == "" || key == "apiVersion" || key == "kind" {
			return
		}
		object := map[string]interface{}{"apiVersion": AlphaVersion, "kind": "SpaceMission", key: value}
		raw, err := json.Marshal(object)
		if err != nil {
			return
		}
		beta, err := ConvertRaw(raw, BetaVersion)
		if err != nil {
			t.Fatal(err)
		}
		alpha, err := ConvertRaw(beta, AlphaVersion)
		if err != nil {
			t.Fatal(err)
		}
		assertEquivalentExceptAPIVersion(t, raw, alpha)
	})
}

func assertAPIVersion(t *testing.T, raw []byte, want string) {
	t.Helper()
	var value map[string]interface{}
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	if value["apiVersion"] != want {
		t.Fatalf("apiVersion=%v want=%s", value["apiVersion"], want)
	}
}

func assertEquivalentExceptAPIVersion(t *testing.T, a, b []byte) {
	t.Helper()
	var left, right map[string]interface{}
	if err := json.Unmarshal(a, &left); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &right); err != nil {
		t.Fatal(err)
	}
	delete(left, "apiVersion")
	delete(right, "apiVersion")
	if !reflect.DeepEqual(left, right) {
		t.Fatalf("conversion lost data:\nleft=%#v\nright=%#v", left, right)
	}
}
