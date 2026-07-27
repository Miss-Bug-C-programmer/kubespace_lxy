// Package conversion implements the CRD conversion boundary between the served
// v1alpha1 compatibility API and the canonical v1beta1 storage API.
package conversion

import (
	"encoding/json"
	"fmt"
	"strings"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

const (
	AlphaVersion = spacev1.GroupName + "/v1alpha1"
	BetaVersion  = spacev1.GroupName + "/v1beta1"
)

var convertibleKinds = map[string]struct{}{
	"SpaceLinkSnapshot":          {},
	"SpaceDomainResourceSummary": {},
	"PhysicalDeviceInventory":    {},
	"SpaceDomainReporterBinding": {},
	"SpaceTransferIntent":        {},
	"SpaceTransferReceipt":       {},
	"SpaceExecutionLease":        {},
	"SpaceExecutionObservation":  {},
	"SpaceResultReceipt":         {},
	"SpaceMission":               {},
	"SpacePlacementIntent":       {},
}

// ConvertRaw performs a lossless structural conversion. Phase 9 deliberately
// keeps every canonical field representable in both served versions; therefore
// conversion changes only apiVersion and preserves unknown JSON members rather
// than round-tripping through a typed struct that could silently drop them.
func ConvertRaw(raw []byte, desiredAPIVersion string) ([]byte, error) {
	if desiredAPIVersion != AlphaVersion && desiredAPIVersion != BetaVersion {
		return nil, fmt.Errorf("unsupported desired apiVersion %q", desiredAPIVersion)
	}
	// Keep every JSON value as RawMessage so large int64 values and fields unknown
	// to this binary never pass through float64 or a typed struct during conversion.
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, fmt.Errorf("decode conversion object: %w", err)
	}
	var kind string
	if err := json.Unmarshal(object["kind"], &kind); err != nil {
		return nil, fmt.Errorf("decode conversion kind: %w", err)
	}
	if _, ok := convertibleKinds[kind]; !ok {
		return nil, fmt.Errorf("unsupported conversion kind %q", kind)
	}
	var apiVersion string
	if err := json.Unmarshal(object["apiVersion"], &apiVersion); err != nil {
		return nil, fmt.Errorf("decode source apiVersion: %w", err)
	}
	if apiVersion != AlphaVersion && apiVersion != BetaVersion {
		return nil, fmt.Errorf("unsupported source apiVersion %q", apiVersion)
	}
	encodedVersion, err := json.Marshal(desiredAPIVersion)
	if err != nil {
		return nil, fmt.Errorf("encode desired apiVersion: %w", err)
	}
	object["apiVersion"] = encodedVersion
	converted, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("encode converted object: %w", err)
	}
	return converted, nil
}

func ConvertReview(review *apiextensionsv1.ConversionReview) *apiextensionsv1.ConversionReview {
	response := &apiextensionsv1.ConversionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "apiextensions.k8s.io/v1", Kind: "ConversionReview"},
		Response: &apiextensionsv1.ConversionResponse{Result: metav1.Status{Status: metav1.StatusSuccess}},
	}
	if review == nil || review.Request == nil {
		response.Response.Result = metav1.Status{Status: metav1.StatusFailure, Message: "conversion request is required", Reason: metav1.StatusReasonInvalid, Code: 400}
		return response
	}
	response.Response.UID = review.Request.UID
	if !strings.HasPrefix(review.Request.DesiredAPIVersion, spacev1.GroupName+"/") {
		response.Response.Result = metav1.Status{Status: metav1.StatusFailure, Message: "desired apiVersion is outside spacecompute.k3s.io", Reason: metav1.StatusReasonInvalid, Code: 400}
		return response
	}
	response.Response.ConvertedObjects = make([]runtime.RawExtension, 0, len(review.Request.Objects))
	for index, object := range review.Request.Objects {
		converted, err := ConvertRaw(object.Raw, review.Request.DesiredAPIVersion)
		if err != nil {
			response.Response.ConvertedObjects = nil
			response.Response.Result = metav1.Status{Status: metav1.StatusFailure, Message: fmt.Sprintf("object %d: %v", index, err), Reason: metav1.StatusReasonInvalid, Code: 400}
			return response
		}
		response.Response.ConvertedObjects = append(response.Response.ConvertedObjects, runtime.RawExtension{Raw: converted})
	}
	return response
}
