package conversion

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func TestHandlerConversionReviewAndBounds(t *testing.T) {
	review := apiextensionsv1.ConversionReview{Request: &apiextensionsv1.ConversionRequest{
		UID:               types.UID("handler-1"),
		DesiredAPIVersion: BetaVersion,
		Objects:           []runtime.RawExtension{{Raw: []byte(`{"apiVersion":"spacecompute.k3s.io/v1alpha1","kind":"SpaceMission","metadata":{"name":"m"},"spec":{"future":{"preserved":true}}}`)}},
	}}
	body, err := json.Marshal(review)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/convert", bytes.NewReader(body))
	res := httptest.NewRecorder()
	Handler{MaxBodyBytes: int64(len(body) + 16)}.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
	var converted apiextensionsv1.ConversionReview
	if err := json.Unmarshal(res.Body.Bytes(), &converted); err != nil {
		t.Fatal(err)
	}
	if converted.Response == nil || converted.Response.UID != review.Request.UID || len(converted.Response.ConvertedObjects) != 1 {
		t.Fatalf("response=%+v", converted.Response)
	}
	assertAPIVersion(t, converted.Response.ConvertedObjects[0].Raw, BetaVersion)

	req = httptest.NewRequest(http.MethodPost, "/convert", bytes.NewReader(body))
	res = httptest.NewRecorder()
	Handler{MaxBodyBytes: 32}.ServeHTTP(res, req)
	if res.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status=%d", res.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/convert", nil)
	res = httptest.NewRecorder()
	Handler{}.ServeHTTP(res, req)
	if res.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status=%d", res.Code)
	}
}
