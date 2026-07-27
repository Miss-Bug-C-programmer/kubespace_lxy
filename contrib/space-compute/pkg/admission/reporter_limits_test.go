package admission

import (
	"context"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type countingValidator struct{ calls int }

func (v *countingValidator) Validate(context.Context, *admissionv1.AdmissionRequest) error {
	v.calls++
	return nil
}

type staticReporterCounts map[string]int

func (c staticReporterCounts) Count(resource string) int { return c[resource] }

func TestReporterLimitValidatorEnforcesRateAndClusterCounts(t *testing.T) {
	next := &countingValidator{}
	guard, err := NewReporterLimitValidator(next, ReporterLimits{MaxLinkSnapshots: 2, MaxResourceSummaries: 3, MaxPhysicalDeviceInventories: 4, QPS: 1, Burst: 1, MaxTrackedPrincipals: 2}, staticReporterCounts{"spacelinksnapshots": 1})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	guard.now = func() time.Time { return now }
	request := &admissionv1.AdmissionRequest{Operation: admissionv1.Create, Resource: metav1.GroupVersionResource{Group: "spacecompute.k3s.io", Version: "v1alpha1", Resource: "spacelinksnapshots"}, UserInfo: authenticationv1.UserInfo{Username: "reporter-a"}}
	if err := guard.Validate(context.Background(), request); err != nil {
		t.Fatalf("first request: %v", err)
	}
	if err := guard.Validate(context.Background(), request); err == nil {
		t.Fatal("second request exceeded burst but was allowed")
	} else if status, ok := err.(interface {
		AdmissionStatus() (int32, metav1.StatusReason)
	}); !ok {
		t.Fatalf("rate error type=%T", err)
	} else if code, reason := status.AdmissionStatus(); code != 429 || reason != metav1.StatusReasonTooManyRequests {
		t.Fatalf("rate status=%d/%s", code, reason)
	}
	now = now.Add(time.Second)
	if err := guard.Validate(context.Background(), request); err != nil {
		t.Fatalf("refilled request: %v", err)
	}

	quotaGuard, err := NewReporterLimitValidator(next, ReporterLimits{MaxLinkSnapshots: 1, MaxResourceSummaries: 3, MaxPhysicalDeviceInventories: 4, QPS: 10, Burst: 10, MaxTrackedPrincipals: 2}, staticReporterCounts{"spacelinksnapshots": 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := quotaGuard.Validate(context.Background(), request); err == nil {
		t.Fatal("cluster link quota was not enforced")
	} else if status, ok := err.(interface {
		AdmissionStatus() (int32, metav1.StatusReason)
	}); !ok {
		t.Fatalf("quota error type=%T", err)
	} else if code, reason := status.AdmissionStatus(); code != 403 || reason != metav1.StatusReasonForbidden {
		t.Fatalf("quota status=%d/%s", code, reason)
	}
	if next.calls != 2 {
		t.Fatalf("downstream validator calls=%d, want 2", next.calls)
	}
}

func TestReporterLimitValidatorBoundsPrincipalCardinality(t *testing.T) {
	next := &countingValidator{}
	guard, err := NewReporterLimitValidator(next, ReporterLimits{MaxLinkSnapshots: 10, MaxResourceSummaries: 10, MaxPhysicalDeviceInventories: 10, QPS: 10, Burst: 10, MaxTrackedPrincipals: 1}, staticReporterCounts{})
	if err != nil {
		t.Fatal(err)
	}
	request := func(user string) *admissionv1.AdmissionRequest {
		return &admissionv1.AdmissionRequest{Operation: admissionv1.Create, Resource: metav1.GroupVersionResource{Resource: "spacetransferreceipts"}, UserInfo: authenticationv1.UserInfo{Username: user}}
	}
	if err := guard.Validate(context.Background(), request("a")); err != nil {
		t.Fatal(err)
	}
	if err := guard.Validate(context.Background(), request("b")); err == nil {
		t.Fatal("second principal should fail closed at cardinality limit")
	}
}
