package admission

import (
	"context"
	"fmt"
	"sync"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ReporterLimits struct {
	MaxLinkSnapshots     int
	MaxResourceSummaries int
	QPS                  float64
	Burst                int
	MaxTrackedPrincipals int
}

type ReporterObjectCounter interface {
	Count(resource string) int
}

type admissionStatusError struct {
	code   int32
	reason metav1.StatusReason
	msg    string
}

func (e *admissionStatusError) Error() string { return e.msg }
func (e *admissionStatusError) AdmissionStatus() (int32, metav1.StatusReason) {
	return e.code, e.reason
}

type reporterBucket struct {
	tokens float64
	last   time.Time
}

type ReporterLimitValidator struct {
	next    RequestValidator
	limits  ReporterLimits
	counter ReporterObjectCounter
	now     func() time.Time

	mu      sync.Mutex
	buckets map[string]reporterBucket
}

func NewReporterLimitValidator(next RequestValidator, limits ReporterLimits, counter ReporterObjectCounter) (*ReporterLimitValidator, error) {
	if next == nil {
		return nil, fmt.Errorf("next reporter validator is required")
	}
	if limits.MaxLinkSnapshots < 1 || limits.MaxResourceSummaries < 1 {
		return nil, fmt.Errorf("reporter object quotas must be positive")
	}
	if limits.QPS <= 0 || limits.QPS > 10000 || limits.Burst < 1 || limits.Burst > 100000 {
		return nil, fmt.Errorf("reporter QPS/burst are out of range")
	}
	if limits.MaxTrackedPrincipals < 1 || limits.MaxTrackedPrincipals > 100000 {
		return nil, fmt.Errorf("max tracked reporter principals is out of range")
	}
	if counter == nil {
		return nil, fmt.Errorf("reporter object counter is required")
	}
	return &ReporterLimitValidator{next: next, limits: limits, counter: counter, now: time.Now, buckets: map[string]reporterBucket{}}, nil
}

func (v *ReporterLimitValidator) Validate(ctx context.Context, request *admissionv1.AdmissionRequest) error {
	if request == nil {
		return v.next.Validate(ctx, request)
	}
	resource := request.Resource.Resource
	if resource == "spacedomainreporterbindings" {
		return v.next.Validate(ctx, request)
	}
	if request.Operation != admissionv1.Create && request.Operation != admissionv1.Update {
		return v.next.Validate(ctx, request)
	}
	principal := request.UserInfo.Username
	if principal == "" {
		return v.next.Validate(ctx, request)
	}
	if !v.allow(principal) {
		return &admissionStatusError{code: 429, reason: metav1.StatusReasonTooManyRequests, msg: "reporter admission rate limit exceeded"}
	}
	if request.Operation == admissionv1.Create {
		switch resource {
		case "spacelinksnapshots":
			if v.counter.Count(resource) >= v.limits.MaxLinkSnapshots {
				return &admissionStatusError{code: 403, reason: metav1.StatusReasonForbidden, msg: fmt.Sprintf("cluster SpaceLinkSnapshot quota %d reached", v.limits.MaxLinkSnapshots)}
			}
		case "spacedomainresourcesummaries":
			if v.counter.Count(resource) >= v.limits.MaxResourceSummaries {
				return &admissionStatusError{code: 403, reason: metav1.StatusReasonForbidden, msg: fmt.Sprintf("cluster SpaceDomainResourceSummary quota %d reached", v.limits.MaxResourceSummaries)}
			}
		}
	}
	return v.next.Validate(ctx, request)
}

func (v *ReporterLimitValidator) allow(principal string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	now := v.now()
	bucket, exists := v.buckets[principal]
	if !exists {
		if len(v.buckets) >= v.limits.MaxTrackedPrincipals {
			return false
		}
		bucket = reporterBucket{tokens: float64(v.limits.Burst), last: now}
	}
	if now.After(bucket.last) {
		bucket.tokens += now.Sub(bucket.last).Seconds() * v.limits.QPS
		if bucket.tokens > float64(v.limits.Burst) {
			bucket.tokens = float64(v.limits.Burst)
		}
		bucket.last = now
	}
	if bucket.tokens < 1 {
		v.buckets[principal] = bucket
		return false
	}
	bucket.tokens--
	v.buckets[principal] = bucket
	return true
}
