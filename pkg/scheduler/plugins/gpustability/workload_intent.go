package gpustability

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
)

const (
	AnnotationWorkloadIntent = "gpustability.k3s.io/workload-intent"
	workloadIntentAPIVersion = "gpustability.k3s.io/v1alpha1"
	workloadIntentKind       = "SpaceComputeWorkloadIntent"
)

// WorkloadIntent is a strict, versioned Pod annotation schema. Resource
// quantities deliberately remain in Pod resource requests so Kubernetes owns
// accounting and allocation.
type WorkloadIntent struct {
	metav1.TypeMeta     `json:",inline"`
	StatePolicy         string            `json:"statePolicy,omitempty"`
	MinFreeMemoryMiB    *float64          `json:"minFreeMemoryMiB,omitempty"`
	MaxTemperatureC     *float64          `json:"maxTemperatureC,omitempty"`
	MinEligibleDevices  *int64            `json:"minEligibleDevices,omitempty"`
	RequiredProfiles    []string          `json:"requiredProfiles,omitempty"`
	RequiredNodeLabels  map[string]string `json:"requiredNodeLabels,omitempty"`
	PreferredNodeLabels map[string]string `json:"preferredNodeLabels,omitempty"`
}

func parseWorkloadIntent(pod *v1.Pod) (*WorkloadIntent, error) {
	if pod == nil {
		return nil, nil
	}
	raw := strings.TrimSpace(pod.Annotations[AnnotationWorkloadIntent])
	if raw == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	intent := &WorkloadIntent{}
	if err := decoder.Decode(intent); err != nil {
		return nil, fmt.Errorf("annotation %s: %w", AnnotationWorkloadIntent, err)
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("annotation %s contains multiple JSON values", AnnotationWorkloadIntent)
		}
		return nil, fmt.Errorf("annotation %s contains trailing data: %w", AnnotationWorkloadIntent, err)
	}
	if intent.APIVersion != workloadIntentAPIVersion || intent.Kind != workloadIntentKind {
		return nil, fmt.Errorf("annotation %s must use apiVersion %q and kind %q", AnnotationWorkloadIntent, workloadIntentAPIVersion, workloadIntentKind)
	}
	if intent.StatePolicy != "" && !validStatePolicy(StatePolicy(strings.ToLower(strings.TrimSpace(intent.StatePolicy)))) {
		return nil, fmt.Errorf("annotation %s statePolicy must be strict, degraded, or best-effort", AnnotationWorkloadIntent)
	}
	if intent.MinFreeMemoryMiB != nil && (*intent.MinFreeMemoryMiB < 0 || !finite(*intent.MinFreeMemoryMiB)) {
		return nil, fmt.Errorf("annotation %s minFreeMemoryMiB must be finite and non-negative", AnnotationWorkloadIntent)
	}
	if intent.MaxTemperatureC != nil && (!finite(*intent.MaxTemperatureC) || *intent.MaxTemperatureC <= -100 || *intent.MaxTemperatureC > 250) {
		return nil, fmt.Errorf("annotation %s maxTemperatureC must be finite, greater than -100, and at most 250", AnnotationWorkloadIntent)
	}
	if intent.MinEligibleDevices != nil && *intent.MinEligibleDevices < 1 {
		return nil, fmt.Errorf("annotation %s minEligibleDevices must be positive", AnnotationWorkloadIntent)
	}
	seenProfiles := map[string]struct{}{}
	for i, profile := range intent.RequiredProfiles {
		profile = strings.ToLower(strings.TrimSpace(profile))
		if profile == "" {
			return nil, fmt.Errorf("annotation %s requiredProfiles[%d] is empty", AnnotationWorkloadIntent, i)
		}
		if _, exists := seenProfiles[profile]; exists {
			return nil, fmt.Errorf("annotation %s contains duplicate required profile %q", AnnotationWorkloadIntent, profile)
		}
		seenProfiles[profile] = struct{}{}
		intent.RequiredProfiles[i] = profile
	}
	for field, labels := range map[string]map[string]string{
		"requiredNodeLabels":  intent.RequiredNodeLabels,
		"preferredNodeLabels": intent.PreferredNodeLabels,
	} {
		for key, value := range labels {
			if errs := utilvalidation.IsQualifiedName(key); len(errs) > 0 {
				return nil, fmt.Errorf("annotation %s %s key %q is invalid", AnnotationWorkloadIntent, field, key)
			}
			if errs := utilvalidation.IsValidLabelValue(value); len(errs) > 0 {
				return nil, fmt.Errorf("annotation %s %s value for %q is invalid", AnnotationWorkloadIntent, field, key)
			}
		}
	}
	return intent, nil
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
