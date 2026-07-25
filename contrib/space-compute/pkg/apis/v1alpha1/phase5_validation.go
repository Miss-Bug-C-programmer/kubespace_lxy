package v1alpha1

import (
	"fmt"
	"strings"
	"time"

	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
)

func ValidateTransferIntent(intent *SpaceTransferIntent, clock Clock) error {
	var errs ValidationErrors
	if intent == nil {
		errs.add("intent", "is required")
		return errs
	}
	if clock == nil {
		errs.add("clock", "is required")
		return errs
	}
	validateReceiptIdentity("spec.transferID", intent.Spec.TransferID, &errs)
	validateReceiptCommon(intent.Spec.MissionUID, intent.Spec.PlanID, intent.Spec.Attempt, intent.Spec.Source, intent.Spec.Destination, intent.Spec.Bytes, intent.Spec.PayloadDigest, Provenance{ReporterID: "local", Source: "intent", Digest: strings.Repeat("0", 64), Sequence: 1}, true, &errs)
	validateDomain("spec.coordinator", intent.Spec.Coordinator, &errs)
	switch intent.Spec.Purpose {
	case TransferPurposeInput:
		if intent.Spec.LeaseEpoch != 0 || intent.Spec.TokenHash != "" {
			errs.add("spec", "input transfer cannot carry execution fence")
		}
	case TransferPurposeResult:
		if intent.Spec.LeaseEpoch < 1 {
			errs.add("spec.leaseEpoch", "result transfer requires a positive lease epoch")
		}
		validateLowerSHA256("spec.tokenHash", intent.Spec.TokenHash, &errs)
	default:
		errs.add("spec.purpose", "must be Input or Result")
	}
	if intent.Spec.DataID == "" || len(intent.Spec.DataID) > 253 || strings.ContainsAny(intent.Spec.DataID, "\r\n\x00") {
		errs.add("spec.dataID", "must be non-empty and bounded")
	}
	if intent.Spec.Window.Start.IsZero() || intent.Spec.Window.End.IsZero() || !intent.Spec.Window.End.After(intent.Spec.Window.Start.Time) {
		errs.add("spec.window", "must have a positive interval")
	}
	if intent.Spec.Window.Bytes != intent.Spec.Bytes {
		errs.add("spec.window.bytes", "must equal spec.bytes")
	}
	if intent.Spec.Window.Source != intent.Spec.Source || intent.Spec.Window.Destination != intent.Spec.Destination {
		errs.add("spec.window", "source/destination must equal transfer intent")
	}
	if intent.Spec.Window.DataID != "" && intent.Spec.Window.DataID != intent.Spec.DataID {
		errs.add("spec.window.dataID", "must be empty or equal spec.dataID")
	}
	if intent.Spec.ExpiresAt.IsZero() || !intent.Spec.ExpiresAt.After(clock.Now()) {
		errs.add("spec.expiresAt", "must be in the future")
	}
	if intent.Name != TransferIntentName(intent.Spec.Source, intent.Spec.Destination, intent.Spec.MissionUID, intent.Spec.PlanID, intent.Spec.TransferID) {
		errs.add("metadata.name", "must be derived from transfer identity")
	}
	return errs.errOrNil()
}

func ValidateExecutionFence(path string, fence ExecutionFence, errs *ValidationErrors) {
	if fence.MissionUID == "" || len(fence.MissionUID) > 128 {
		errs.add(path+".missionUID", "must be non-empty and at most 128 bytes")
	}
	if problems := utilvalidation.IsDNS1123Label(fence.PlanID); len(problems) > 0 {
		errs.add(path+".planID", strings.Join(problems, ", "))
	}
	if fence.Attempt < 1 || fence.Attempt > 100 {
		errs.add(path+".attempt", "must be between 1 and 100")
	}
	if fence.LeaseEpoch < 1 {
		errs.add(path+".leaseEpoch", "must be positive")
	}
	validateLowerSHA256(path+".tokenHash", fence.TokenHash, errs)
	if fence.ExpiresAt.IsZero() {
		errs.add(path+".expiresAt", "is required")
	}
}

func ValidateExecutionLease(lease *SpaceExecutionLease, clock Clock) error {
	var errs ValidationErrors
	if lease == nil {
		errs.add("lease", "is required")
		return errs
	}
	if clock == nil {
		errs.add("clock", "is required")
		return errs
	}
	validateDomain("spec.source", lease.Spec.Source, &errs)
	validateDomain("spec.destination", lease.Spec.Destination, &errs)
	// A local-domain execution still requires a fence. source==destination is
	// therefore valid for locally issued leases and never bypasses signature or
	// epoch/token validation.
	ValidateExecutionFence("spec.fence", lease.Spec.Fence, &errs)
	validateProvenance("spec.provenance", lease.Spec.Provenance, &errs)
	if lease.Spec.HeartbeatAt.IsZero() {
		errs.add("spec.heartbeatAt", "is required")
	}
	if lease.Spec.MaximumClockSkewSeconds < 0 || lease.Spec.MaximumClockSkewSeconds > MaxClockSkewSecs {
		errs.add("spec.maximumClockSkewSeconds", fmt.Sprintf("must be between 0 and %d", MaxClockSkewSecs))
	}
	skew := time.Duration(lease.Spec.MaximumClockSkewSeconds) * time.Second
	if lease.Spec.HeartbeatAt.After(clock.Now().Add(skew)) {
		errs.add("spec.heartbeatAt", "is beyond allowed clock skew")
	}
	if !lease.Spec.Fence.ExpiresAt.After(lease.Spec.HeartbeatAt.Time) {
		errs.add("spec.fence.expiresAt", "must be after heartbeatAt")
	}
	if lease.Name != ExecutionLeaseName(lease.Spec.Fence.MissionUID, lease.Spec.Fence.PlanID, lease.Spec.Fence.Attempt, lease.Spec.Fence.LeaseEpoch) {
		errs.add("metadata.name", "must be derived from fence identity")
	}
	return errs.errOrNil()
}

func ValidateExecutionObservation(observation *SpaceExecutionObservation, clock Clock) error {
	var errs ValidationErrors
	if observation == nil {
		errs.add("observation", "is required")
		return errs
	}
	if clock == nil {
		errs.add("clock", "is required")
		return errs
	}
	validateReceiptIdentity("spec.observationID", observation.Spec.ObservationID, &errs)
	if observation.Spec.MissionUID == "" || len(observation.Spec.MissionUID) > 128 || strings.ContainsAny(observation.Spec.MissionUID, "\r\n\x00") {
		errs.add("spec.missionUID", "must be non-empty and bounded")
	}
	if problems := utilvalidation.IsDNS1123Label(observation.Spec.PlanID); len(problems) > 0 {
		errs.add("spec.planID", strings.Join(problems, ", "))
	}
	if observation.Spec.Attempt < 1 || observation.Spec.Attempt > 100 {
		errs.add("spec.attempt", "must be between 1 and 100")
	}
	validateDomain("spec.source", observation.Spec.Source, &errs)
	validateDomain("spec.destination", observation.Spec.Destination, &errs)
	validateProvenance("spec.provenance", observation.Spec.Provenance, &errs)
	if observation.Spec.Provenance.PreviousDigest != "" {
		validateLowerSHA256("spec.provenance.previousDigest", observation.Spec.Provenance.PreviousDigest, &errs)
	}

	if observation.Spec.LeaseEpoch < 1 {
		errs.add("spec.leaseEpoch", "must be positive")
	}
	validateLowerSHA256("spec.tokenHash", observation.Spec.TokenHash, &errs)
	switch observation.Spec.Phase {
	case ExecutionObservationHeartbeat, ExecutionObservationStopped, ExecutionObservationCompleted, ExecutionObservationFailed:
		if observation.Spec.CheckpointID != "" {
			errs.add("spec.checkpointID", "is allowed only for Checkpointed phase")
		}
	case ExecutionObservationCheckpointed:
		if observation.Spec.CheckpointID == "" || len(observation.Spec.CheckpointID) > 253 {
			errs.add("spec.checkpointID", "is required and bounded for Checkpointed phase")
		}
	default:
		errs.add("spec.phase", "is not a supported execution observation phase")
	}
	if observation.Spec.ObservedAt.IsZero() || observation.Spec.ObservedAt.After(clock.Now().Add(time.Duration(MaxClockSkewSecs)*time.Second)) {
		errs.add("spec.observedAt", "is required and cannot exceed clock skew")
	}
	if observation.Name != ExecutionObservationName(observation.Spec.Source, observation.Spec.Destination, observation.Spec.MissionUID, observation.Spec.PlanID, observation.Spec.ObservationID) {
		errs.add("metadata.name", "must be derived from observation identity")
	}
	return errs.errOrNil()
}
