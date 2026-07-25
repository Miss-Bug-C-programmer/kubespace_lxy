package execution

import (
	"fmt"
	"time"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

func CanDispatch(mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent, lease *spacev1.SpaceExecutionLease, receipts []*spacev1.SpaceTransferReceipt, now time.Time) error {
	if mission == nil || placement == nil || lease == nil {
		return fmt.Errorf("mission, placement and execution lease are required")
	}
	now = now.UTC()
	if !placement.Spec.ExpiresAt.After(now) {
		return fmt.Errorf("placement expired")
	}
	dispatchAt := placement.Spec.ComputeStart.Time.UTC()
	if placement.Spec.NotBefore.Time.After(dispatchAt) {
		dispatchAt = placement.Spec.NotBefore.Time.UTC()
	}
	if now.Before(dispatchAt) {
		return fmt.Errorf("compute start has not arrived")
	}
	if err := ValidateLease(lease, now); err != nil {
		return err
	}
	f := lease.Spec.Fence
	if f.MissionUID != string(mission.UID) || f.PlanID != placement.Spec.PlanID || f.Attempt != placement.Spec.Attempt || lease.Spec.Source != placement.Spec.Target {
		return fmt.Errorf("execution lease does not fence placement")
	}
	for i, epoch := range placement.Spec.InputTransfers {
		digest := ""
		for _, input := range mission.Spec.Inputs {
			if input.ID == epoch.DataID {
				digest = input.PayloadDigest
				break
			}
		}
		if digest == "" {
			return fmt.Errorf("input %q has no trusted payload digest", epoch.DataID)
		}
		transferID := spacev1.InputTransferID(i, epoch.DataID)
		matched := false
		for _, receipt := range receipts {
			if receipt == nil {
				continue
			}
			s := receipt.Spec
			if s.TransferID == transferID && s.MissionUID == string(mission.UID) && s.PlanID == placement.Spec.PlanID && s.Attempt == placement.Spec.Attempt && s.Source == epoch.Source && s.Destination == epoch.Destination && s.DataID == epoch.DataID && s.Bytes == epoch.Bytes && s.PayloadDigest == digest {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("input %q has no matching trusted transfer receipt", epoch.DataID)
		}
	}
	return nil
}

type Report struct {
	MissionUID   string                            `json:"missionUID"`
	PlanID       string                            `json:"planID"`
	Attempt      int32                             `json:"attempt"`
	LeaseEpoch   int64                             `json:"leaseEpoch"`
	Token        string                            `json:"token"`
	Phase        spacev1.ExecutionObservationPhase `json:"phase"`
	CheckpointID string                            `json:"checkpointID,omitempty"`
	ResultDataID string                            `json:"resultDataID,omitempty"`
}

func ValidateReport(report Report, lease *spacev1.SpaceExecutionLease, now time.Time) error {
	if err := ValidateLease(lease, now); err != nil {
		return err
	}
	hash, err := TokenHash(report.Token)
	if err != nil {
		return err
	}
	f := lease.Spec.Fence
	if report.MissionUID != f.MissionUID || report.PlanID != f.PlanID || report.Attempt != f.Attempt || report.LeaseEpoch != f.LeaseEpoch || hash != f.TokenHash {
		return fmt.Errorf("execution report uses stale or foreign fence token")
	}
	switch report.Phase {
	case spacev1.ExecutionObservationHeartbeat, spacev1.ExecutionObservationStopped, spacev1.ExecutionObservationFailed:
		if report.CheckpointID != "" || report.ResultDataID != "" {
			return fmt.Errorf("phase does not accept checkpoint/result payload")
		}
	case spacev1.ExecutionObservationCheckpointed:
		if report.CheckpointID == "" {
			return fmt.Errorf("checkpointed report requires checkpointID")
		}
	case spacev1.ExecutionObservationCompleted:
	default:
		return fmt.Errorf("unsupported report phase")
	}
	return nil
}
