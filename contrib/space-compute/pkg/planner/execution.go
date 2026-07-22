package planner

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

// ApplyExecutionObservation is monotonic and idempotent. Duplicate and older
// delivery returns changed=false; it never regresses terminal state.
func ApplyExecutionObservation(placement *spacev1.SpacePlacementIntent, mission *spacev1.SpaceMission, observation spacev1.ExecutionObservation, clock spacev1.Clock) (bool, error) {
	if placement == nil || mission == nil || clock == nil {
		return false, fmt.Errorf("placement, mission and clock are required")
	}
	if observation.Sequence < 1 || observation.Attempt != placement.Spec.Attempt || observation.ObservedAt.IsZero() {
		return false, fmt.Errorf("observation identity, sequence, attempt and time are required")
	}
	if observation.Sequence <= placement.Status.LastObservationSequence {
		return false, nil
	}
	if observation.ObservedAt.After(clock.Now().Add(time.Duration(mission.Spec.MaximumClockSkewSeconds) * time.Second)) {
		return false, fmt.Errorf("observation time exceeds allowed clock skew")
	}
	if terminalPlacement(placement.Status.Phase) {
		return false, nil
	}
	next, err := observationPhase(observation.Phase)
	if err != nil {
		return false, err
	}
	if !allowedPlacementTransition(placement.Status.Phase, next) {
		return false, fmt.Errorf("placement transition %s -> %s is not allowed", placement.Status.Phase, next)
	}
	if next == spacev1.PlacementCheckpointed && (!mission.Spec.Checkpoint.Checkpointable || observation.CheckpointID == "") {
		return false, fmt.Errorf("checkpointed observation requires checkpoint policy and checkpoint ID")
	}
	placement.Status.ObservedGeneration = placement.Generation
	placement.Status.LastObservationSequence = observation.Sequence
	value := observation
	placement.Status.LastObservation = &value
	placement.Status.Phase = next
	if next == spacev1.PlacementCompleted {
		placement.Status.ResultReturned = !mission.Spec.ResultReturnRequired || observation.Phase == "completed"
	}
	addPlacementCondition(placement, clock, metav1.ConditionTrue, "ObservationAccepted", fmt.Sprintf("accepted attempt %d observation sequence %d", observation.Attempt, observation.Sequence))
	return true, nil
}

// HandleLinkPartition enforces declared checkpoint behavior without creating a
// replacement execution. A checkpointable workload is fenced for orchestration;
// a non-checkpointable running workload fails explicitly.
func HandleLinkPartition(placement *spacev1.SpacePlacementIntent, mission *spacev1.SpaceMission, clock spacev1.Clock) error {
	if placement == nil || mission == nil || clock == nil {
		return fmt.Errorf("placement, mission and clock are required")
	}
	if placement.Status.Phase != spacev1.PlacementRunning && placement.Status.Phase != spacev1.PlacementDispatched {
		return nil
	}
	if mission.Spec.Checkpoint.Checkpointable {
		placement.Status.Phase = spacev1.PlacementReplanning
		addPlacementCondition(placement, clock, metav1.ConditionFalse, "LinkPartitionCheckpointRequired", "execution is fenced; workload controller must persist a checkpoint before migration")
		return nil
	}
	placement.Status.Phase = spacev1.PlacementFailed
	addPlacementCondition(placement, clock, metav1.ConditionFalse, "LinkPartitionNonCheckpointable", "link partition failed a non-checkpointable execution; no duplicate Pod was created")
	return nil
}

func observationPhase(value string) (spacev1.PlacementPhase, error) {
	switch value {
	case "dispatched":
		return spacev1.PlacementDispatched, nil
	case "running":
		return spacev1.PlacementRunning, nil
	case "checkpointed":
		return spacev1.PlacementCheckpointed, nil
	case "return-pending":
		return spacev1.PlacementReturnPending, nil
	case "completed":
		return spacev1.PlacementCompleted, nil
	case "failed":
		return spacev1.PlacementFailed, nil
	default:
		return "", fmt.Errorf("unknown execution observation phase %q", value)
	}
}
func terminalPlacement(value spacev1.PlacementPhase) bool {
	return value == spacev1.PlacementCompleted || value == spacev1.PlacementFailed
}
func allowedPlacementTransition(from, to spacev1.PlacementPhase) bool {
	if from == "" {
		from = spacev1.PlacementPending
	}
	allowed := map[spacev1.PlacementPhase]map[spacev1.PlacementPhase]bool{
		spacev1.PlacementPending:         {spacev1.PlacementDispatched: true, spacev1.PlacementRunning: true, spacev1.PlacementFailed: true},
		spacev1.PlacementTransferPending: {spacev1.PlacementDispatched: true, spacev1.PlacementRunning: true, spacev1.PlacementFailed: true},
		spacev1.PlacementReady:           {spacev1.PlacementDispatched: true, spacev1.PlacementRunning: true, spacev1.PlacementFailed: true},
		spacev1.PlacementDispatched:      {spacev1.PlacementDispatched: true, spacev1.PlacementRunning: true, spacev1.PlacementReturnPending: true, spacev1.PlacementCompleted: true, spacev1.PlacementFailed: true},
		spacev1.PlacementRunning:         {spacev1.PlacementRunning: true, spacev1.PlacementCheckpointed: true, spacev1.PlacementReturnPending: true, spacev1.PlacementCompleted: true, spacev1.PlacementFailed: true},
		spacev1.PlacementCheckpointed:    {spacev1.PlacementReplanning: true, spacev1.PlacementFailed: true},
		spacev1.PlacementReplanning:      {spacev1.PlacementCheckpointed: true, spacev1.PlacementFailed: true},
		spacev1.PlacementReturnPending:   {spacev1.PlacementReturnPending: true, spacev1.PlacementCompleted: true, spacev1.PlacementFailed: true},
	}
	return allowed[from][to]
}
