package planner

import (
	"context"
	"errors"
	"fmt"
	"time"

	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

type MissionKey struct{ Namespace, Name string }

var ErrNotFound = errors.New("not found")

// Repository is the durable boundary. Production implements it with CRDs and
// optimistic concurrency; tests may use the same reconciler with an in-memory
// implementation. ApplyPlacement must be idempotent by mission UID, PlanID and
// material-input digest.
type Repository interface {
	GetMission(context.Context, MissionKey) (*spacev1.SpaceMission, error)
	ListResourceSummaries(context.Context) ([]*spacev1.SpaceDomainResourceSummary, error)
	ListLinkSnapshots(context.Context) ([]*spacev1.SpaceLinkSnapshot, error)
	GetPlacement(context.Context, MissionKey) (*spacev1.SpacePlacementIntent, error)
	ApplyPlacement(context.Context, *spacev1.SpacePlacementIntent, string) (bool, error)
	UpdatePlacementStatus(context.Context, *spacev1.SpacePlacementIntent) error
	UpdateMissionStatus(context.Context, *spacev1.SpaceMission) error
	Event(context.Context, MissionKey, string, string, string)
}

type Observer interface {
	PlanningStarted()
	PlanningFinished(time.Duration, string)
	Replan(string)
	ReconciliationError(string)
	DeadlineSlack(time.Duration)
	SnapshotAge(time.Duration)
	LinkRisk(string)
}

type noopObserver struct{}

func (noopObserver) PlanningStarted()                       {}
func (noopObserver) PlanningFinished(time.Duration, string) {}
func (noopObserver) Replan(string)                          {}
func (noopObserver) ReconciliationError(string)             {}
func (noopObserver) DeadlineSlack(time.Duration)            {}
func (noopObserver) SnapshotAge(time.Duration)              {}
func (noopObserver) LinkRisk(string)                        {}

type Controller struct {
	Repository Repository
	Clock      spacev1.Clock
	Observer   Observer
}

type ReconcileResult struct{ RequeueAfter time.Duration }

func (c *Controller) Reconcile(ctx context.Context, key MissionKey) (ReconcileResult, error) {
	start := c.clock().Now()
	observer := c.observer()
	observer.PlanningStarted()
	mission, err := c.Repository.GetMission(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			observer.PlanningFinished(c.clock().Now().Sub(start), "deleted")
			return ReconcileResult{}, nil
		}
		return c.fail(observer, start, "mission_read", err)
	}
	if !mission.DeletionTimestamp.IsZero() || mission.Spec.Suspend {
		observer.PlanningFinished(c.clock().Now().Sub(start), "suspended")
		return ReconcileResult{}, nil
	}
	if err := spacev1.ValidateMission(mission, c.clock()); err != nil {
		setMissionCondition(mission, c.clock(), metav1.ConditionFalse, "InvalidIntent", boundedMessage(err.Error(), 512))
		mission.Status.Phase = spacev1.MissionBlocked
		if updateErr := c.Repository.UpdateMissionStatus(ctx, mission); updateErr != nil {
			return c.fail(observer, start, "mission_status", updateErr)
		}
		c.Repository.Event(ctx, key, "Warning", "InvalidMissionIntent", boundedMessage(err.Error(), 512))
		observer.PlanningFinished(c.clock().Now().Sub(start), "invalid")
		return ReconcileResult{}, nil
	}
	summaries, err := c.Repository.ListResourceSummaries(ctx)
	if err != nil {
		return c.fail(observer, start, "resource_list", err)
	}
	links, err := c.Repository.ListLinkSnapshots(ctx)
	if err != nil {
		return c.fail(observer, start, "link_list", err)
	}
	for _, summary := range summaries {
		if summary != nil {
			observer.SnapshotAge(c.clock().Now().Sub(summary.Spec.ObservedAt.Time))
		}
	}
	for _, link := range links {
		if link != nil {
			observer.SnapshotAge(c.clock().Now().Sub(link.Spec.ObservedAt.Time))
		}
	}
	decision, err := Plan(mission, summaries, links, c.clock())
	if err != nil {
		setMissionCondition(mission, c.clock(), metav1.ConditionFalse, "NoFeasiblePlan", boundedMessage(err.Error(), 512))
		mission.Status.Phase = spacev1.MissionBlocked
		if updateErr := c.Repository.UpdateMissionStatus(ctx, mission); updateErr != nil {
			return c.fail(observer, start, "mission_status", updateErr)
		}
		c.Repository.Event(ctx, key, "Warning", "MissionPlanningBlocked", boundedMessage(err.Error(), 512))
		observer.PlanningFinished(c.clock().Now().Sub(start), "blocked")
		return ReconcileResult{RequeueAfter: boundedRequeue(mission.Spec.Deadline.Time.Sub(c.clock().Now()))}, nil
	}
	existing, getErr := c.Repository.GetPlacement(ctx, key)
	if getErr != nil && !errors.Is(getErr, ErrNotFound) {
		return c.fail(observer, start, "placement_read", getErr)
	}
	expectedPlanID := ""
	if existing != nil {
		expectedPlanID = existing.Spec.PlanID
		if existing.Spec.MaterialInputDigest == decision.Placement.Spec.MaterialInputDigest && existing.Spec.ExpiresAt.After(c.clock().Now()) {
			mission.Status.Phase = missionPhaseForPlacement(existing.Status.Phase)
			mission.Status.PlacementName = existing.Name
			mission.Status.PlanID = existing.Spec.PlanID
			mission.Status.LastDecisionDigest = existing.Spec.MaterialInputDigest
			setMissionCondition(mission, c.clock(), metav1.ConditionTrue, "PlanCurrent", "existing placement intent matches all material inputs")
			if err := c.Repository.UpdateMissionStatus(ctx, mission); err != nil {
				return c.fail(observer, start, "mission_status", err)
			}
			observer.DeadlineSlack(mission.Spec.Deadline.Time.Sub(existing.Spec.ComputeEnd.Time))
			observer.PlanningFinished(c.clock().Now().Sub(start), "idempotent")
			return ReconcileResult{RequeueAfter: untilExpiry(c.clock().Now(), existing.Spec.ExpiresAt.Time)}, nil
		}
		if existing.Status.Phase == spacev1.PlacementRunning || existing.Status.Phase == spacev1.PlacementDispatched {
			if !mission.Spec.Checkpoint.Checkpointable {
				existing.Status.Phase = spacev1.PlacementFailed
				addPlacementCondition(existing, c.clock(), metav1.ConditionFalse, "MaterialInputChanged", "running non-checkpointable attempt cannot be duplicated during replan")
				if err := c.Repository.UpdatePlacementStatus(ctx, existing); err != nil {
					return c.fail(observer, start, "placement_status", err)
				}
				mission.Status.Phase = spacev1.MissionFailed
				setMissionCondition(mission, c.clock(), metav1.ConditionFalse, "NonCheckpointableReplan", "material inputs changed during a non-checkpointable execution")
				if err := c.Repository.UpdateMissionStatus(ctx, mission); err != nil {
					return c.fail(observer, start, "mission_status", err)
				}
				c.Repository.Event(ctx, key, "Warning", "MissionExecutionFailed", "non-checkpointable execution fenced after material input change")
				observer.Replan("non_checkpointable_failed")
				observer.PlanningFinished(c.clock().Now().Sub(start), "failed")
				return ReconcileResult{}, nil
			}
			existing.Status.Phase = spacev1.PlacementReplanning
			addPlacementCondition(existing, c.clock(), metav1.ConditionTrue, "CheckpointRequired", "fence the current attempt and persist a checkpoint before applying the new plan")
			if err := c.Repository.UpdatePlacementStatus(ctx, existing); err != nil {
				return c.fail(observer, start, "placement_status", err)
			}
			observer.Replan("material_input_changed")
			observer.PlanningFinished(c.clock().Now().Sub(start), "checkpoint_wait")
			return ReconcileResult{RequeueAfter: time.Second}, nil
		}
		decision.Placement.Spec.Attempt = existing.Spec.Attempt + 1
		if decision.Placement.Spec.Attempt > mission.Spec.Retry.MaxAttempts {
			mission.Status.Phase = spacev1.MissionFailed
			setMissionCondition(mission, c.clock(), metav1.ConditionFalse, "RetryBudgetExceeded", "no attempt remains for replanning")
			if err := c.Repository.UpdateMissionStatus(ctx, mission); err != nil {
				return c.fail(observer, start, "mission_status", err)
			}
			observer.PlanningFinished(c.clock().Now().Sub(start), "retry_exhausted")
			return ReconcileResult{}, nil
		}
		observer.Replan(replanReason(existing, decision.Placement, c.clock().Now()))
	}
	changed, err := c.Repository.ApplyPlacement(ctx, decision.Placement, expectedPlanID)
	if err != nil {
		return c.fail(observer, start, "placement_apply", err)
	}
	mission.Status.ObservedGeneration = mission.Generation
	mission.Status.Phase = spacev1.MissionPlanned
	mission.Status.PlacementName = decision.Placement.Name
	mission.Status.PlanID = decision.Placement.Spec.PlanID
	mission.Status.LastDecisionDigest = decision.Placement.Spec.MaterialInputDigest
	setMissionCondition(mission, c.clock(), metav1.ConditionTrue, "PlanReady", "durable placement intent selects a target domain and guarded epoch")
	if err := c.Repository.UpdateMissionStatus(ctx, mission); err != nil {
		return c.fail(observer, start, "mission_status", err)
	}
	if changed {
		c.Repository.Event(ctx, key, "Normal", "MissionPlanned", fmt.Sprintf("plan %s selects domain %s", decision.Placement.Spec.PlanID, decision.Placement.Spec.Target.Name))
	}
	observer.DeadlineSlack(mission.Spec.Deadline.Time.Sub(decision.Placement.Spec.ComputeEnd.Time))
	observer.LinkRisk(linkRiskClass(decision.Placement.Spec.Score.LinkRisk))
	observer.PlanningFinished(c.clock().Now().Sub(start), "planned")
	return ReconcileResult{RequeueAfter: untilExpiry(c.clock().Now(), decision.Placement.Spec.ExpiresAt.Time)}, nil
}

func (c *Controller) fail(observer Observer, start time.Time, reason string, err error) (ReconcileResult, error) {
	observer.ReconciliationError(reason)
	observer.PlanningFinished(c.clock().Now().Sub(start), "error")
	return ReconcileResult{}, err
}
func (c *Controller) clock() spacev1.Clock {
	if c.Clock == nil {
		return spacev1.RealClock{}
	}
	return c.Clock
}
func (c *Controller) observer() Observer {
	if c.Observer == nil {
		return noopObserver{}
	}
	return c.Observer
}
func setMissionCondition(mission *spacev1.SpaceMission, clock spacev1.Clock, status metav1.ConditionStatus, reason, message string) {
	apiMeta.SetStatusCondition(&mission.Status.Conditions, metav1.Condition{Type: "Planned", Status: status, Reason: reason, Message: message, ObservedGeneration: mission.Generation, LastTransitionTime: metav1.NewTime(clock.Now())})
}
func addPlacementCondition(placement *spacev1.SpacePlacementIntent, clock spacev1.Clock, status metav1.ConditionStatus, reason, message string) {
	apiMeta.SetStatusCondition(&placement.Status.Conditions, metav1.Condition{Type: "ExecutionSafe", Status: status, Reason: reason, Message: message, ObservedGeneration: placement.Generation, LastTransitionTime: metav1.NewTime(clock.Now())})
}
func missionPhaseForPlacement(phase spacev1.PlacementPhase) spacev1.MissionPhase {
	switch phase {
	case spacev1.PlacementRunning, spacev1.PlacementDispatched:
		return spacev1.MissionExecuting
	case spacev1.PlacementReturnPending:
		return spacev1.MissionReturning
	case spacev1.PlacementCompleted:
		return spacev1.MissionSucceeded
	case spacev1.PlacementFailed:
		return spacev1.MissionFailed
	case spacev1.PlacementReplanning, spacev1.PlacementExpired:
		return spacev1.MissionReplanning
	default:
		return spacev1.MissionPlanned
	}
}
func boundedRequeue(remaining time.Duration) time.Duration {
	if remaining <= 0 {
		return 0
	}
	if remaining < 30*time.Second {
		return remaining
	}
	return 30 * time.Second
}
func untilExpiry(now, expiry time.Time) time.Duration {
	value := expiry.Sub(now)
	if value <= 0 {
		return 0
	}
	if value > 5*time.Minute {
		return 5 * time.Minute
	}
	return value
}
func replanReason(existing, desired *spacev1.SpacePlacementIntent, now time.Time) string {
	if !existing.Spec.ExpiresAt.After(now) {
		return "plan_expired"
	}
	if existing.Spec.Target != desired.Spec.Target {
		return "target_changed"
	}
	return "material_input_changed"
}
func linkRiskClass(score int32) string {
	if score >= 80 {
		return "low"
	}
	if score >= 50 {
		return "medium"
	}
	return "high"
}
