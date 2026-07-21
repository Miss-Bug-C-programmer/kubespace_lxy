// Package workload owns local transfer waiting, deterministic Pod dispatch and
// execution observations. It does not select domains or score Nodes.
package workload

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	"github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"
	spacepolicy "github.com/k3s-io/k3s/contrib/space-compute/pkg/policy"
)

type Store interface {
	GetPod(context.Context, string, string) (*corev1.Pod, error)
	CreatePod(context.Context, *corev1.Pod) (*corev1.Pod, error)
	UpdatePlacementStatus(context.Context, *spacev1.SpacePlacementIntent) error
	Event(context.Context, string, string, string, string, string)
}

// ReconcilePodStatus converts watched local Pod state into monotonic execution
// observations. A result-return agent must explicitly set result-returned=true;
// Pod success alone cannot fabricate delivery across a disconnected link.
func (c *Controller) ReconcilePodStatus(ctx context.Context, mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent, pod *corev1.Pod) (bool, error) {
	clock := c.Clock
	if clock == nil {
		clock = spacev1.RealClock{}
	}
	if mission == nil || placement == nil || pod == nil {
		return false, fmt.Errorf("mission, placement and Pod are required")
	}
	if placement.Status.ActivePod != nil && placement.Status.ActivePod.UID != "" && placement.Status.ActivePod.UID != pod.UID {
		return false, fmt.Errorf("Pod UID is fenced by the active execution")
	}
	phase := "dispatched"
	switch pod.Status.Phase {
	case corev1.PodRunning:
		phase = "running"
	case corev1.PodFailed:
		phase = "failed"
	case corev1.PodSucceeded:
		returned, _ := strconv.ParseBool(pod.Annotations[spacev1.AnnotationResultReturned])
		if mission.Spec.ResultReturnRequired && !returned {
			phase = "return-pending"
		} else {
			phase = "completed"
		}
	}
	checkpointID := pod.Annotations[spacev1.AnnotationCheckpointID]
	if checkpointID != "" && placement.Status.Phase == spacev1.PlacementReplanning {
		phase = "checkpointed"
	}
	if last := placement.Status.LastObservation; last != nil && last.PodUID == string(pod.UID) && last.Phase == phase && last.CheckpointID == checkpointID {
		return false, nil
	}
	observation := spacev1.ExecutionObservation{Sequence: placement.Status.LastObservationSequence + 1, Attempt: placement.Spec.Attempt, PodUID: string(pod.UID), Phase: phase, ObservedAt: metav1.NewTime(clock.Now()), CheckpointID: checkpointID}
	changed, err := planner.ApplyExecutionObservation(placement, mission, observation, clock)
	if err != nil {
		return false, err
	}
	if changed {
		if err := c.Store.UpdatePlacementStatus(ctx, placement); err != nil {
			return false, err
		}
	}
	return changed, nil
}

type Controller struct {
	Store Store
	Clock spacev1.Clock
}

// ReconcileDispatch creates at most one Pod for an attempt. Future-window
// waiting remains here and returns a requeue duration without creating a Pod.
func (c *Controller) ReconcileDispatch(ctx context.Context, mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent, template corev1.PodTemplateSpec) (time.Duration, error) {
	clock := c.Clock
	if clock == nil {
		clock = spacev1.RealClock{}
	}
	if err := spacev1.ValidateMission(mission, clock); err != nil {
		return 0, err
	}
	if err := spacev1.ValidatePlacement(placement, mission); err != nil {
		return 0, err
	}
	if placement.Status.Phase == spacev1.PlacementCompleted || placement.Status.Phase == spacev1.PlacementFailed {
		return 0, nil
	}
	if !placement.Spec.ExpiresAt.After(clock.Now()) {
		placement.Status.Phase = spacev1.PlacementExpired
		return 0, c.Store.UpdatePlacementStatus(ctx, placement)
	}
	if placement.Spec.NotBefore.After(clock.Now()) {
		placement.Status.Phase = spacev1.PlacementTransferPending
		if err := c.Store.UpdatePlacementStatus(ctx, placement); err != nil {
			return 0, err
		}
		return placement.Spec.NotBefore.Time.Sub(clock.Now()), nil
	}
	name := AttemptPodName(mission.Name, placement.Spec.Attempt)
	existing, err := c.Store.GetPod(ctx, mission.Namespace, name)
	if err == nil {
		if existing.Labels[spacev1.LabelPlacementID] != placement.Spec.PlanID {
			return 0, fmt.Errorf("deterministic attempt Pod %s is fenced by a different plan", name)
		}
		if placement.Status.ActivePod == nil || placement.Status.ActivePod.UID != existing.UID {
			placement.Status.ActivePod = &corev1.ObjectReference{Namespace: existing.Namespace, Name: existing.Name, UID: existing.UID}
			placement.Status.Phase = spacev1.PlacementDispatched
			return 0, c.Store.UpdatePlacementStatus(ctx, placement)
		}
		return 0, nil
	}
	if !errors.Is(err, planner.ErrNotFound) {
		return 0, err
	}
	pod, err := BuildAttemptPod(mission, placement, template)
	if err != nil {
		return 0, err
	}
	created, err := c.Store.CreatePod(ctx, pod)
	if err != nil {
		return 0, err
	}
	placement.Status.ActivePod = &corev1.ObjectReference{Namespace: created.Namespace, Name: created.Name, UID: created.UID}
	placement.Status.Phase = spacev1.PlacementDispatched
	if err := c.Store.UpdatePlacementStatus(ctx, placement); err != nil {
		return 0, err
	}
	c.Store.Event(ctx, mission.Namespace, mission.Name, "Normal", "MissionAttemptDispatched", fmt.Sprintf("created attempt %d Pod %s for plan %s", placement.Spec.Attempt, created.Name, placement.Spec.PlanID))
	return 0, nil
}

func BuildAttemptPod(mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent, template corev1.PodTemplateSpec) (*corev1.Pod, error) {
	if mission == nil || placement == nil {
		return nil, fmt.Errorf("mission and placement are required")
	}
	missionIntent := spacepolicy.PodMissionIntent{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "PodMissionIntent"}, MissionUID: string(mission.UID), Spec: mission.Spec}
	podPlacement := spacepolicy.PodPlacement{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "PodPlacement"}, Spec: placement.Spec}
	missionRaw, err := json.Marshal(missionIntent)
	if err != nil {
		return nil, err
	}
	placementRaw, err := json.Marshal(podPlacement)
	if err != nil {
		return nil, err
	}
	pod := &corev1.Pod{ObjectMeta: *template.ObjectMeta.DeepCopy(), Spec: *template.Spec.DeepCopy()}
	pod.Namespace = mission.Namespace
	pod.Name = AttemptPodName(mission.Name, placement.Spec.Attempt)
	pod.GenerateName = ""
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Labels[spacev1.LabelPlacementID] = placement.Spec.PlanID
	pod.Annotations[spacev1.AnnotationMissionIntent] = string(missionRaw)
	pod.Annotations[spacev1.AnnotationPlacement] = string(placementRaw)
	pod.Spec.SchedulerName = "space-compute-scheduler"
	return pod, nil
}

func AttemptPodName(missionName string, attempt int32) string {
	suffix := fmt.Sprintf("-attempt-%d", attempt)
	limit := 253 - len(suffix)
	if len(missionName) > limit {
		missionName = missionName[:limit]
	}
	return missionName + suffix
}
