#!/usr/bin/env python3
from pathlib import Path


def replace_once(path, old, new):
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: marker count {count} for {old[:100]!r}")
    p.write_text(text.replace(old, new, 1))


path = "pkg/scheduler/plugins/gpustability/phase4_test.go"
replace_once(
    path,
    'pod, err := spaceworkload.BuildAttemptPod(mission, decision.Placement, template)',
    'pod, err := spaceworkload.BuildAttemptPodWithLease(mission, decision.Placement, template, phase4ExecutionLease(mission, decision.Placement, now))',
)
replace_once(
    path,
    'pod, err := spaceworkload.BuildAttemptPod(mission, decision.Placement, v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{Name: "processor", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{"iluvatar.com/gpu": resource.MustParse("1")}}}}}})',
    'pod, err := spaceworkload.BuildAttemptPodWithLease(mission, decision.Placement, v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{Name: "processor", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{"iluvatar.com/gpu": resource.MustParse("1")}}}}}}, phase4ExecutionLease(mission, decision.Placement, now))',
)
text = Path(path).read_text()
anchor = 'func readPhase4Node(t *testing.T) *v1.Node {'
if text.count(anchor) != 1:
    raise SystemExit("phase4 lease helper anchor mismatch")
helper = '''func phase4ExecutionLease(mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent, now time.Time) *spacev1.SpaceExecutionLease {
\tfence := spacev1.ExecutionFence{MissionUID: string(mission.UID), PlanID: placement.Spec.PlanID, Attempt: placement.Spec.Attempt, LeaseEpoch: 1, TokenHash: strings.Repeat("d", 64), ExpiresAt: metav1.NewTime(now.Add(30 * time.Minute))}
\treturn &spacev1.SpaceExecutionLease{ObjectMeta: metav1.ObjectMeta{Name: spacev1.ExecutionLeaseName(fence.MissionUID, fence.PlanID, fence.Attempt, fence.LeaseEpoch)}, Spec: spacev1.SpaceExecutionLeaseSpec{Source: placement.Spec.Target, Destination: placement.Spec.Target, Fence: fence, HeartbeatAt: metav1.NewTime(now), MaximumClockSkewSeconds: 2, Provenance: phase4Provenance(1)}}
}

'''
Path(path).write_text(text.replace(anchor, helper + anchor, 1))

integration = "pkg/scheduler/plugins/gpustability/phase4_integration_test.go"
replace_once(
    integration,
    'pod, err := spaceworkload.BuildAttemptPod(mission, decision.Placement, mission.Spec.WorkloadTemplate)',
    'pod, err := spaceworkload.BuildAttemptPodWithLease(mission, decision.Placement, mission.Spec.WorkloadTemplate, phase4ExecutionLease(mission, decision.Placement, now))',
)

print("phase4 scheduler integration fixtures now carry a real execution fence")
