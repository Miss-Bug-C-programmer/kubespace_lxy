package gpustability

import (
	"context"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	spacepolicy "github.com/k3s-io/k3s/contrib/space-compute/pkg/policy"
	dto "github.com/prometheus/client_model/go"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	Name = "K3SGPUStability"

	AnnotationEnabled          = "gpustability.k3s.io/enabled"
	AnnotationExporterEndpoint = "gpustability.k3s.io/exporter-endpoint" // ignored legacy annotation
	AnnotationExporterPort     = "gpustability.k3s.io/exporter-port"
	AnnotationExporterPath     = "gpustability.k3s.io/exporter-path"
	AnnotationExporterScheme   = "gpustability.k3s.io/exporter-scheme"
	AnnotationExporterProfile  = "gpustability.k3s.io/exporter-profile"
	AnnotationMaxTemperature   = "gpustability.k3s.io/max-temperature-celsius"
	AnnotationMinEligible      = "gpustability.k3s.io/min-eligible-devices"
	AnnotationMinFreeMemoryMiB = "gpustability.k3s.io/min-free-memory-mib"
	AnnotationStatePolicy      = "gpustability.k3s.io/state-policy"

	defaultExporterPort = "32021"
	defaultExporterPath = "/metrics"
	defaultTimeout      = 2 * time.Second
	defaultCacheTTL     = 10 * time.Second
	defaultCacheMax     = 4096

	defaultMaxTemperatureC = 90
	defaultTargetTempC     = 70
	defaultMinClockMHz     = 1
)

var (
	_ framework.PreFilterPlugin = &Plugin{}
	_ framework.FilterPlugin    = &Plugin{}
	_ framework.PreScorePlugin  = &Plugin{}
	_ framework.ScorePlugin     = &Plugin{}
	_ io.Closer                 = &Plugin{}
)

const (
	workloadStateKey framework.StateKey = "K3SGPUStabilityWorkload"
	preScoreStateKey framework.StateKey = "K3SGPUStabilityPreScore"
)

type Plugin struct {
	config    Config
	collector *collector
	handle    framework.Handle
	blocked   *blockedPodIndex
	clock     spacev1.Clock
}

type nodeMetrics struct {
	Profile           string
	Devices           []deviceMetrics
	Fields            map[deviceMetricField]struct{}
	GPUCount          int
	GPUUtilization    float64
	MemoryUtilization float64
	MemoryFreeMiB     float64
	MemoryUsedMiB     float64
	MemoryTotalMiB    float64
	TemperatureC      float64
	SMClockMHz        float64
	MemClockMHz       float64
	PowerUsageW       float64
	Endpoint          string
	FetchedAt         time.Time
	ValidUntil        time.Time
}

type deviceMetrics struct {
	ID                string
	Class             DeviceClass
	Name              string
	UUID              string
	Fields            map[deviceMetricField]struct{}
	Healthy           bool
	HealthKnown       bool
	HealthValue       float64
	GPUUtilization    float64
	MemoryUtilization float64
	MemoryFreeMiB     float64
	MemoryUsedMiB     float64
	MemoryTotalMiB    float64
	TemperatureC      float64
	SMClockMHz        float64
	MemClockMHz       float64
	PowerUsageW       float64
}

func New(ctx context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	cfg, err := configFromArgs(obj)
	if err != nil {
		return nil, fmt.Errorf("configure %s: %w", Name, err)
	}
	registerPluginMetrics()
	if cfg.AllowInsecureHTTP {
		klog.FromContext(ctx).Info("GPU exporter compatibility mode permits unauthenticated plain HTTP; use authenticated HTTPS in production")
	}
	collector, err := newCollector(ctx, cfg, nil)
	if err != nil {
		return nil, fmt.Errorf("create exporter collector: %w", err)
	}
	plugin := &Plugin{
		config: cfg, collector: collector, handle: handle,
		blocked: newBlockedPodIndex(cfg.MaxTrackedPods, cfg.BlockedPodTTL), clock: spacev1.RealClock{},
	}
	collector.setSnapshotListener(plugin.activateForSnapshot)
	if err := collector.registerNodeInformer(handle); err != nil {
		collector.Close()
		return nil, fmt.Errorf("register Node informer handler: %w", err)
	}
	return plugin, nil
}

func (p *Plugin) Name() string { return Name }

func (p *Plugin) Close() error {
	if p.blocked != nil {
		p.blocked.clear()
	}
	if p.collector != nil {
		p.collector.Close()
	}
	return nil
}

type workloadRequirement struct {
	Required            bool
	Observational       bool
	Policy              StatePolicy
	Resources           map[v1.ResourceName]int64
	Classes             map[DeviceClass]int64
	MinFreeMemoryMiB    float64
	MaxTemperatureC     float64
	SoftMinEligible     int64
	RequiredProfiles    map[string]struct{}
	RequiredNodeLabels  map[string]string
	PreferredNodeLabels map[string]string
	Space               *spacepolicy.Requirement
}

func (w *workloadRequirement) Clone() framework.StateData {
	if w == nil {
		return (*workloadRequirement)(nil)
	}
	clone := *w
	clone.Resources = copyResourceDemand(w.Resources)
	clone.Classes = copyClassDemand(w.Classes)
	clone.RequiredProfiles = copyStringSet(w.RequiredProfiles)
	clone.RequiredNodeLabels = copyStringMap(w.RequiredNodeLabels)
	clone.PreferredNodeLabels = copyStringMap(w.PreferredNodeLabels)
	clone.Space = w.Space.Clone()
	return &clone
}

func (p *Plugin) PreFilter(_ context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	requirement, err := p.schedulingRequirement(pod)
	if err != nil {
		return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable, err.Error())
	}
	state.Write(workloadStateKey, requirement)
	if !requirement.Required {
		return nil, framework.NewStatus(framework.Skip)
	}
	return nil, nil
}

func (p *Plugin) PreFilterExtensions() framework.PreFilterExtensions { return nil }

func (p *Plugin) Filter(_ context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	requirement, status := workloadFromState(state)
	if !status.IsSuccess() {
		return status
	}
	if !requirement.Required {
		return nil
	}
	if nodeInfo == nil || nodeInfo.Node() == nil {
		observeFilterDecision(requirement.Policy, "node_missing")
		return framework.NewStatus(framework.Unschedulable, "node information is unavailable")
	}
	nodeName := nodeInfo.Node().Name
	spaceEvaluation := spacepolicy.Evaluate(requirement.Space, nodeInfo.Node(), p.clock)
	if !spaceEvaluation.Feasible {
		p.trackBlocked(nodeName, pod)
		observeFilterDecision(requirement.Policy, spaceEvaluation.ReasonCode)
		return framework.NewStatus(framework.Unschedulable, structuredSpaceReason(spaceEvaluation))
	}
	if reason := labelMismatchReason(nodeInfo.Node().Labels, requirement.RequiredNodeLabels); reason != "" {
		p.trackBlocked(nodeName, pod)
		observeFilterDecision(requirement.Policy, "node_label_incompatible")
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, reason)
	}
	if requirement.Observational {
		observeFilterDecision(requirement.Policy, "observational")
		return nil
	}
	snapshot := p.collector.lookupSnapshotForNodeInfo(nodeInfo)
	if snapshot.State != snapshotFresh && snapshot.TargetGeneration != 0 {
		p.collector.requestRefreshExisting(nodeName, snapshot.TargetGeneration)
	}
	if snapshot.State != snapshotFresh {
		if requirement.Policy == StatePolicyStrict {
			p.trackBlocked(nodeName, pod)
			observeFilterDecision(requirement.Policy, string(snapshot.State))
			return framework.NewStatus(framework.Unschedulable, snapshot.Reason)
		}
		observeFilterDecision(requirement.Policy, "static_fallback")
		return nil
	}
	evaluation := p.evaluateFreshSnapshot(requirement, snapshot.Metrics, snapshot.Resources, nodeInfo.Node().Labels)
	if !evaluation.Compatible {
		p.trackBlocked(nodeName, pod)
		observeFilterDecision(requirement.Policy, evaluation.ReasonCode)
		return framework.NewStatus(framework.Unschedulable, evaluation.Reason)
	}
	if requirement.Policy == StatePolicyStrict && !evaluation.DynamicEligible {
		p.trackBlocked(nodeName, pod)
		observeFilterDecision(requirement.Policy, evaluation.ReasonCode)
		return framework.NewStatus(framework.Unschedulable, evaluation.Reason)
	}
	observeFilterDecision(requirement.Policy, "accepted")
	if p.blocked != nil {
		p.blocked.remove(nodeName, pod)
	}
	return nil
}

func (p *Plugin) trackBlocked(nodeName string, pod *v1.Pod) {
	if p.blocked != nil {
		p.blocked.track(nodeName, pod)
	}
}

type preScoreState struct {
	Nodes map[string]nodeScoreInput
}

func (s *preScoreState) Clone() framework.StateData {
	if s == nil {
		return (*preScoreState)(nil)
	}
	clone := &preScoreState{Nodes: make(map[string]nodeScoreInput, len(s.Nodes))}
	for name, input := range s.Nodes {
		input.Evaluation.Devices = append([]deviceMetrics(nil), input.Evaluation.Devices...)
		clone.Nodes[name] = input
	}
	return clone
}

type nodeScoreInput struct {
	State      snapshotState
	Evaluation nodeEvaluation
	Space      spacepolicy.Evaluation
	Reason     string
}

func (p *Plugin) PreScore(_ context.Context, state *framework.CycleState, _ *v1.Pod, nodes []*framework.NodeInfo) *framework.Status {
	requirement, status := workloadFromState(state)
	if !status.IsSuccess() {
		return status
	}
	if !requirement.Required {
		return framework.NewStatus(framework.Skip)
	}
	result := &preScoreState{Nodes: make(map[string]nodeScoreInput, len(nodes))}
	for _, nodeInfo := range nodes {
		if nodeInfo == nil || nodeInfo.Node() == nil {
			continue
		}
		node := nodeInfo.Node()
		snapshot := p.collector.lookupSnapshotForNodeInfo(nodeInfo)
		if snapshot.State != snapshotFresh && snapshot.TargetGeneration != 0 {
			p.collector.requestRefreshExisting(node.Name, snapshot.TargetGeneration)
		}
		input := nodeScoreInput{State: snapshot.State, Reason: snapshot.Reason, Space: spacepolicy.Evaluate(requirement.Space, node, p.clock)}
		if snapshot.State == snapshotFresh {
			input.Evaluation = p.evaluateFreshSnapshot(requirement, snapshot.Metrics, snapshot.Resources, node.Labels)
			input.Evaluation.applySpace(input.Space)
		}
		result.Nodes[node.Name] = input
	}
	state.Write(preScoreStateKey, result)
	return nil
}

func (p *Plugin) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) (int64, *framework.Status) {
	requirement, status := workloadFromState(state)
	if !status.IsSuccess() {
		return framework.MinNodeScore, status
	}
	if !requirement.Required {
		return framework.MinNodeScore, nil
	}
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return framework.MinNodeScore, framework.NewStatus(framework.Error, "node information is unavailable")
	}
	preScore, status := scoreState(state)
	if !status.IsSuccess() {
		return framework.MinNodeScore, status
	}
	input, ok := preScore.Nodes[nodeInfo.Node().Name]
	if !ok {
		return framework.MinNodeScore, framework.NewStatus(framework.Error, "node was not evaluated during PreScore")
	}
	if !input.Space.Feasible {
		return framework.MinNodeScore, framework.NewStatus(framework.Unschedulable, structuredSpaceReason(input.Space))
	}
	score := p.scoreInput(requirement, input)
	reason := string(input.State)
	if input.State == snapshotFresh {
		reason = input.Evaluation.ReasonCode
	}
	observeScore(requirement.Policy, input.State, reason, score)
	klog.FromContext(ctx).V(4).Info("space-compute score evaluated", "pod", klog.KObj(pod), "node", nodeInfo.Node().Name, "score", score, "reason", reason, "dimensions", input.Evaluation.Dimensions)
	return score, nil
}

func (p *Plugin) ScoreExtensions() framework.ScoreExtensions { return nil }

func workloadFromState(state *framework.CycleState) (*workloadRequirement, *framework.Status) {
	if state == nil {
		return nil, framework.NewStatus(framework.Error, "PreFilter cycle state is unavailable")
	}
	data, err := state.Read(workloadStateKey)
	if err != nil {
		return nil, framework.NewStatus(framework.Error, "PreFilter must run before other K3SGPUStability callbacks")
	}
	requirement, ok := data.(*workloadRequirement)
	if !ok || requirement == nil {
		return nil, framework.NewStatus(framework.Error, "invalid K3SGPUStability workload cycle state")
	}
	return requirement, nil
}

func scoreState(state *framework.CycleState) (*preScoreState, *framework.Status) {
	data, err := state.Read(preScoreStateKey)
	if err != nil {
		return nil, framework.NewStatus(framework.Error, "PreScore must run before Score")
	}
	result, ok := data.(*preScoreState)
	if !ok || result == nil {
		return nil, framework.NewStatus(framework.Error, "invalid K3SGPUStability score cycle state")
	}
	return result, nil
}

func (p *Plugin) schedulingRequirement(pod *v1.Pod) (*workloadRequirement, error) {
	requirement := &workloadRequirement{
		Policy: p.config.DefaultStatePolicy, Resources: map[v1.ResourceName]int64{},
		Classes: map[DeviceClass]int64{}, MaxTemperatureC: p.config.MaxTemperatureC,
	}
	if pod == nil {
		return requirement, nil
	}
	intent, err := parseWorkloadIntent(pod)
	if err != nil {
		return nil, err
	}
	spaceRequirement, err := spacepolicy.ParsePod(pod, p.clock)
	if err != nil {
		return nil, err
	}
	requirement.Space = spaceRequirement
	forced := strings.EqualFold(strings.TrimSpace(pod.Annotations[AnnotationEnabled]), "true") || intent != nil || spaceRequirement != nil
	requests := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{})
	for name, quantity := range requests {
		mapping, managed := p.config.ResourceMappings[name]
		if !managed {
			continue
		}
		if quantity.Sign() < 0 {
			return nil, fmt.Errorf("resource request %q cannot be negative", name)
		}
		count, exact := quantity.AsInt64()
		if !exact {
			return nil, fmt.Errorf("resource request %q must be a whole number", name)
		}
		if count == 0 {
			continue
		}
		requirement.Resources[name] = count
		requirement.Classes[mapping.Class] += count
	}
	requirement.Required = forced || p.config.ScoreAllPods || len(requirement.Resources) > 0
	requirement.Observational = requirement.Required && len(requirement.Resources) == 0
	if requirement.Observational {
		requirement.Policy = StatePolicyBestEffort
	}
	if raw, ok := pod.Annotations[AnnotationStatePolicy]; ok {
		policy := StatePolicy(strings.ToLower(strings.TrimSpace(raw)))
		if !validStatePolicy(policy) {
			return nil, fmt.Errorf("annotation %s must be strict, degraded, or best-effort", AnnotationStatePolicy)
		}
		requirement.Policy = policy
	}
	requirement.MinFreeMemoryMiB, err = annotationNonNegativeFloat(pod.Annotations, AnnotationMinFreeMemoryMiB, 0)
	if err != nil {
		return nil, err
	}
	requirement.MaxTemperatureC, err = annotationFiniteFloat(pod.Annotations, AnnotationMaxTemperature, p.config.MaxTemperatureC)
	if err != nil {
		return nil, err
	}
	if requirement.MaxTemperatureC <= -100 || requirement.MaxTemperatureC > 250 {
		return nil, fmt.Errorf("annotation %s must be greater than -100 and at most 250", AnnotationMaxTemperature)
	}
	if raw, ok := pod.Annotations[AnnotationMinEligible]; ok {
		value, parseErr := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if parseErr != nil || value < 1 {
			return nil, fmt.Errorf("annotation %s must be a positive integer", AnnotationMinEligible)
		}
		if len(requirement.Classes) > 1 {
			return nil, fmt.Errorf("annotation %s is ambiguous for mixed device classes", AnnotationMinEligible)
		}
		if len(requirement.Classes) == 1 {
			for class, count := range requirement.Classes {
				if value > count {
					requirement.Classes[class] = value
				}
			}
		} else {
			requirement.SoftMinEligible = value
		}
	}
	if requirement.Observational && requirement.SoftMinEligible == 0 {
		requirement.SoftMinEligible = 1
	}
	if intent != nil {
		if intent.StatePolicy != "" {
			requirement.Policy = StatePolicy(strings.ToLower(strings.TrimSpace(intent.StatePolicy)))
		}
		if intent.MinFreeMemoryMiB != nil {
			requirement.MinFreeMemoryMiB = *intent.MinFreeMemoryMiB
		}
		if intent.MaxTemperatureC != nil {
			requirement.MaxTemperatureC = *intent.MaxTemperatureC
		}
		if intent.MinEligibleDevices != nil {
			if len(requirement.Classes) > 1 {
				return nil, fmt.Errorf("annotation %s minEligibleDevices is ambiguous for mixed device classes", AnnotationWorkloadIntent)
			}
			if len(requirement.Classes) == 1 {
				for class, count := range requirement.Classes {
					if *intent.MinEligibleDevices > count {
						requirement.Classes[class] = *intent.MinEligibleDevices
					}
				}
			} else {
				requirement.SoftMinEligible = *intent.MinEligibleDevices
			}
		}
		requirement.RequiredProfiles = make(map[string]struct{}, len(intent.RequiredProfiles))
		for _, profile := range intent.RequiredProfiles {
			requirement.RequiredProfiles[profile] = struct{}{}
		}
		requirement.RequiredNodeLabels = copyStringMap(intent.RequiredNodeLabels)
		requirement.PreferredNodeLabels = copyStringMap(intent.PreferredNodeLabels)
	}
	if requirement.Observational && len(requirement.RequiredNodeLabels) > 0 {
		if requirement.PreferredNodeLabels == nil {
			requirement.PreferredNodeLabels = map[string]string{}
		}
		for key, value := range requirement.RequiredNodeLabels {
			if _, exists := requirement.PreferredNodeLabels[key]; !exists {
				requirement.PreferredNodeLabels[key] = value
			}
		}
		requirement.RequiredNodeLabels = nil
	}
	if requirement.Space != nil && !requirement.Observational {
		missionPolicy := StatePolicy(requirement.Space.Mission.Spec.StatePolicy)
		if intent != nil && intent.StatePolicy != "" && StatePolicy(intent.StatePolicy) != missionPolicy {
			return nil, fmt.Errorf("workload and mission intent statePolicy values contradict")
		}
		if raw := strings.TrimSpace(pod.Annotations[AnnotationStatePolicy]); raw != "" && StatePolicy(strings.ToLower(raw)) != missionPolicy {
			return nil, fmt.Errorf("annotation %s contradicts mission intent statePolicy", AnnotationStatePolicy)
		}
		requirement.Policy = missionPolicy
	}
	return requirement, nil
}

type nodeEvaluation struct {
	Compatible       bool
	DynamicEligible  bool
	ReasonCode       string
	Reason           string
	Devices          []deviceMetrics
	DeviceQuality    float64
	FragmentationFit float64
	Dimensions       scoreDimensions
}

type scoreDimensions struct {
	Utilization         float64 `json:"utilization"`
	MemoryHeadroom      float64 `json:"memoryHeadroom"`
	ThermalHeadroom     float64 `json:"thermalHeadroom"`
	EnergyHeadroom      float64 `json:"energyHeadroom"`
	ComputeCapability   float64 `json:"computeCapability"`
	Health              float64 `json:"health"`
	DataLocality        float64 `json:"dataLocality"`
	Fragmentation       float64 `json:"fragmentation"`
	PredictedCompletion float64 `json:"predictedCompletion"`
	LinkRisk            float64 `json:"linkRisk"`
	Resilience          float64 `json:"resilience"`
}

func (e *nodeEvaluation) applySpace(space spacepolicy.Evaluation) {
	if !space.Feasible {
		e.Compatible = false
		e.DynamicEligible = false
		e.ReasonCode = space.ReasonCode
		e.Reason = structuredSpaceReason(space)
		return
	}
	e.Dimensions.PredictedCompletion = space.Dimensions.PredictedCompletion
	if len(space.Explanations) > 0 || space.ReasonCode != "not_space_mission" {
		e.Dimensions.DataLocality = space.Dimensions.DataLocality
	}
	e.Dimensions.LinkRisk = space.Dimensions.LinkRisk
	e.Dimensions.Resilience = space.Dimensions.Resilience
	if space.Degraded {
		e.DynamicEligible = false
		e.ReasonCode = space.ReasonCode
		e.Reason = structuredSpaceReason(space)
	}
}

func (p *Plugin) evaluateFreshSnapshot(requirement *workloadRequirement, metrics nodeMetrics, resources nodeResourceContext, nodeLabels map[string]string) nodeEvaluation {
	evaluation := nodeEvaluation{Compatible: true, DynamicEligible: true, ReasonCode: "accepted"}
	if len(requirement.RequiredProfiles) > 0 {
		if _, accepted := requirement.RequiredProfiles[strings.ToLower(metrics.Profile)]; !accepted {
			return nodeEvaluation{Compatible: false, DynamicEligible: false, ReasonCode: "workload_profile_incompatible", Reason: fmt.Sprintf("exporter profile %q is not accepted by workload intent", metrics.Profile)}
		}
	}
	if requirement.Observational {
		evaluation.Devices = eligibleDevices(metrics.Devices, requirement.MaxTemperatureC, requirement.MinFreeMemoryMiB, p.config.MinSMClockMHz, p.config.MinMemClockMHz)
		if int64(len(evaluation.Devices)) < requirement.SoftMinEligible {
			evaluation.DynamicEligible = false
			evaluation.ReasonCode = "observational_threshold"
			evaluation.Reason = fmt.Sprintf("exporter reported %d eligible device(s), observational target is %d", len(evaluation.Devices), requirement.SoftMinEligible)
		}
		evaluation.finishScores(p.config, requirement, nodeLabels)
		return evaluation
	}

	classes := sortedClassNames(requirement.Classes)
	for _, class := range classes {
		demand := requirement.Classes[class]
		classDevices := devicesForClass(metrics.Devices, class)
		if int64(len(classDevices)) < demand {
			evaluation.Compatible = false
			evaluation.DynamicEligible = false
			evaluation.ReasonCode = "wrong_or_insufficient_class"
			evaluation.Reason = fmt.Sprintf("exporter reported %d %s device(s), workload requires %d", len(classDevices), class, demand)
			return evaluation
		}
		eligible := make([]deviceMetrics, 0, len(classDevices))
		ineligibleReasons := map[string]int{}
		for _, device := range classDevices {
			if reasonCode := device.ineligibilityReason(requirement.MaxTemperatureC, requirement.MinFreeMemoryMiB, p.config.MinSMClockMHz, p.config.MinMemClockMHz); reasonCode != "" {
				ineligibleReasons[reasonCode]++
				continue
			}
			eligible = append(eligible, device)
		}
		if int64(len(eligible)) < demand || len(eligible) != len(classDevices) {
			evaluation.DynamicEligible = false
			reasonCodes := make([]string, 0, len(ineligibleReasons))
			for reasonCode := range ineligibleReasons {
				reasonCodes = append(reasonCodes, reasonCode)
			}
			sort.Strings(reasonCodes)
			primary := "dynamic_threshold"
			if len(reasonCodes) > 0 {
				primary = reasonCodes[0]
			}
			evaluation.ReasonCode = primary
			evaluation.Reason = fmt.Sprintf("only %d of %d %s device(s) satisfy telemetry thresholds (%s)", len(eligible), len(classDevices), class, formatReasonCounts(reasonCodes, ineligibleReasons))
		}
		evaluation.Devices = append(evaluation.Devices, eligible...)
	}
	// Extended resources do not identify the device that a vendor plugin will
	// allocate. Strict per-device thresholds are therefore honest only when the
	// exporter covers every allocatable device of each requested resource and
	// every covered device is eligible.
	for resourceName := range requirement.Resources {
		mapping := p.config.ResourceMappings[resourceName]
		allocatable, known := resources.Allocatable[resourceName]
		observed := int64(len(devicesForClass(metrics.Devices, mapping.Class)))
		if !known || allocatable <= 0 || observed != allocatable {
			evaluation.DynamicEligible = false
			evaluation.ReasonCode = "allocation_identity_unlinked"
			evaluation.Reason = fmt.Sprintf("strict telemetry coverage for %q is %d device(s), Kubernetes allocatable is %d; physical allocation identity is not linked", resourceName, observed, allocatable)
		}
	}
	for resourceName := range requirement.Resources {
		mapping := p.config.ResourceMappings[resourceName]
		if len(mapping.Profiles) > 0 {
			if _, ok := mapping.Profiles[strings.ToLower(metrics.Profile)]; !ok {
				evaluation.Compatible = false
				evaluation.DynamicEligible = false
				evaluation.ReasonCode = "profile_incompatible"
				evaluation.Reason = fmt.Sprintf("exporter profile %q is not compatible with resource %q", metrics.Profile, resourceName)
				return evaluation
			}
		}
	}
	evaluation.finishScores(p.config, requirement, nodeLabels)
	return evaluation
}

func (e *nodeEvaluation) finishScores(cfg Config, requirement *workloadRequirement, nodeLabels map[string]string) {
	devices := append([]deviceMetrics(nil), e.Devices...)
	sort.Slice(devices, func(i, j int) bool {
		if devices[i].Class == devices[j].Class {
			return devices[i].ID < devices[j].ID
		}
		return devices[i].Class < devices[j].Class
	})
	if len(devices) == 0 {
		e.DeviceQuality = 0
	} else {
		total := 0.0
		dimensions := scoreDimensions{}
		for _, device := range devices {
			total += deviceScore(device, cfg, requirement.MaxTemperatureC)
			value := deviceDimensions(device, cfg, requirement.MaxTemperatureC)
			dimensions.Utilization += value.Utilization
			dimensions.MemoryHeadroom += value.MemoryHeadroom
			dimensions.ThermalHeadroom += value.ThermalHeadroom
			dimensions.EnergyHeadroom += value.EnergyHeadroom
			dimensions.ComputeCapability += value.ComputeCapability
			dimensions.Health += value.Health
		}
		e.DeviceQuality = total / float64(len(devices))
		e.Dimensions = dimensions.divide(float64(len(devices)))
	}
	e.Dimensions.DataLocality = preferredLabelScore(nodeLabels, requirement.PreferredNodeLabels)
	if requirement.Observational || len(requirement.Classes) == 0 {
		e.FragmentationFit = 50
		e.Dimensions.Fragmentation = e.FragmentationFit
		return
	}
	totalFit := 0.0
	for class, demand := range requirement.Classes {
		available := int64(0)
		for _, device := range devices {
			if device.Class == class {
				available++
			}
		}
		slack := available - demand
		if slack < 0 {
			slack = 0
		}
		totalFit += 100 / float64(1+slack)
	}
	e.FragmentationFit = totalFit / float64(len(requirement.Classes))
	e.Dimensions.Fragmentation = e.FragmentationFit
}

func (d scoreDimensions) divide(divisor float64) scoreDimensions {
	if divisor == 0 {
		return d
	}
	d.Utilization /= divisor
	d.MemoryHeadroom /= divisor
	d.ThermalHeadroom /= divisor
	d.EnergyHeadroom /= divisor
	d.ComputeCapability /= divisor
	d.Health /= divisor
	return d
}

func preferredLabelScore(actual, preferred map[string]string) float64 {
	if len(preferred) == 0 {
		return 50
	}
	matched := 0
	for key, value := range preferred {
		if actual[key] == value {
			matched++
		}
	}
	return 100 * float64(matched) / float64(len(preferred))
}

func (p *Plugin) scoreInput(requirement *workloadRequirement, input nodeScoreInput) int64 {
	if input.State != snapshotFresh {
		switch requirement.Policy {
		case StatePolicyDegraded:
			return p.config.DegradedScore
		case StatePolicyBestEffort:
			return p.config.BestEffortScore
		default:
			return framework.MinNodeScore
		}
	}
	if !input.Evaluation.Compatible {
		return framework.MinNodeScore
	}
	dimensions := input.Evaluation.Dimensions
	weights := p.config.Scoring
	weighted := dimensions.Utilization*float64(weights.Utilization) +
		dimensions.MemoryHeadroom*float64(weights.MemoryHeadroom) +
		dimensions.ThermalHeadroom*float64(weights.ThermalHeadroom) +
		dimensions.EnergyHeadroom*float64(weights.EnergyHeadroom) +
		dimensions.ComputeCapability*float64(weights.ComputeCapability) +
		dimensions.Health*float64(weights.Health) +
		dimensions.DataLocality*float64(weights.DataLocality) +
		dimensions.Fragmentation*float64(weights.Fragmentation) +
		dimensions.PredictedCompletion*float64(weights.PredictedCompletion) +
		dimensions.LinkRisk*float64(weights.LinkRisk) +
		dimensions.Resilience*float64(weights.Resilience)
	score := int64(math.Round(clampScore(weighted / 100)))
	if !input.Evaluation.DynamicEligible {
		switch requirement.Policy {
		case StatePolicyDegraded:
			if score > p.config.DegradedScore {
				score = p.config.DegradedScore
			}
		case StatePolicyBestEffort:
			if score > p.config.BestEffortScore {
				score = p.config.BestEffortScore
			}
		default:
			return framework.MinNodeScore
		}
	}
	return score
}

func structuredSpaceReason(evaluation spacepolicy.Evaluation) string {
	if len(evaluation.Explanations) == 0 {
		return evaluation.Reason
	}
	parts := make([]string, 0, len(evaluation.Explanations))
	for _, explanation := range evaluation.Explanations {
		parts = append(parts, explanation.Code+":"+explanation.Message)
	}
	sort.Strings(parts)
	return evaluation.ReasonCode + " [" + strings.Join(parts, ",") + "]"
}

func deviceScore(device deviceMetrics, cfg Config, maxTemperature float64) float64 {
	dimensions := deviceDimensions(device, cfg, maxTemperature)
	weights := cfg.Scoring
	totalWeight := weights.Utilization + weights.MemoryHeadroom + weights.ThermalHeadroom +
		weights.EnergyHeadroom + weights.ComputeCapability + weights.Health
	if totalWeight == 0 {
		return 0
	}
	weighted := dimensions.Utilization*float64(weights.Utilization) +
		dimensions.MemoryHeadroom*float64(weights.MemoryHeadroom) +
		dimensions.ThermalHeadroom*float64(weights.ThermalHeadroom) +
		dimensions.EnergyHeadroom*float64(weights.EnergyHeadroom) +
		dimensions.ComputeCapability*float64(weights.ComputeCapability) +
		dimensions.Health*float64(weights.Health)
	return clampScore(weighted / float64(totalWeight))
}

func deviceDimensions(device deviceMetrics, cfg Config, maxTemperature float64) scoreDimensions {
	result := scoreDimensions{}
	if device.hasField(fieldGPUUtilization) {
		result.Utilization = clampScore(100 - device.GPUUtilization)
	}
	if device.hasMemoryCapacity() {
		result.MemoryHeadroom = clampScore(device.MemoryFreeMiB / device.MemoryTotalMiB * 100)
	} else if device.hasField(fieldMemoryUtilization) {
		result.MemoryHeadroom = clampScore(100 - device.MemoryUtilization)
	}
	if device.hasField(fieldTemperatureC) {
		result.ThermalHeadroom = temperatureScore(device.TemperatureC, cfg.TargetTempC, maxTemperature)
	}
	if device.hasField(fieldPowerUsageW) {
		result.EnergyHeadroom = clampScore(100 - device.PowerUsageW/cfg.MaxPowerWatts*100)
	}
	clockFields := 0
	clockEligible := true
	if device.hasField(fieldSMClockMHz) {
		clockFields++
		if device.SMClockMHz < cfg.MinSMClockMHz {
			clockEligible = false
		}
	}
	if device.hasField(fieldMemClockMHz) {
		clockFields++
		if device.MemClockMHz < cfg.MinMemClockMHz {
			clockEligible = false
		}
	}
	if clockEligible {
		result.ComputeCapability = 50 * float64(clockFields)
	}
	if device.HealthKnown && !device.Healthy {
		result.ComputeCapability = 0
	}
	if device.HealthKnown {
		if device.Healthy {
			result.Health = 100
		}
	}
	return result
}

func devicesForClass(devices []deviceMetrics, class DeviceClass) []deviceMetrics {
	result := make([]deviceMetrics, 0, len(devices))
	for _, device := range devices {
		if device.Class == class {
			result = append(result, device)
		}
	}
	return result
}

func eligibleDevices(devices []deviceMetrics, maxTemperature, minFreeMemory, minSMClock, minMemClock float64) []deviceMetrics {
	result := make([]deviceMetrics, 0, len(devices))
	for _, device := range devices {
		if device.eligible(maxTemperature, minFreeMemory, minSMClock, minMemClock) {
			result = append(result, device)
		}
	}
	return result
}

func formatReasonCounts(reasonCodes []string, counts map[string]int) string {
	parts := make([]string, 0, len(reasonCodes))
	for _, reasonCode := range reasonCodes {
		parts = append(parts, fmt.Sprintf("%s: %d device(s)", strings.ReplaceAll(reasonCode, "_", " "), counts[reasonCode]))
	}
	if len(parts) == 0 {
		return "one or more devices"
	}
	return strings.Join(parts, ", ")
}

func sortedClassNames(classes map[DeviceClass]int64) []DeviceClass {
	result := make([]DeviceClass, 0, len(classes))
	for class := range classes {
		result = append(result, class)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func copyResourceDemand(in map[v1.ResourceName]int64) map[v1.ResourceName]int64 {
	out := make(map[v1.ResourceName]int64, len(in))
	for name, value := range in {
		out[name] = value
	}
	return out
}

func copyClassDemand(in map[DeviceClass]int64) map[DeviceClass]int64 {
	out := make(map[DeviceClass]int64, len(in))
	for class, value := range in {
		out[class] = value
	}
	return out
}

func annotationFiniteFloat(annotations map[string]string, name string, fallback float64) (float64, error) {
	raw, ok := annotations[name]
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("annotation %s must be a finite number", name)
	}
	return value, nil
}

func annotationNonNegativeFloat(annotations map[string]string, name string, fallback float64) (float64, error) {
	value, err := annotationFiniteFloat(annotations, name, fallback)
	if err != nil {
		return 0, err
	}
	if value < 0 {
		return 0, fmt.Errorf("annotation %s cannot be negative", name)
	}
	return value, nil
}

func genericAcceleratorMetricNames(suffix string) []string {
	names := make([]string, 0, 4)
	for _, prefix := range []string{"k3s_accelerator_", "k3s_gpu_", "k3s_npu_", "k3s_fpga_"} {
		names = append(names, prefix+suffix)
	}
	return names
}

func appendMetricNames(base []string, extra ...[]string) []string {
	for _, names := range extra {
		base = append(base, names...)
	}
	return base
}

func metricValue(metric *dto.Metric) (float64, bool) {
	if metric.GetGauge() != nil {
		return metric.GetGauge().GetValue(), true
	}
	if metric.GetCounter() != nil {
		return metric.GetCounter().GetValue(), true
	}
	if metric.GetUntyped() != nil {
		return metric.GetUntyped().GetValue(), true
	}
	return 0, false
}

func parseMetrics(r io.Reader) (nodeMetrics, error) {
	return parseMetricsForProfile(r, defaultMetricProfile)
}

func temperatureScore(current, target, maximum float64) float64 {
	if maximum <= target {
		return 0
	}
	if current <= target {
		return 100
	}
	if current >= maximum {
		return 0
	}
	return clampScore((maximum - current) / (maximum - target) * 100)
}

func clampScore(score float64) float64 {
	if math.IsNaN(score) || math.IsInf(score, 0) || score < float64(framework.MinNodeScore) {
		return float64(framework.MinNodeScore)
	}
	if score > float64(framework.MaxNodeScore) {
		return float64(framework.MaxNodeScore)
	}
	return score
}

func sum(values []float64) float64 {
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total
}

func avg(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	return sum(values) / float64(len(values))
}

func max(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	result := values[0]
	for _, value := range values[1:] {
		if value > result {
			result = value
		}
	}
	return result
}

func minPositive(values []float64) float64 {
	result := 0.0
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if result == 0 || value < result {
			result = value
		}
	}
	return result
}
