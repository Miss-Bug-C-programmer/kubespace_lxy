// Package planner implements deterministic cross-domain planning. It has no
// Kubernetes scheduler-framework dependency and performs no I/O; controllers
// provide already validated snapshots and durable persistence.
package planner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

const (
	completionWeight    = int32(30)
	localityWeight      = int32(20)
	linkRiskWeight      = int32(20)
	energyWeight        = int32(15)
	resilienceWeight    = int32(10)
	fragmentationWeight = int32(5)
)

type CandidateRejection struct {
	Domain       spacev1.DomainReference
	Explanations []spacev1.ConstraintExplanation
}

type Decision struct {
	Placement *spacev1.SpacePlacementIntent
	Rejected  []CandidateRejection
}

type candidate struct {
	summary          *spacev1.SpaceDomainResourceSummary
	inputTransfers   []spacev1.TransferEpoch
	resultTransfer   *spacev1.TransferEpoch
	computeStart     time.Time
	computeEnd       time.Time
	completion       time.Time
	notBefore        time.Time
	expiresAt        time.Time
	sequences        map[string]int64
	score            spacev1.DecisionScore
	explanations     []spacev1.ConstraintExplanation
	linkQualityMilli int32
	localBytes       int64
	totalBytes       int64
}

// Plan chooses a domain and complete transfer/compute/return epoch. Inputs are
// cloned or read only. The same inputs and clock time produce byte-for-byte
// equivalent placement decisions.
func Plan(mission *spacev1.SpaceMission, summaries []*spacev1.SpaceDomainResourceSummary, links []*spacev1.SpaceLinkSnapshot, clock spacev1.Clock) (Decision, error) {
	if err := spacev1.ValidateMission(mission, clock); err != nil {
		return Decision{}, err
	}
	now := clock.Now().UTC()
	sortedSummaries := append([]*spacev1.SpaceDomainResourceSummary(nil), summaries...)
	sort.SliceStable(sortedSummaries, func(i, j int) bool {
		return domainKey(sortedSummaries[i].Spec.Domain) < domainKey(sortedSummaries[j].Spec.Domain)
	})
	linkIndex := buildLinkIndex(links, clock)
	decision := Decision{}
	var feasible []candidate
	for _, summary := range sortedSummaries {
		if summary == nil {
			continue
		}
		current, rejection := evaluateCandidate(mission, summary, linkIndex, clock, now)
		if len(rejection) > 0 {
			decision.Rejected = append(decision.Rejected, CandidateRejection{Domain: summary.Spec.Domain, Explanations: rejection})
			continue
		}
		feasible = append(feasible, current)
	}
	if len(feasible) == 0 {
		return decision, fmt.Errorf("no feasible domain: %s", summarizeRejections(decision.Rejected))
	}
	sort.SliceStable(feasible, func(i, j int) bool {
		if feasible[i].score.Total != feasible[j].score.Total {
			return feasible[i].score.Total > feasible[j].score.Total
		}
		if !feasible[i].completion.Equal(feasible[j].completion) {
			return feasible[i].completion.Before(feasible[j].completion)
		}
		return domainKey(feasible[i].summary.Spec.Domain) < domainKey(feasible[j].summary.Spec.Domain)
	})
	selected := feasible[0]
	digest, err := materialDigest(mission, sortedSummaries, links)
	if err != nil {
		return decision, fmt.Errorf("calculate material input digest: %w", err)
	}
	planID := "plan-" + digest[:20]
	placementName := mission.Name + "-placement"
	if len(placementName) > 253 {
		placementName = placementName[:232] + "-" + digest[:20]
	}
	placement := &spacev1.SpacePlacementIntent{
		TypeMeta:   metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpacePlacementIntent"},
		ObjectMeta: metav1.ObjectMeta{Name: placementName, Namespace: mission.Namespace, Labels: map[string]string{spacev1.LabelPlacementID: planID, spacev1.LabelMissionUID: string(mission.UID)}},
		Spec: spacev1.SpacePlacementIntentSpec{
			MissionRef: corev1.ObjectReference{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceMission", Namespace: mission.Namespace, Name: mission.Name, UID: mission.UID},
			PlanID:     planID, Attempt: nextAttempt(mission), Target: selected.summary.Spec.Domain,
			NotBefore: metav1.NewTime(selected.computeStart), ExpiresAt: metav1.NewTime(selected.expiresAt),
			ComputeStart: metav1.NewTime(selected.computeStart), ComputeEnd: metav1.NewTime(selected.computeEnd),
			InputTransfers: selected.inputTransfers, ResultTransfer: selected.resultTransfer,
			MaterialInputDigest: digest, SnapshotSequences: selected.sequences, Score: selected.score,
			Explanations: selected.explanations,
		},
		Status: spacev1.SpacePlacementIntentStatus{Phase: initialPlacementPhase(selected, now)},
	}
	if err := spacev1.ValidatePlacement(placement, mission); err != nil {
		return decision, fmt.Errorf("planner produced invalid placement: %w", err)
	}
	decision.Placement = placement
	return decision, nil
}

func evaluateCandidate(mission *spacev1.SpaceMission, summary *spacev1.SpaceDomainResourceSummary, links map[string][]*spacev1.SpaceLinkSnapshot, clock spacev1.Clock, now time.Time) (candidate, []spacev1.ConstraintExplanation) {
	result := candidate{summary: summary, sequences: map[string]int64{}, notBefore: now, expiresAt: mission.Spec.Deadline.Time.UTC(), linkQualityMilli: 1000}
	var rejected []spacev1.ConstraintExplanation
	if !resourceSummaryAccepted(summary) {
		return result, []spacev1.ConstraintExplanation{reject("resource_snapshot_unaccepted", "resource-controller acceptance", fmt.Sprint(summary.Status.ObservedGeneration), fmt.Sprint(summary.Generation), "domain resource summary generation has not been accepted")}
	}
	if err := spacev1.ValidateResourceSummary(summary, clock); err != nil {
		return result, []spacev1.ConstraintExplanation{reject("resource_snapshot_invalid", "resource snapshot validation", err.Error(), "fresh validated summary", "domain resource summary is invalid or stale")}
	}
	result.sequences["resource/"+summary.Name] = summary.Spec.Provenance.Sequence
	result.expiresAt = earlier(result.expiresAt, summary.Spec.ValidUntil.Time.UTC())
	if explanations := capabilityExplanations(mission.Spec, summary.Spec); len(explanations) > 0 {
		rejected = append(rejected, explanations...)
	}
	if missing := softwareMismatch(mission.Spec.RequiredSoftware, summary.Spec.Software); missing != "" {
		rejected = append(rejected, reject("software_incompatible", "required software", missing, "all required versions", "domain software stack is incompatible"))
	}
	energyLow := summary.Spec.EnergyHeadroomMilli < summary.Spec.MinimumEnergyMilli
	thermalLow := summary.Spec.ThermalHeadroomMilli < summary.Spec.MinimumThermalMilli
	if mission.Spec.StatePolicy == spacev1.PolicyStrict && energyLow {
		rejected = append(rejected, reject("energy_below_minimum", "energy headroom", fmt.Sprint(summary.Spec.EnergyHeadroomMilli), fmt.Sprint(summary.Spec.MinimumEnergyMilli), "strict policy rejects insufficient energy headroom"))
	}
	if mission.Spec.StatePolicy == spacev1.PolicyStrict && thermalLow {
		rejected = append(rejected, reject("thermal_below_minimum", "thermal headroom", fmt.Sprint(summary.Spec.ThermalHeadroomMilli), fmt.Sprint(summary.Spec.MinimumThermalMilli), "strict policy rejects insufficient thermal headroom"))
	}
	if len(rejected) > 0 {
		return result, rejected
	}

	cursor := now
	for _, input := range mission.Spec.Inputs {
		result.totalBytes += input.SizeBytes
		if input.SizeBytes == 0 || locationMatchesDomain(input.Locations, summary.Spec.Domain) || contains(summary.Spec.DataLocations, input.ID) {
			result.localBytes += input.SizeBytes
			continue
		}
		transfer, snapshot, ok, reasons := findIngress(input, summary.Spec.Domain, cursor, mission.Spec, links, now)
		if !ok {
			rejected = append(rejected, reasons...)
			continue
		}
		result.inputTransfers = append(result.inputTransfers, transfer)
		result.sequences["link/"+snapshot.Name] = snapshot.Spec.Provenance.Sequence
		result.expiresAt = earlier(result.expiresAt, snapshot.Spec.ValidUntil.Time.UTC())
		result.linkQualityMilli = min32(result.linkQualityMilli, linkQuality(snapshot, transfer.WindowID))
		cursor = transfer.End.Time.UTC()
		if result.notBefore.Equal(now) || transfer.Start.Time.Before(result.notBefore) {
			result.notBefore = transfer.Start.Time.UTC()
		}
	}
	if len(rejected) > 0 {
		return result, rejected
	}
	safety := time.Duration(mission.Spec.SafetyMarginSeconds) * time.Second
	result.computeStart = cursor.Add(safety).Add(time.Duration(summary.Spec.QueueDelaySeconds) * time.Second)
	computeSeconds := predictedComputeSeconds(mission.Spec, summary.Spec)
	result.computeEnd = result.computeStart.Add(time.Duration(computeSeconds) * time.Second)
	result.completion = result.computeEnd
	if mission.Spec.ResultReturnRequired {
		transfer, snapshot, ok, reasons := findEgress(mission.Spec.OutputSizeBytes, summary.Spec.Domain, mission.Spec.ResultDestinations, result.computeEnd, mission.Spec, links, now)
		if !ok {
			return result, append(rejected, reasons...)
		}
		result.resultTransfer = &transfer
		result.sequences["link/"+snapshot.Name] = snapshot.Spec.Provenance.Sequence
		result.expiresAt = earlier(result.expiresAt, snapshot.Spec.ValidUntil.Time.UTC())
		result.linkQualityMilli = min32(result.linkQualityMilli, linkQuality(snapshot, transfer.WindowID))
		result.completion = transfer.End.Time.UTC()
	}
	deadlineGuard := time.Duration(mission.Spec.SafetyMarginSeconds+mission.Spec.MaximumClockSkewSeconds) * time.Second
	if result.completion.Add(deadlineGuard).After(mission.Spec.Deadline.Time) {
		return result, []spacev1.ConstraintExplanation{reject("deadline_missed", "mission deadline", result.completion.Add(deadlineGuard).Format(time.RFC3339Nano), mission.Spec.Deadline.Time.Format(time.RFC3339Nano), "execution or result return cannot complete before the guarded deadline")}
	}
	if !result.expiresAt.After(result.completion) {
		return result, []spacev1.ConstraintExplanation{reject("plan_inputs_expire", "snapshot validity", result.expiresAt.Format(time.RFC3339Nano), result.completion.Format(time.RFC3339Nano), "material snapshot expires before planned completion")}
	}
	result.score = scoreCandidate(result, mission, energyLow, thermalLow, now)
	result.explanations = []spacev1.ConstraintExplanation{
		accept("capabilities_satisfied", "device and software capabilities", "compatible", "compatible"),
		accept("deadline_feasible", "guarded completion", result.completion.Add(deadlineGuard).Format(time.RFC3339Nano), mission.Spec.Deadline.Time.Format(time.RFC3339Nano)),
		scoreExplanation("predicted_completion", result.score.PredictedCompletion),
		scoreExplanation("data_locality", result.score.DataLocality),
		scoreExplanation("link_risk", result.score.LinkRisk),
		scoreExplanation("energy_thermal", result.score.EnergyThermal),
		scoreExplanation("resilience", result.score.Resilience),
		scoreExplanation("fragmentation", result.score.Fragmentation),
	}
	return result, nil
}

func capabilityExplanations(mission spacev1.SpaceMissionSpec, summary spacev1.SpaceDomainResourceSummarySpec) []spacev1.ConstraintExplanation {
	if reason := capacityMismatch(mission.RequiredCapabilities, summary.Devices); reason != "" {
		return []spacev1.ConstraintExplanation{reject("required_capability_missing", "required capabilities", reason, "all required capabilities", "domain cannot satisfy required device capabilities")}
	}
	if len(mission.AlternativeCapabilities) == 0 {
		return nil
	}
	for _, alternative := range mission.AlternativeCapabilities {
		if capacityMismatch(alternative.AllOf, summary.Devices) == "" {
			return nil
		}
	}
	return []spacev1.ConstraintExplanation{reject("alternative_capability_missing", "alternative capability set", "no set satisfied", "one complete alternative set", "domain cannot satisfy any declared alternative")}
}

func capacityMismatch(requirements []spacev1.CapabilityRequirement, capacities []spacev1.DeviceCapacity) string {
	for _, requirement := range requirements {
		matched := int64(0)
		for _, capacity := range capacities {
			if capacity.Class != requirement.Class || !optionalContains(capacity.Architectures, requirement.Architecture) || !optionalContains(capacity.Models, requirement.Model) || !containsAll(capacity.Precision, requirement.Precision) {
				continue
			}
			matched += capacity.Count
		}
		if matched < requirement.Quantity {
			return fmt.Sprintf("class %s has %d compatible, requires %d", requirement.Class, matched, requirement.Quantity)
		}
	}
	return ""
}

func predictedComputeSeconds(mission spacev1.SpaceMissionSpec, summary spacev1.SpaceDomainResourceSummarySpec) int64 {
	computeMilli := int64(0)
	classes := map[string]struct{}{}
	for _, requirement := range mission.RequiredCapabilities {
		classes[requirement.Class] = struct{}{}
	}
	for _, capacity := range summary.Devices {
		if _, needed := classes[capacity.Class]; needed && capacity.ComputeMilli > computeMilli {
			computeMilli = capacity.ComputeMilli
		}
	}
	if computeMilli < 1 {
		return mission.MaximumDurationSeconds
	}
	seconds := ceilDiv(mission.MaximumDurationSeconds*1000, computeMilli)
	if seconds < mission.ExpectedDurationSeconds {
		seconds = mission.ExpectedDurationSeconds
	}
	if seconds > mission.MaximumDurationSeconds {
		seconds = mission.MaximumDurationSeconds
	}
	return seconds
}

func findIngress(input spacev1.DataObject, target spacev1.DomainReference, earliest time.Time, mission spacev1.SpaceMissionSpec, links map[string][]*spacev1.SpaceLinkSnapshot, now time.Time) (spacev1.TransferEpoch, *spacev1.SpaceLinkSnapshot, bool, []spacev1.ConstraintExplanation) {
	locations := append([]string(nil), input.Locations...)
	sort.Strings(locations)
	var best spacev1.TransferEpoch
	var bestSnapshot *spacev1.SpaceLinkSnapshot
	for _, source := range locations {
		for _, snapshot := range links[source+"->"+target.Name] {
			if snapshot.Spec.Destination != target {
				continue
			}
			if transfer, ok := fitTransfer(snapshot, input.SizeBytes, earliest, mission, now); ok && (bestSnapshot == nil || transfer.End.Before(&best.End) || (transfer.End.Equal(&best.End) && snapshot.Name < bestSnapshot.Name)) {
				best, bestSnapshot = transfer, snapshot
			}
		}
	}
	if bestSnapshot == nil {
		return spacev1.TransferEpoch{}, nil, false, []spacev1.ConstraintExplanation{reject("input_transfer_window_missing", "input transfer for "+input.ID, strings.Join(input.Locations, ","), target.Name, "no validated contact window can transfer input before it closes")}
	}
	return best, bestSnapshot, true, nil
}

func findEgress(size int64, source spacev1.DomainReference, destinations []string, earliest time.Time, mission spacev1.SpaceMissionSpec, links map[string][]*spacev1.SpaceLinkSnapshot, now time.Time) (spacev1.TransferEpoch, *spacev1.SpaceLinkSnapshot, bool, []spacev1.ConstraintExplanation) {
	values := append([]string(nil), destinations...)
	sort.Strings(values)
	if locationMatchesDomain(values, source) {
		at := metav1.NewTime(earliest)
		return spacev1.TransferEpoch{WindowID: "local-result", Start: at, End: at, Bytes: size}, &spacev1.SpaceLinkSnapshot{ObjectMeta: metav1.ObjectMeta{Name: "local"}, Spec: spacev1.SpaceLinkSnapshotSpec{Provenance: spacev1.Provenance{Sequence: 1}, ValidUntil: mission.Deadline}}, true, nil
	}
	var best spacev1.TransferEpoch
	var bestSnapshot *spacev1.SpaceLinkSnapshot
	for _, destination := range values {
		for _, snapshot := range links[source.Name+"->"+destination] {
			if snapshot.Spec.Source != source {
				continue
			}
			if transfer, ok := fitTransfer(snapshot, size, earliest, mission, now); ok && (bestSnapshot == nil || transfer.End.Before(&best.End) || (transfer.End.Equal(&best.End) && snapshot.Name < bestSnapshot.Name)) {
				best, bestSnapshot = transfer, snapshot
			}
		}
	}
	if bestSnapshot == nil {
		return spacev1.TransferEpoch{}, nil, false, []spacev1.ConstraintExplanation{reject("result_return_window_missing", "result return", source.Name, strings.Join(destinations, ","), "execution fits but no validated return window completes before deadline")}
	}
	return best, bestSnapshot, true, nil
}

func fitTransfer(snapshot *spacev1.SpaceLinkSnapshot, size int64, earliest time.Time, mission spacev1.SpaceMissionSpec, now time.Time) (spacev1.TransferEpoch, bool) {
	if snapshot == nil || !snapshot.Spec.ValidUntil.After(now) {
		return spacev1.TransferEpoch{}, false
	}
	windows := append([]spacev1.ContactWindow(nil), snapshot.Spec.Windows...)
	sort.SliceStable(windows, func(i, j int) bool {
		if windows[i].Start.Equal(&windows[j].Start) {
			return windows[i].ID < windows[j].ID
		}
		return windows[i].Start.Before(&windows[j].Start)
	})
	for _, window := range windows {
		if window.Predicted && snapshot.Spec.Provenance.Sequence == 0 {
			continue
		}
		skew := time.Duration(mission.MaximumClockSkewSeconds+snapshot.Spec.MaximumClockSkewSeconds) * time.Second
		start := later(earliest, window.Start.Time.Add(skew), now)
		seconds := ceilDiv(size*8, window.BandwidthBitsPerSec) + ceilDiv(window.RTTMicroseconds, 1_000_000)
		if seconds < 1 {
			seconds = 1
		}
		end := start.Add(time.Duration(seconds) * time.Second)
		usableEnd := window.End.Time.Add(-skew).Add(-time.Duration(mission.SafetyMarginSeconds) * time.Second)
		if !end.After(usableEnd) && !end.After(mission.Deadline.Time) {
			return spacev1.TransferEpoch{LinkSnapshotName: snapshot.Name, WindowID: window.ID, Start: metav1.NewTime(start.UTC()), End: metav1.NewTime(end.UTC()), Bytes: size}, true
		}
	}
	return spacev1.TransferEpoch{}, false
}

func buildLinkIndex(links []*spacev1.SpaceLinkSnapshot, clock spacev1.Clock) map[string][]*spacev1.SpaceLinkSnapshot {
	result := map[string][]*spacev1.SpaceLinkSnapshot{}
	for _, link := range links {
		if link != nil && linkSnapshotAccepted(link) && spacev1.ValidateLinkSnapshot(link, nil, clock) == nil {
			key := link.Spec.Source.Name + "->" + link.Spec.Destination.Name
			result[key] = append(result[key], link)
		}
	}
	for key := range result {
		sort.SliceStable(result[key], func(i, j int) bool {
			if result[key][i].Spec.Provenance.Sequence != result[key][j].Spec.Provenance.Sequence {
				return result[key][i].Spec.Provenance.Sequence > result[key][j].Spec.Provenance.Sequence
			}
			return result[key][i].Name < result[key][j].Name
		})
	}
	return result
}

// API-server objects (generation > 0) are usable only after the resource
// controller has accepted that exact generation. Generation-zero values are
// deterministic typed fixtures that still pass the same production validators.
func linkSnapshotAccepted(link *spacev1.SpaceLinkSnapshot) bool {
	if link == nil || link.Generation == 0 {
		return link != nil
	}
	condition := apiMeta.FindStatusCondition(link.Status.Conditions, ConditionLinkValidated)
	return link.Status.ObservedGeneration == link.Generation && link.Status.AcceptedSequence == link.Spec.Provenance.Sequence && condition != nil && condition.ObservedGeneration == link.Generation && condition.Status == metav1.ConditionTrue
}

func resourceSummaryAccepted(summary *spacev1.SpaceDomainResourceSummary) bool {
	if summary == nil || summary.Generation == 0 {
		return summary != nil
	}
	condition := apiMeta.FindStatusCondition(summary.Status.Conditions, "Validated")
	return summary.Status.ObservedGeneration == summary.Generation && condition != nil && condition.ObservedGeneration == summary.Generation && condition.Status == metav1.ConditionTrue
}

func scoreCandidate(value candidate, mission *spacev1.SpaceMission, energyLow, thermalLow bool, now time.Time) spacev1.DecisionScore {
	totalHorizon := mission.Spec.Deadline.Time.Sub(now)
	slack := mission.Spec.Deadline.Time.Sub(value.completion)
	completion := int32(0)
	if totalHorizon > 0 {
		completion = clampMilli(int64(slack) * 1000 / int64(totalHorizon))
	}
	locality := int32(1000)
	if value.totalBytes > 0 {
		locality = clampMilli(value.localBytes * 1000 / value.totalBytes)
	}
	energy := (value.summary.Spec.EnergyHeadroomMilli + value.summary.Spec.ThermalHeadroomMilli) / 2
	if (energyLow || thermalLow) && mission.Spec.StatePolicy != spacev1.PolicyStrict {
		energy /= 2
	}
	fragmentation := int32(0)
	if len(value.summary.Spec.Devices) > 0 {
		for _, device := range value.summary.Spec.Devices {
			fragmentation += device.FragmentationMilli
		}
		fragmentation /= int32(len(value.summary.Spec.Devices))
	}
	result := spacev1.DecisionScore{PredictedCompletion: completion / 10, DataLocality: locality / 10, LinkRisk: value.linkQualityMilli / 10, EnergyThermal: energy / 10, Resilience: value.summary.Spec.ResilienceMilli / 10, Fragmentation: fragmentation / 10}
	weightedMilli := completion*completionWeight + locality*localityWeight + value.linkQualityMilli*linkRiskWeight + energy*energyWeight + value.summary.Spec.ResilienceMilli*resilienceWeight + fragmentation*fragmentationWeight
	result.Total = weightedMilli / 1000
	return result
}

func linkQuality(snapshot *spacev1.SpaceLinkSnapshot, windowID string) int32 {
	if snapshot.Name == "local" || windowID == "local-result" {
		return 1000
	}
	for _, window := range snapshot.Spec.Windows {
		if window.ID == windowID {
			loss := int32((int64(window.LossPartsPerMillion) + int64(window.ErrorPartsPerMillion)) * 1000 / 2_000_000)
			return min32(window.StabilityMilli, window.ConfidenceMilli, 1000-loss)
		}
	}
	return 0
}

func materialDigest(mission *spacev1.SpaceMission, summaries []*spacev1.SpaceDomainResourceSummary, links []*spacev1.SpaceLinkSnapshot) (string, error) {
	type material struct {
		Mission           spacev1.SpaceMissionSpec                 `json:"mission"`
		MissionGeneration int64                                    `json:"missionGeneration"`
		Resources         []spacev1.SpaceDomainResourceSummarySpec `json:"resources"`
		Links             []spacev1.SpaceLinkSnapshotSpec          `json:"links"`
	}
	value := material{Mission: mission.Spec, MissionGeneration: mission.Generation}
	for _, summary := range summaries {
		if summary != nil {
			value.Resources = append(value.Resources, summary.Spec)
		}
	}
	sortedLinks := append([]*spacev1.SpaceLinkSnapshot(nil), links...)
	sort.SliceStable(sortedLinks, func(i, j int) bool {
		if sortedLinks[i] == nil {
			return false
		}
		if sortedLinks[j] == nil {
			return true
		}
		return sortedLinks[i].Name < sortedLinks[j].Name
	})
	for _, link := range sortedLinks {
		if link != nil {
			value.Links = append(value.Links, link.Spec)
		}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func nextAttempt(mission *spacev1.SpaceMission) int32 {
	if mission.Status.PlanID == "" {
		return 1
	}
	return 1
}
func initialPlacementPhase(value candidate, now time.Time) spacev1.PlacementPhase {
	if len(value.inputTransfers) > 0 && value.inputTransfers[0].Start.Time.After(now) {
		return spacev1.PlacementTransferPending
	}
	if len(value.inputTransfers) > 0 {
		return spacev1.PlacementTransferPending
	}
	return spacev1.PlacementReady
}
func domainKey(value spacev1.DomainReference) string {
	return string(value.OrbitClass) + "/" + value.ClusterID + "/" + value.Name
}
func locationMatchesDomain(values []string, domain spacev1.DomainReference) bool {
	return contains(values, domain.Name) || contains(values, domain.ClusterID)
}
func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
func optionalContains(values []string, required string) bool {
	return required == "" || contains(values, required)
}
func containsAll(actual, required []string) bool {
	for _, value := range required {
		if !contains(actual, value) {
			return false
		}
	}
	return true
}
func softwareMismatch(required, actual map[string]string) string {
	keys := make([]string, 0, len(required))
	for key := range required {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if actual[key] != required[key] {
			return fmt.Sprintf("%s=%q, requires %q", key, actual[key], required[key])
		}
	}
	return ""
}
func ceilDiv(value, divisor int64) int64 {
	if value <= 0 {
		return 0
	}
	return 1 + (value-1)/divisor
}
func earlier(values ...time.Time) time.Time {
	result := values[0]
	for _, value := range values[1:] {
		if value.Before(result) {
			result = value
		}
	}
	return result
}
func later(values ...time.Time) time.Time {
	result := values[0]
	for _, value := range values[1:] {
		if value.After(result) {
			result = value
		}
	}
	return result
}
func min32(values ...int32) int32 {
	result := values[0]
	for _, value := range values[1:] {
		if value < result {
			result = value
		}
	}
	return result
}
func clampMilli(value int64) int32 {
	if value < 0 {
		return 0
	}
	if value > 1000 {
		return 1000
	}
	return int32(value)
}
func reject(code, constraint, observed, required, message string) spacev1.ConstraintExplanation {
	return spacev1.ConstraintExplanation{Code: code, Constraint: constraint, Observed: observed, Required: required, Message: message}
}
func accept(code, constraint, observed, required string) spacev1.ConstraintExplanation {
	return spacev1.ConstraintExplanation{Code: code, Constraint: constraint, Observed: observed, Required: required, Message: "constraint satisfied"}
}
func scoreExplanation(component string, value int32) spacev1.ConstraintExplanation {
	return spacev1.ConstraintExplanation{Code: "score_" + component, Constraint: component, Observed: fmt.Sprintf("%d/100", value), Required: "fixed SLO normalization", Message: "deterministic score component"}
}
func summarizeRejections(values []CandidateRejection) string {
	if len(values) == 0 {
		return "no resource summaries"
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		codes := make([]string, len(value.Explanations))
		for i := range value.Explanations {
			codes[i] = value.Explanations[i].Code
		}
		parts = append(parts, value.Domain.Name+"["+strings.Join(codes, ",")+"]")
	}
	return strings.Join(parts, "; ")
}
