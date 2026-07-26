package planner

import (
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

type capacityAllocation struct {
	Requirement spacev1.CapabilityRequirement
	Bucket      spacev1.DeviceCapacity
	Quantity    int64
}

type capabilitySelection struct {
	selectedCapabilities []spacev1.CapabilityRequirement
	selectedSetName      string
	allocations          []capacityAllocation
}

type capacityBucket struct {
	value spacev1.DeviceCapacity
	key   string
}

type allocationEdgeRef struct {
	requirement int
	bucket      int
	node        int
	edge        int
	capacity    int64
}

type flowEdge struct {
	to  int
	rev int
	cap int64
}

type flowGraph [][]flowEdge

func selectCapabilitySet(mission spacev1.SpaceMissionSpec, summary spacev1.SpaceDomainResourceSummarySpec) (capabilitySelection, string, error) {
	required, err := normalizeCapabilityRequirements(mission.RequiredCapabilities)
	if err != nil {
		return capabilitySelection{}, "", err
	}
	if len(mission.AlternativeCapabilities) == 0 {
		allocations, reason, err := allocateCapabilities(required, summary)
		return capabilitySelection{selectedCapabilities: required, allocations: allocations}, reason, err
	}

	sets := append([]spacev1.CapabilitySet(nil), mission.AlternativeCapabilities...)
	sort.SliceStable(sets, func(i, j int) bool {
		if sets[i].Name != sets[j].Name {
			return sets[i].Name < sets[j].Name
		}
		return capabilitySetKey(sets[i]) < capabilitySetKey(sets[j])
	})
	reasons := make([]string, 0, len(sets))
	for _, set := range sets {
		combined := append(copyCapabilities(required), set.AllOf...)
		combined, err = normalizeCapabilityRequirements(combined)
		if err != nil {
			return capabilitySelection{}, "", err
		}
		allocations, reason, err := allocateCapabilities(combined, summary)
		if err != nil {
			return capabilitySelection{}, "", err
		}
		if reason == "" {
			return capabilitySelection{selectedCapabilities: combined, selectedSetName: set.Name, allocations: allocations}, "", nil
		}
		reasons = append(reasons, fmt.Sprintf("%s: %s", set.Name, reason))
	}
	return capabilitySelection{}, strings.Join(reasons, "; "), nil
}

func copyCapabilities(in []spacev1.CapabilityRequirement) []spacev1.CapabilityRequirement {
	out := make([]spacev1.CapabilityRequirement, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].Precision = append([]string(nil), in[i].Precision...)
		if in[i].Software != nil {
			out[i].Software = make(map[string]string, len(in[i].Software))
			for key, value := range in[i].Software {
				out[i].Software[key] = value
			}
		}
	}
	return out
}

func normalizeCapabilityRequirements(in []spacev1.CapabilityRequirement) ([]spacev1.CapabilityRequirement, error) {
	aggregated := map[string]spacev1.CapabilityRequirement{}
	for _, raw := range in {
		requirement := raw
		requirement.Class = strings.TrimSpace(requirement.Class)
		requirement.Architecture = strings.TrimSpace(requirement.Architecture)
		requirement.Model = strings.TrimSpace(requirement.Model)
		requirement.Precision = sortedUniqueStrings(requirement.Precision)
		requirement.Software = copyStringMapSorted(raw.Software)
		key := capabilityRequirementKey(requirement)
		if existing, ok := aggregated[key]; ok {
			quantity, err := checkedAddInt64(existing.Quantity, requirement.Quantity)
			if err != nil {
				return nil, fmt.Errorf("aggregate capability %s: %w", capabilityConstraintString(requirement), err)
			}
			existing.Quantity = quantity
			aggregated[key] = existing
		} else {
			aggregated[key] = requirement
		}
	}
	keys := make([]string, 0, len(aggregated))
	for key := range aggregated {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]spacev1.CapabilityRequirement, 0, len(keys))
	for _, key := range keys {
		out = append(out, aggregated[key])
	}
	return out, nil
}

func capabilitySetKey(set spacev1.CapabilitySet) string {
	values, err := normalizeCapabilityRequirements(set.AllOf)
	if err != nil {
		return set.Name
	}
	keys := make([]string, len(values))
	for i := range values {
		keys[i] = capabilityRequirementKey(values[i]) + "=" + strconv.FormatInt(values[i].Quantity, 10)
	}
	return set.Name + "\x00" + strings.Join(keys, "\x1e")
}

func capabilityRequirementKey(value spacev1.CapabilityRequirement) string {
	software := make([]string, 0, len(value.Software))
	for key, version := range value.Software {
		software = append(software, key+"="+version)
	}
	sort.Strings(software)
	return strings.Join([]string{
		value.Class,
		value.Architecture,
		value.Model,
		strings.Join(sortedUniqueStrings(value.Precision), "\x1f"),
		strings.Join(software, "\x1f"),
	}, "\x00")
}

func capabilityConstraintString(value spacev1.CapabilityRequirement) string {
	parts := []string{"class=" + value.Class}
	if value.Architecture != "" {
		parts = append(parts, "arch="+value.Architecture)
	}
	if value.Model != "" {
		parts = append(parts, "model="+value.Model)
	}
	if len(value.Precision) > 0 {
		parts = append(parts, "precision="+strings.Join(sortedUniqueStrings(value.Precision), "+"))
	}
	keys := make([]string, 0, len(value.Software))
	for key := range value.Software {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts = append(parts, "software="+key+"="+value.Software[key])
	}
	return strings.Join(parts, ",")
}

func allocateCapabilities(requirements []spacev1.CapabilityRequirement, summary spacev1.SpaceDomainResourceSummarySpec) ([]capacityAllocation, string, error) {
	if len(requirements) == 0 {
		return nil, "", nil
	}
	buckets, err := normalizeCapacityBuckets(summary.Devices)
	if err != nil {
		return nil, "", err
	}

	totalDemand := int64(0)
	for _, requirement := range requirements {
		totalDemand, err = checkedAddInt64(totalDemand, requirement.Quantity)
		if err != nil {
			return nil, "", fmt.Errorf("total capability demand: %w", err)
		}
	}

	source := 0
	requirementBase := 1
	bucketBase := requirementBase + len(requirements)
	sink := bucketBase + len(buckets)
	graph := make(flowGraph, sink+1)
	for i, requirement := range requirements {
		addFlowEdge(graph, source, requirementBase+i, requirement.Quantity)
	}
	for i, bucket := range buckets {
		addFlowEdge(graph, bucketBase+i, sink, bucket.value.Count)
	}

	refs := []allocationEdgeRef{}
	for ri, requirement := range requirements {
		for bi, bucket := range buckets {
			if !capacityBucketMatches(bucket.value, requirement, summary.Software) {
				continue
			}
			node := requirementBase + ri
			edgeIndex := len(graph[node])
			addFlowEdge(graph, node, bucketBase+bi, requirement.Quantity)
			refs = append(refs, allocationEdgeRef{requirement: ri, bucket: bi, node: node, edge: edgeIndex, capacity: requirement.Quantity})
		}
	}

	flow, err := maxFlow(graph, source, sink)
	if err != nil {
		return nil, "", err
	}
	allocatedByRequirement := make([]int64, len(requirements))
	allocations := make([]capacityAllocation, 0, len(refs))
	for _, ref := range refs {
		remaining := graph[ref.node][ref.edge].cap
		used := ref.capacity - remaining
		if used <= 0 {
			continue
		}
		allocatedByRequirement[ref.requirement], err = checkedAddInt64(allocatedByRequirement[ref.requirement], used)
		if err != nil {
			return nil, "", err
		}
		allocations = append(allocations, capacityAllocation{Requirement: requirements[ref.requirement], Bucket: buckets[ref.bucket].value, Quantity: used})
	}
	if flow != totalDemand {
		for index, requirement := range requirements {
			if allocatedByRequirement[index] < requirement.Quantity {
				return nil, fmt.Sprintf("%s has %d allocatable, requires %d", capabilityConstraintString(requirement), allocatedByRequirement[index], requirement.Quantity), nil
			}
		}
		return nil, fmt.Sprintf("allocated %d of %d requested capacity units", flow, totalDemand), nil
	}
	sort.SliceStable(allocations, func(i, j int) bool {
		left, right := capabilityRequirementKey(allocations[i].Requirement), capabilityRequirementKey(allocations[j].Requirement)
		if left != right {
			return left < right
		}
		return capacityBucketKey(allocations[i].Bucket) < capacityBucketKey(allocations[j].Bucket)
	})
	return allocations, "", nil
}

func normalizeCapacityBuckets(values []spacev1.DeviceCapacity) ([]capacityBucket, error) {
	buckets := make([]capacityBucket, len(values))
	for i, raw := range values {
		value := raw
		value.Class = strings.TrimSpace(value.Class)
		value.Architectures = sortedUniqueStrings(value.Architectures)
		value.Models = sortedUniqueStrings(value.Models)
		value.Precision = sortedUniqueStrings(value.Precision)
		buckets[i] = capacityBucket{value: value, key: capacityBucketKey(value)}
	}
	sort.SliceStable(buckets, func(i, j int) bool { return buckets[i].key < buckets[j].key })
	return buckets, nil
}

func capacityBucketKey(value spacev1.DeviceCapacity) string {
	return strings.Join([]string{
		value.Class,
		strings.Join(sortedUniqueStrings(value.Architectures), "\x1f"),
		strings.Join(sortedUniqueStrings(value.Models), "\x1f"),
		strings.Join(sortedUniqueStrings(value.Precision), "\x1f"),
		strconv.FormatInt(value.ComputeMilli, 10),
		strconv.FormatInt(int64(value.FragmentationMilli), 10),
		strconv.FormatInt(value.Count, 10),
	}, "\x00")
}

func capacityBucketMatches(capacity spacev1.DeviceCapacity, requirement spacev1.CapabilityRequirement, software map[string]string) bool {
	if capacity.Class != requirement.Class || !optionalContains(capacity.Architectures, requirement.Architecture) || !optionalContains(capacity.Models, requirement.Model) || !containsAll(capacity.Precision, requirement.Precision) {
		return false
	}
	return softwareMismatch(requirement.Software, software) == ""
}

func selectionExplanations(selection capabilitySelection) []spacev1.ConstraintExplanation {
	setName := selection.selectedSetName
	if setName == "" {
		setName = "required-only"
	}
	result := []spacev1.ConstraintExplanation{
		accept("capabilities_satisfied", "device and software capabilities", "compatible", "compatible"),
		accept("capability_set_selected", "selected capability set", setName, "deterministic feasible set"),
	}
	byRequirement := map[string][]capacityAllocation{}
	for _, allocation := range selection.allocations {
		key := capabilityRequirementKey(allocation.Requirement)
		byRequirement[key] = append(byRequirement[key], allocation)
	}
	for _, requirement := range selection.selectedCapabilities {
		key := capabilityRequirementKey(requirement)
		parts := make([]string, 0, len(byRequirement[key]))
		for _, allocation := range byRequirement[key] {
			parts = append(parts, fmt.Sprintf("%dx[%s]", allocation.Quantity, compactCapacityBucket(allocation.Bucket)))
		}
		sort.Strings(parts)
		result = append(result, spacev1.ConstraintExplanation{
			Code:       "capability_allocation",
			Constraint: capabilityConstraintString(requirement),
			Observed:   strings.Join(parts, ";"),
			Required:   fmt.Sprintf("quantity=%d", requirement.Quantity),
			Message:    "capacity allocated without reusing a device bucket",
		})
	}
	return result
}

func compactCapacityBucket(value spacev1.DeviceCapacity) string {
	parts := []string{"class=" + value.Class}
	if len(value.Architectures) > 0 {
		parts = append(parts, "arch="+strings.Join(sortedUniqueStrings(value.Architectures), "+"))
	}
	if len(value.Models) > 0 {
		parts = append(parts, "model="+strings.Join(sortedUniqueStrings(value.Models), "+"))
	}
	parts = append(parts, fmt.Sprintf("computeMilli=%d", value.ComputeMilli))
	return strings.Join(parts, ",")
}

func predictedComputeSeconds(mission spacev1.SpaceMissionSpec, allocations []capacityAllocation) (int64, error) {
	if len(allocations) == 0 {
		return mission.MaximumDurationSeconds, nil
	}
	totalComputeMilli := int64(0)
	totalUnits := int64(0)
	for _, allocation := range allocations {
		if allocation.Quantity <= 0 || allocation.Bucket.ComputeMilli <= 0 {
			return mission.MaximumDurationSeconds, nil
		}
		contribution, err := checkedMulInt64(allocation.Quantity, allocation.Bucket.ComputeMilli)
		if err != nil {
			return 0, fmt.Errorf("selected compute capacity: %w", err)
		}
		totalComputeMilli, err = checkedAddInt64(totalComputeMilli, contribution)
		if err != nil {
			return 0, fmt.Errorf("selected compute capacity: %w", err)
		}
		totalUnits, err = checkedAddInt64(totalUnits, allocation.Quantity)
		if err != nil {
			return 0, fmt.Errorf("selected compute units: %w", err)
		}
	}
	if totalUnits < 1 || totalComputeMilli < 1 {
		return mission.MaximumDurationSeconds, nil
	}
	effectiveComputeMilli, err := checkedDivInt64(totalComputeMilli, totalUnits)
	if err != nil {
		return 0, fmt.Errorf("selected compute average: %w", err)
	}
	if effectiveComputeMilli < 1 {
		return mission.MaximumDurationSeconds, nil
	}
	scaled, err := checkedMulInt64(mission.MaximumDurationSeconds, 1000)
	if err != nil {
		return 0, fmt.Errorf("scale maximum duration: %w", err)
	}
	seconds, err := checkedCeilDiv(scaled, effectiveComputeMilli)
	if err != nil {
		return 0, fmt.Errorf("compute duration: %w", err)
	}
	if seconds < mission.ExpectedDurationSeconds {
		seconds = mission.ExpectedDurationSeconds
	}
	if seconds > mission.MaximumDurationSeconds {
		seconds = mission.MaximumDurationSeconds
	}
	return seconds, nil
}

func validatePlannerTopology(summaries []*spacev1.SpaceDomainResourceSummary, links []*spacev1.SpaceLinkSnapshot) error {
	total, err := checkedAddInt64(int64(len(summaries)), int64(len(links)))
	if err != nil {
		return fmt.Errorf("planner topology size: %w", err)
	}
	if total > spacev1.MaxPlannerTopologyEntries {
		return fmt.Errorf("planner topology contains %d objects; maximum is %d", total, spacev1.MaxPlannerTopologyEntries)
	}
	return nil
}

func directedDomainKey(source, destination spacev1.DomainReference) string {
	return fullDomainKey(source) + "->" + fullDomainKey(destination)
}

func fullDomainKey(value spacev1.DomainReference) string {
	return value.ClusterID + "/" + value.Name + "/" + string(value.OrbitClass)
}

func dataLocationKey(value spacev1.DataLocation) string {
	return fullDomainKey(value.Domain) + "\x00" + value.URI
}

func sortedDataLocations(values []spacev1.DataLocation) []spacev1.DataLocation {
	out := append([]spacev1.DataLocation(nil), values...)
	sort.SliceStable(out, func(i, j int) bool { return dataLocationKey(out[i]) < dataLocationKey(out[j]) })
	return out
}

func locationMatchesDomain(values []spacev1.DataLocation, domain spacev1.DomainReference) bool {
	for _, value := range values {
		if value.Domain == domain {
			return true
		}
	}
	return false
}

func formatDataLocations(values []spacev1.DataLocation) string {
	values = sortedDataLocations(values)
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = fullDomainKey(value.Domain)
		if value.URI != "" {
			parts[i] += "@" + value.URI
		}
	}
	return strings.Join(parts, ",")
}

func sortedUniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := append([]string(nil), values...)
	sort.Strings(out)
	unique := out[:0]
	for _, value := range out {
		if len(unique) == 0 || unique[len(unique)-1] != value {
			unique = append(unique, value)
		}
	}
	return unique
}

func copyStringMapSorted(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func addFlowEdge(graph flowGraph, from, to int, capacity int64) {
	forward := flowEdge{to: to, rev: len(graph[to]), cap: capacity}
	reverse := flowEdge{to: from, rev: len(graph[from]), cap: 0}
	graph[from] = append(graph[from], forward)
	graph[to] = append(graph[to], reverse)
}

func maxFlow(graph flowGraph, source, sink int) (int64, error) {
	flow := int64(0)
	level := make([]int, len(graph))
	for {
		for i := range level {
			level[i] = -1
		}
		queue := []int{source}
		level[source] = 0
		for len(queue) > 0 {
			node := queue[0]
			queue = queue[1:]
			for _, edge := range graph[node] {
				if edge.cap > 0 && level[edge.to] < 0 {
					level[edge.to] = level[node] + 1
					queue = append(queue, edge.to)
				}
			}
		}
		if level[sink] < 0 {
			return flow, nil
		}
		next := make([]int, len(graph))
		for {
			pushed, err := sendFlow(graph, level, next, source, sink, math.MaxInt64)
			if err != nil {
				return 0, err
			}
			if pushed == 0 {
				break
			}
			flow, err = checkedAddInt64(flow, pushed)
			if err != nil {
				return 0, fmt.Errorf("max-flow total: %w", err)
			}
		}
	}
}

func sendFlow(graph flowGraph, level, next []int, node, sink int, limit int64) (int64, error) {
	if node == sink {
		return limit, nil
	}
	for next[node] < len(graph[node]) {
		edgeIndex := next[node]
		edge := &graph[node][edgeIndex]
		if edge.cap > 0 && level[edge.to] == level[node]+1 {
			pushed, err := sendFlow(graph, level, next, edge.to, sink, minInt64(limit, edge.cap))
			if err != nil {
				return 0, err
			}
			if pushed > 0 {
				edge.cap -= pushed
				reverse := &graph[edge.to][edge.rev]
				reverse.cap, err = checkedAddInt64(reverse.cap, pushed)
				if err != nil {
					return 0, err
				}
				return pushed, nil
			}
		}
		next[node]++
	}
	return 0, nil
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func checkedAddInt64(a, b int64) (int64, error) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, fmt.Errorf("integer addition overflow")
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, fmt.Errorf("integer addition underflow")
	}
	return a + b, nil
}

func checkedMulInt64(a, b int64) (int64, error) {
	if a == 0 || b == 0 {
		return 0, nil
	}
	if a == -1 && b == math.MinInt64 || b == -1 && a == math.MinInt64 {
		return 0, fmt.Errorf("integer multiplication overflow")
	}
	result := a * b
	if result/b != a {
		return 0, fmt.Errorf("integer multiplication overflow")
	}
	return result, nil
}

func checkedDivInt64(value, divisor int64) (int64, error) {
	if divisor == 0 {
		return 0, fmt.Errorf("integer division by zero")
	}
	if value == math.MinInt64 && divisor == -1 {
		return 0, fmt.Errorf("integer division overflow")
	}
	return value / divisor, nil
}

func checkedRatioMilli(numerator, denominator int64) (int64, error) {
	if numerator <= 0 {
		return 0, nil
	}
	if denominator <= 0 {
		return 0, fmt.Errorf("ratio denominator must be positive")
	}
	if numerator >= denominator {
		return 1000, nil
	}
	scaled, err := checkedMulInt64(numerator, 1000)
	if err == nil {
		return checkedDivInt64(scaled, denominator)
	}
	// The ratio itself is bounded by 1000; big.Int is used only after the
	// checked fixed-width multiplication proves the intermediate would overflow.
	value := new(big.Int).Mul(big.NewInt(numerator), big.NewInt(1000))
	value.Quo(value, big.NewInt(denominator))
	if !value.IsInt64() {
		return 0, fmt.Errorf("scaled ratio exceeds int64")
	}
	return value.Int64(), nil
}

func checkedCeilDiv(value, divisor int64) (int64, error) {
	if value < 0 {
		return 0, fmt.Errorf("ceil division value cannot be negative")
	}
	if divisor <= 0 {
		return 0, fmt.Errorf("ceil division divisor must be positive")
	}
	quotient := value / divisor
	if value%divisor == 0 {
		return quotient, nil
	}
	return checkedAddInt64(quotient, 1)
}

func checkedSecondsDuration(seconds int64) (time.Duration, error) {
	if seconds < 0 {
		return 0, fmt.Errorf("duration seconds cannot be negative")
	}
	value, err := checkedMulInt64(seconds, int64(time.Second))
	if err != nil {
		return 0, fmt.Errorf("duration conversion: %w", err)
	}
	return time.Duration(value), nil
}

func checkedTimeAdd(value time.Time, delta time.Duration) (time.Time, error) {
	result := value.Add(delta)
	if delta > 0 && result.Before(value) || delta < 0 && result.After(value) {
		return time.Time{}, fmt.Errorf("timestamp addition overflow")
	}
	if year := result.Year(); year < 1 || year > 9999 {
		return time.Time{}, fmt.Errorf("timestamp addition is outside RFC3339 range")
	}
	return result, nil
}

func normalizedMissionSpecForDigest(spec spacev1.SpaceMissionSpec) (spacev1.SpaceMissionSpec, error) {
	out := spec
	var err error
	out.RequiredCapabilities, err = normalizeCapabilityRequirements(spec.RequiredCapabilities)
	if err != nil {
		return spacev1.SpaceMissionSpec{}, err
	}
	out.AlternativeCapabilities = append([]spacev1.CapabilitySet(nil), spec.AlternativeCapabilities...)
	for i := range out.AlternativeCapabilities {
		out.AlternativeCapabilities[i].AllOf, err = normalizeCapabilityRequirements(out.AlternativeCapabilities[i].AllOf)
		if err != nil {
			return spacev1.SpaceMissionSpec{}, err
		}
	}
	sort.SliceStable(out.AlternativeCapabilities, func(i, j int) bool {
		return capabilitySetKey(out.AlternativeCapabilities[i]) < capabilitySetKey(out.AlternativeCapabilities[j])
	})
	out.Inputs = append([]spacev1.DataObject(nil), spec.Inputs...)
	for i := range out.Inputs {
		out.Inputs[i].Locations = sortedDataLocations(out.Inputs[i].Locations)
	}
	out.ResultDestinations = sortedDataLocations(spec.ResultDestinations)
	return out, nil
}

func normalizedResourceSummarySpecForDigest(spec spacev1.SpaceDomainResourceSummarySpec) spacev1.SpaceDomainResourceSummarySpec {
	out := spec
	out.Devices = append([]spacev1.DeviceCapacity(nil), spec.Devices...)
	for i := range out.Devices {
		out.Devices[i].Architectures = sortedUniqueStrings(out.Devices[i].Architectures)
		out.Devices[i].Models = sortedUniqueStrings(out.Devices[i].Models)
		out.Devices[i].Precision = sortedUniqueStrings(out.Devices[i].Precision)
	}
	sort.SliceStable(out.Devices, func(i, j int) bool {
		return capacityBucketKey(out.Devices[i]) < capacityBucketKey(out.Devices[j])
	})
	out.DataLocations = sortedUniqueStrings(spec.DataLocations)
	return out
}

func normalizedLinkSpecForDigest(spec spacev1.SpaceLinkSnapshotSpec) spacev1.SpaceLinkSnapshotSpec {
	out := spec
	out.Windows = append([]spacev1.ContactWindow(nil), spec.Windows...)
	sort.SliceStable(out.Windows, func(i, j int) bool {
		if out.Windows[i].Start.Equal(&out.Windows[j].Start) {
			return out.Windows[i].ID < out.Windows[j].ID
		}
		return out.Windows[i].Start.Before(&out.Windows[j].Start)
	})
	return out
}
