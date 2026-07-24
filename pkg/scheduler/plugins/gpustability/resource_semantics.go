package gpustability

import (
	"fmt"
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
)

type allocationMode string

const (
	allocationModeExclusive allocationMode = "exclusive"
	allocationModeDRALinked allocationMode = "dra-linked"
)

type inventorySelector struct {
	IDPrefix   string
	UUIDPrefix string
	NamePrefix string
	DRADriver  string
	DRAPool    string
	Canonical  string
}

func parseInventorySelector(raw string) (inventorySelector, error) {
	var selector inventorySelector
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return selector, nil
	}
	values := map[string]string{}
	for _, term := range strings.Split(raw, ",") {
		parts := strings.SplitN(strings.TrimSpace(term), "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return inventorySelector{}, fmt.Errorf("inventorySelector term %q must be key=value", term)
		}
		key := strings.ToLower(strings.TrimSpace(parts[0]))
		value := strings.TrimSpace(parts[1])
		if _, exists := values[key]; exists {
			return inventorySelector{}, fmt.Errorf("inventorySelector contains duplicate key %q", key)
		}
		switch key {
		case "id-prefix", "uuid-prefix", "name-prefix":
			if len(value) > 128 {
				return inventorySelector{}, fmt.Errorf("inventorySelector %s is longer than 128 characters", key)
			}
		case "dra-driver":
			if errs := utilvalidation.IsDNS1123Subdomain(value); len(errs) != 0 {
				return inventorySelector{}, fmt.Errorf("inventorySelector dra-driver %q is not a DNS subdomain", value)
			}
		case "dra-pool":
			if len(value) > 253 {
				return inventorySelector{}, fmt.Errorf("inventorySelector dra-pool is longer than 253 characters")
			}
		default:
			return inventorySelector{}, fmt.Errorf("inventorySelector key %q is unsupported; use id-prefix, uuid-prefix, name-prefix, dra-driver, or dra-pool", key)
		}
		values[key] = value
	}
	selector.IDPrefix = values["id-prefix"]
	selector.UUIDPrefix = values["uuid-prefix"]
	selector.NamePrefix = values["name-prefix"]
	selector.DRADriver = values["dra-driver"]
	selector.DRAPool = values["dra-pool"]
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+values[key])
	}
	selector.Canonical = strings.Join(parts, ",")
	return selector, nil
}

func parseAllocationMode(raw string) (allocationMode, error) {
	mode := allocationMode(strings.ToLower(strings.TrimSpace(raw)))
	if mode == "" {
		return allocationModeExclusive, nil
	}
	switch mode {
	case allocationModeExclusive, allocationModeDRALinked:
		return mode, nil
	default:
		return "", fmt.Errorf("allocationMode %q is invalid; use exclusive or dra-linked", raw)
	}
}

func mappingAcceptsProfile(mapping resourceMapping, profile string) bool {
	if len(mapping.Profiles) == 0 {
		return true
	}
	_, ok := mapping.Profiles[strings.ToLower(strings.TrimSpace(profile))]
	return ok
}

func devicesForMapping(devices []deviceMetrics, mapping resourceMapping) []deviceMetrics {
	result := make([]deviceMetrics, 0, len(devices))
	for _, device := range devices {
		if device.Class != mapping.Class || !mapping.Selector.matchesDevice(device) {
			continue
		}
		result = append(result, device)
	}
	return result
}

func (s inventorySelector) matchesDevice(device deviceMetrics) bool {
	if s.IDPrefix != "" && !hasFoldPrefix(device.ID, s.IDPrefix) {
		return false
	}
	if s.UUIDPrefix != "" && !hasFoldPrefix(device.UUID, s.UUIDPrefix) {
		return false
	}
	if s.NamePrefix != "" && !hasFoldPrefix(device.Name, s.NamePrefix) {
		return false
	}
	return true
}

func (s inventorySelector) matchesDRA(driver, pool string) bool {
	if s.DRADriver == "" || !strings.EqualFold(strings.TrimSpace(driver), s.DRADriver) {
		return false
	}
	return s.DRAPool == "" || strings.EqualFold(strings.TrimSpace(pool), s.DRAPool)
}

func resourceMappingsMayOverlap(left, right resourceMapping) bool {
	if left.Class != right.Class {
		return false
	}
	if profilesProvablyDisjoint(left.Profiles, right.Profiles) {
		return false
	}
	if prefixesProvablyDisjoint(left.Selector.IDPrefix, right.Selector.IDPrefix) ||
		prefixesProvablyDisjoint(left.Selector.UUIDPrefix, right.Selector.UUIDPrefix) ||
		prefixesProvablyDisjoint(left.Selector.NamePrefix, right.Selector.NamePrefix) {
		return false
	}
	return true
}

func profilesProvablyDisjoint(left, right map[string]struct{}) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	for name := range left {
		if _, ok := right[name]; ok {
			return false
		}
	}
	return true
}

func prefixesProvablyDisjoint(left, right string) bool {
	left = strings.ToLower(strings.TrimSpace(left))
	right = strings.ToLower(strings.TrimSpace(right))
	if left == "" || right == "" {
		return false
	}
	return !strings.HasPrefix(left, right) && !strings.HasPrefix(right, left)
}

func stableDeviceID(device deviceMetrics) string {
	if value := canonicalDeviceIdentity(device.UUID); value != "" {
		return value
	}
	return canonicalDeviceIdentity(device.ID)
}

func canonicalDeviceIdentity(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func hasFoldPrefix(value, prefix string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(value)), strings.ToLower(strings.TrimSpace(prefix)))
}

func sortedResourceNames(resources map[v1.ResourceName]int64) []v1.ResourceName {
	result := make([]v1.ResourceName, 0, len(resources))
	for name := range resources {
		result = append(result, name)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func allocationSetsDisjoint(left, right []physicalAllocationIdentity) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(left))
	for _, identity := range left {
		seen[identity.key()] = struct{}{}
	}
	for _, identity := range right {
		if _, ok := seen[identity.key()]; ok {
			return false
		}
	}
	return true
}

func (p *Plugin) evaluateRequestedResources(requirement *workloadRequirement, metrics nodeMetrics, resources nodeResourceContext, node *v1.Node) nodeEvaluation {
	evaluation := nodeEvaluation{Compatible: true, DynamicEligible: true, ReasonCode: "accepted"}
	names := sortedResourceNames(requirement.Resources)

	for _, name := range names {
		mapping := p.config.ResourceMappings[name]
		if !mappingAcceptsProfile(mapping, metrics.Profile) {
			return nodeEvaluation{Compatible: false, DynamicEligible: false, ReasonCode: "profile_incompatible", Reason: fmt.Sprintf("exporter profile %q is not compatible with resource %q", metrics.Profile, name)}
		}
		if issue := requirement.AllocationIssues[name]; issue != "" {
			markDynamicFailure(&evaluation, "allocation_identity_invalid", issue)
		}
	}

	for i, leftName := range names {
		left := p.config.ResourceMappings[leftName]
		for _, rightName := range names[i+1:] {
			right := p.config.ResourceMappings[rightName]
			if !resourceMappingsMayOverlap(left, right) {
				continue
			}
			leftAllocated := requirement.PhysicalAllocations[leftName]
			rightAllocated := requirement.PhysicalAllocations[rightName]
			leftDemand := requirement.Resources[leftName]
			rightDemand := requirement.Resources[rightName]
			linkedAndDistinct := int64(len(leftAllocated)) == leftDemand && int64(len(rightAllocated)) == rightDemand && allocationSetsDisjoint(leftAllocated, rightAllocated)
			if !linkedAndDistinct {
				markDynamicFailure(&evaluation, "inventory_overlap_unproven", fmt.Sprintf("resource inventories %q and %q may overlap; distinct physical allocation identities are required before both demands can be enforced", leftName, rightName))
			}
		}
	}

	seenEvaluationDevices := map[string]struct{}{}
	for _, resourceName := range names {
		demand := requirement.Resources[resourceName]
		mapping := p.config.ResourceMappings[resourceName]
		partitionDevices := devicesForMapping(metrics.Devices, mapping)
		linked := requirement.PhysicalAllocations[resourceName]
		usingLinked := int64(len(linked)) == demand && demand > 0
		if usingLinked {
			selected, reasonCode, reason := selectAllocatedDevices(partitionDevices, linked, node)
			if reasonCode != "" {
				markDynamicFailure(&evaluation, reasonCode, fmt.Sprintf("resource %q: %s", resourceName, reason))
				partitionDevices = selected
			} else {
				partitionDevices = selected
			}
		} else {
			allocatable, known := resources.Allocatable[resourceName]
			observed := int64(len(partitionDevices))
			if !known || allocatable <= 0 || observed != allocatable {
				markDynamicFailure(&evaluation, "allocation_identity_unlinked", fmt.Sprintf("strict telemetry coverage for %q inventory is %d device(s), Kubernetes allocatable is %d; physical allocation identity is not linked", resourceName, observed, allocatable))
			}
		}

		if int64(len(partitionDevices)) < demand {
			evaluation.Compatible = false
			evaluation.DynamicEligible = false
			evaluation.ReasonCode = "wrong_or_insufficient_inventory"
			evaluation.Reason = fmt.Sprintf("exporter inventory for %q contains %d matching device(s), workload requires %d", resourceName, len(partitionDevices), demand)
			return evaluation
		}

		eligible := make([]deviceMetrics, 0, len(partitionDevices))
		ineligibleReasons := map[string]int{}
		for _, device := range partitionDevices {
			if reasonCode := device.ineligibilityReason(requirement.MaxTemperatureC, requirement.MinFreeMemoryMiB, p.config.MinSMClockMHz, p.config.MinMemClockMHz); reasonCode != "" {
				ineligibleReasons[reasonCode]++
				continue
			}
			eligible = append(eligible, device)
		}
		if int64(len(eligible)) < demand || len(eligible) != len(partitionDevices) {
			reasonCodes := make([]string, 0, len(ineligibleReasons))
			for reasonCode := range ineligibleReasons {
				reasonCodes = append(reasonCodes, reasonCode)
			}
			sort.Strings(reasonCodes)
			primary := "dynamic_threshold"
			if len(reasonCodes) > 0 {
				primary = reasonCodes[0]
			}
			markDynamicFailure(&evaluation, primary, fmt.Sprintf("only %d of %d device(s) in %q inventory satisfy telemetry thresholds (%s)", len(eligible), len(partitionDevices), resourceName, formatReasonCounts(reasonCodes, ineligibleReasons)))
		}
		for _, device := range eligible {
			identity := stableDeviceID(device)
			if identity == "" {
				identity = strings.ToLower(string(device.Class) + "|" + device.ID + "|" + device.Name)
			}
			if _, exists := seenEvaluationDevices[identity]; exists {
				continue
			}
			seenEvaluationDevices[identity] = struct{}{}
			evaluation.Devices = append(evaluation.Devices, device)
		}
	}
	return evaluation
}

func selectAllocatedDevices(candidates []deviceMetrics, allocated []physicalAllocationIdentity, node *v1.Node) ([]deviceMetrics, string, string) {
	byID := make(map[string]deviceMetrics, len(candidates))
	for _, device := range candidates {
		identity := stableDeviceID(device)
		if identity == "" {
			continue
		}
		if _, duplicate := byID[identity]; duplicate {
			return nil, "allocation_device_duplicate", fmt.Sprintf("exporter stable device ID %q is duplicated", identity)
		}
		byID[identity] = device
	}
	selected := make([]deviceMetrics, 0, len(allocated))
	seen := map[string]struct{}{}
	for _, identity := range allocated {
		key := canonicalDeviceIdentity(identity.Device)
		if _, duplicate := seen[key]; duplicate {
			return selected, "allocation_device_duplicate", fmt.Sprintf("allocated device ID %q is duplicated", identity.Device)
		}
		seen[key] = struct{}{}
		if !identity.availableOn(node) {
			return selected, "allocation_device_node_mismatch", fmt.Sprintf("allocated device %q is not available on node %q", identity.Device, nodeName(node))
		}
		device, ok := byID[key]
		if !ok {
			return selected, "allocation_device_missing", fmt.Sprintf("allocated DRA device ID %q does not match an exporter stable device ID", identity.Device)
		}
		selected = append(selected, device)
	}
	return selected, "", ""
}

func markDynamicFailure(evaluation *nodeEvaluation, code, reason string) {
	evaluation.DynamicEligible = false
	if evaluation.ReasonCode == "accepted" || evaluation.ReasonCode == "" {
		evaluation.ReasonCode = code
		evaluation.Reason = reason
	}
}

func nodeName(node *v1.Node) string {
	if node == nil {
		return ""
	}
	return node.Name
}
