package gpustability

import (
	"fmt"
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	resourcev1beta1listers "k8s.io/client-go/listers/resource/v1beta1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type allocationIdentitySource interface {
	GetResourceClaim(namespace, name string) (*resourceapi.ResourceClaim, error)
	ListResourceSlices() ([]*resourceapi.ResourceSlice, error)
}

type informerAllocationIdentitySource struct {
	claims resourcev1beta1listers.ResourceClaimLister
	slices resourcev1beta1listers.ResourceSliceLister
}

func (s *informerAllocationIdentitySource) GetResourceClaim(namespace, name string) (*resourceapi.ResourceClaim, error) {
	return s.claims.ResourceClaims(namespace).Get(name)
}

func (s *informerAllocationIdentitySource) ListResourceSlices() ([]*resourceapi.ResourceSlice, error) {
	return s.slices.List(labels.Everything())
}

func newAllocationIdentitySource(handle framework.Handle, cfg Config) allocationIdentitySource {
	if handle == nil || !configUsesDRALinkage(cfg) {
		return nil
	}
	resourceInformers := handle.SharedInformerFactory().Resource().V1beta1()
	claims := resourceInformers.ResourceClaims()
	slices := resourceInformers.ResourceSlices()
	// Request these informers during plugin construction. The scheduler owns and
	// starts the shared factory; scheduler callbacks only consume its local cache.
	claims.Informer()
	slices.Informer()
	return &informerAllocationIdentitySource{claims: claims.Lister(), slices: slices.Lister()}
}

func configUsesDRALinkage(cfg Config) bool {
	for _, mapping := range cfg.ResourceMappings {
		if mapping.AllocationMode == allocationModeDRALinked {
			return true
		}
	}
	return false
}

type physicalAllocationIdentity struct {
	ClaimName         string
	Request           string
	Driver            string
	Pool              string
	Device            string
	NodeName          string
	AllNodes          bool
	NodeSelector      *v1.NodeSelector
	ClaimNodeSelector *v1.NodeSelector
}

func (identity physicalAllocationIdentity) key() string {
	return strings.ToLower(strings.TrimSpace(identity.Driver)) + "/" + strings.ToLower(strings.TrimSpace(identity.Pool)) + "/" + canonicalDeviceIdentity(identity.Device)
}

func (identity physicalAllocationIdentity) clone() physicalAllocationIdentity {
	out := identity
	if identity.NodeSelector != nil {
		out.NodeSelector = identity.NodeSelector.DeepCopy()
	}
	if identity.ClaimNodeSelector != nil {
		out.ClaimNodeSelector = identity.ClaimNodeSelector.DeepCopy()
	}
	return out
}

func (identity physicalAllocationIdentity) availableOn(node *v1.Node) bool {
	if node == nil {
		return false
	}
	if identity.ClaimNodeSelector != nil {
		selector, err := nodeaffinity.NewNodeSelector(identity.ClaimNodeSelector)
		if err != nil || !selector.Match(node) {
			return false
		}
	}
	if identity.AllNodes {
		return true
	}
	if identity.NodeName != "" {
		return identity.NodeName == node.Name
	}
	if identity.NodeSelector != nil {
		selector, err := nodeaffinity.NewNodeSelector(identity.NodeSelector)
		return err == nil && selector.Match(node)
	}
	return false
}

func clonePhysicalAllocations(in map[v1.ResourceName][]physicalAllocationIdentity) map[v1.ResourceName][]physicalAllocationIdentity {
	if in == nil {
		return nil
	}
	out := make(map[v1.ResourceName][]physicalAllocationIdentity, len(in))
	for name, identities := range in {
		copyIdentities := make([]physicalAllocationIdentity, len(identities))
		for i := range identities {
			copyIdentities[i] = identities[i].clone()
		}
		out[name] = copyIdentities
	}
	return out
}

func cloneAllocationIssues(in map[v1.ResourceName]string) map[v1.ResourceName]string {
	if in == nil {
		return nil
	}
	out := make(map[v1.ResourceName]string, len(in))
	for name, issue := range in {
		out[name] = issue
	}
	return out
}

func (p *Plugin) resolvePhysicalAllocations(pod *v1.Pod, requirement *workloadRequirement) {
	if p == nil || p.allocationSource == nil || pod == nil || requirement == nil || len(requirement.Resources) == 0 || len(pod.Spec.ResourceClaims) == 0 {
		return
	}
	needsDRA := false
	for name := range requirement.Resources {
		mapping := p.config.ResourceMappings[name]
		if mapping.AllocationMode == allocationModeDRALinked && mapping.Selector.DRADriver != "" {
			needsDRA = true
			break
		}
	}
	if !needsDRA {
		return
	}

	slices, err := p.allocationSource.ListResourceSlices()
	if err != nil {
		return // local cache unavailable: retain conservative unlinked semantics
	}
	type resolvedResult struct {
		identity physicalAllocationIdentity
		issue    string
	}
	var results []resolvedResult
	seenTuples := map[string]struct{}{}
	for i := range pod.Spec.ResourceClaims {
		podClaim := &pod.Spec.ResourceClaims[i]
		claimName, mustCheckOwner, err := resourceclaim.Name(pod, podClaim)
		if err != nil || claimName == nil {
			continue
		}
		claim, err := p.allocationSource.GetResourceClaim(pod.Namespace, *claimName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			continue
		}
		if claim.DeletionTimestamp != nil || (mustCheckOwner && resourceclaim.IsForPod(pod, claim) != nil) || claim.Status.Allocation == nil {
			continue
		}
		for _, allocated := range claim.Status.Allocation.Devices.Results {
			identity, issue := resolveAllocatedDeviceIdentity(claim.Name, allocated, claim.Status.Allocation.NodeSelector, slices)
			key := identity.key()
			if key != "//" {
				if _, duplicate := seenTuples[key]; duplicate {
					issue = fmt.Sprintf("allocated physical device identity %q is duplicated across ResourceClaim allocation results", key)
				}
				seenTuples[key] = struct{}{}
			}
			results = append(results, resolvedResult{identity: identity, issue: issue})
		}
	}

	if requirement.PhysicalAllocations == nil {
		requirement.PhysicalAllocations = map[v1.ResourceName][]physicalAllocationIdentity{}
	}
	if requirement.AllocationIssues == nil {
		requirement.AllocationIssues = map[v1.ResourceName]string{}
	}
	claimedBy := map[string]v1.ResourceName{}
	for _, resourceName := range sortedResourceNames(requirement.Resources) {
		mapping := p.config.ResourceMappings[resourceName]
		if mapping.AllocationMode != allocationModeDRALinked || mapping.Selector.DRADriver == "" {
			continue
		}
		var matching []physicalAllocationIdentity
		var issues []string
		for _, result := range results {
			if !mapping.Selector.matchesDRA(result.identity.Driver, result.identity.Pool) {
				continue
			}
			if result.issue != "" {
				issues = append(issues, result.issue)
				continue
			}
			matching = append(matching, result.identity)
		}
		if len(issues) > 0 {
			sort.Strings(issues)
			requirement.AllocationIssues[resourceName] = strings.Join(issues, "; ")
			continue
		}
		if int64(len(matching)) != requirement.Resources[resourceName] {
			// The claim may contain unrelated devices. Without an exact one-to-one
			// correspondence, the plugin cannot truthfully bind this resource demand
			// to those physical IDs and therefore retains the node-wide rule.
			continue
		}
		ambiguous := false
		for _, identity := range matching {
			if owner, exists := claimedBy[identity.key()]; exists && owner != resourceName {
				requirement.AllocationIssues[resourceName] = fmt.Sprintf("physical device %q is already linked to resource %q in this scheduling cycle", identity.key(), owner)
				requirement.AllocationIssues[owner] = fmt.Sprintf("physical device %q is also linked to resource %q in this scheduling cycle", identity.key(), resourceName)
				ambiguous = true
				continue
			}
			claimedBy[identity.key()] = resourceName
		}
		if ambiguous {
			continue
		}
		requirement.PhysicalAllocations[resourceName] = matching
	}
}

func resolveAllocatedDeviceIdentity(claimName string, allocated resourceapi.DeviceRequestAllocationResult, claimSelector *v1.NodeSelector, slices []*resourceapi.ResourceSlice) (physicalAllocationIdentity, string) {
	identity := physicalAllocationIdentity{
		ClaimName: claimName,
		Request:   allocated.Request,
		Driver:    strings.TrimSpace(allocated.Driver),
		Pool:      strings.TrimSpace(allocated.Pool),
		Device:    strings.TrimSpace(allocated.Device),
	}
	if claimSelector != nil {
		identity.ClaimNodeSelector = claimSelector.DeepCopy()
	}
	if identity.Driver == "" || identity.Pool == "" || identity.Device == "" {
		return identity, fmt.Sprintf("ResourceClaim %q contains an incomplete physical allocation identity", claimName)
	}

	var poolSlices []*resourceapi.ResourceSlice
	maxGeneration := int64(-1)
	for _, slice := range slices {
		if slice == nil || slice.Spec.Driver != identity.Driver || slice.Spec.Pool.Name != identity.Pool {
			continue
		}
		poolSlices = append(poolSlices, slice)
		if slice.Spec.Pool.Generation > maxGeneration {
			maxGeneration = slice.Spec.Pool.Generation
		}
	}
	if len(poolSlices) == 0 {
		return identity, fmt.Sprintf("allocated device %s/%s/%s has no ResourceSlice inventory", identity.Driver, identity.Pool, identity.Device)
	}
	var current []*resourceapi.ResourceSlice
	expectedCount := int64(0)
	for _, slice := range poolSlices {
		if slice.Spec.Pool.Generation != maxGeneration {
			continue
		}
		if expectedCount == 0 {
			expectedCount = slice.Spec.Pool.ResourceSliceCount
		} else if expectedCount != slice.Spec.Pool.ResourceSliceCount {
			return identity, fmt.Sprintf("ResourceSlice pool %s/%s has inconsistent slice counts at generation %d", identity.Driver, identity.Pool, maxGeneration)
		}
		current = append(current, slice)
	}
	if expectedCount <= 0 || int64(len(current)) != expectedCount {
		return identity, fmt.Sprintf("ResourceSlice pool %s/%s generation %d is incomplete or stale: have %d slice(s), expect %d", identity.Driver, identity.Pool, maxGeneration, len(current), expectedCount)
	}

	matches := 0
	foundOlder := false
	var matchedSlice *resourceapi.ResourceSlice
	var matchedDevice *resourceapi.Device
	for _, slice := range poolSlices {
		for i := range slice.Spec.Devices {
			device := &slice.Spec.Devices[i]
			if canonicalDeviceIdentity(device.Name) != canonicalDeviceIdentity(identity.Device) {
				continue
			}
			if slice.Spec.Pool.Generation == maxGeneration {
				matches++
				matchedSlice = slice
				matchedDevice = device
			} else {
				foundOlder = true
			}
		}
	}
	if matches == 0 {
		if foundOlder {
			return identity, fmt.Sprintf("allocated device %s/%s/%s is stale and exists only in an older ResourceSlice generation", identity.Driver, identity.Pool, identity.Device)
		}
		return identity, fmt.Sprintf("allocated device %s/%s/%s does not exist in the current ResourceSlice inventory", identity.Driver, identity.Pool, identity.Device)
	}
	if matches != 1 {
		return identity, fmt.Sprintf("allocated device %s/%s/%s appears %d times in current ResourceSlice inventory", identity.Driver, identity.Pool, identity.Device, matches)
	}

	if matchedSlice.Spec.PerDeviceNodeSelection != nil && *matchedSlice.Spec.PerDeviceNodeSelection {
		if matchedDevice.Basic == nil {
			return identity, fmt.Sprintf("allocated device %s/%s/%s has no BasicDevice node selection", identity.Driver, identity.Pool, identity.Device)
		}
		basic := matchedDevice.Basic
		if basic.NodeName != nil {
			identity.NodeName = *basic.NodeName
		}
		if basic.NodeSelector != nil {
			identity.NodeSelector = basic.NodeSelector.DeepCopy()
		}
		if basic.AllNodes != nil {
			identity.AllNodes = *basic.AllNodes
		}
	} else {
		identity.NodeName = matchedSlice.Spec.NodeName
		identity.AllNodes = matchedSlice.Spec.AllNodes
		if matchedSlice.Spec.NodeSelector != nil {
			identity.NodeSelector = matchedSlice.Spec.NodeSelector.DeepCopy()
		}
	}
	if identity.NodeName == "" && !identity.AllNodes && identity.NodeSelector == nil {
		return identity, fmt.Sprintf("allocated device %s/%s/%s has no usable ResourceSlice node selection", identity.Driver, identity.Pool, identity.Device)
	}
	return identity, ""
}
