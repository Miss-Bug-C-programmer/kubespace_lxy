package gpustability

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

var _ framework.EnqueueExtensions = &Plugin{}

type blockedPod struct {
	pod      *v1.Pod
	lastSeen time.Time
}

type blockedPodIndex struct {
	mu      sync.Mutex
	byNode  map[string]map[string]blockedPod
	count   int
	maxPods int
	ttl     time.Duration
	now     func() time.Time
}

func newBlockedPodIndex(maxPods int, ttl time.Duration) *blockedPodIndex {
	return &blockedPodIndex{byNode: map[string]map[string]blockedPod{}, maxPods: maxPods, ttl: ttl, now: time.Now}
}

func (b *blockedPodIndex) track(nodeName string, pod *v1.Pod) {
	if b == nil || nodeName == "" || pod == nil {
		return
	}
	now := b.now()
	key := queuePodKey(pod)
	b.mu.Lock()
	b.pruneLocked(now)
	if b.byNode[nodeName] == nil {
		b.byNode[nodeName] = map[string]blockedPod{}
	}
	if _, exists := b.byNode[nodeName][key]; !exists {
		b.count++
	}
	b.byNode[nodeName][key] = blockedPod{pod: pod.DeepCopy(), lastSeen: now}
	b.pruneLocked(now)
	b.mu.Unlock()
	observeBlockedPods(b.len())
}

func (b *blockedPodIndex) remove(nodeName string, pod *v1.Pod) {
	if b == nil || pod == nil {
		return
	}
	b.mu.Lock()
	b.removeLocked(nodeName, queuePodKey(pod))
	b.mu.Unlock()
}

func (b *blockedPodIndex) blocked(nodeName string, pod *v1.Pod) bool {
	if b == nil || pod == nil {
		return false
	}
	b.mu.Lock()
	b.pruneLocked(b.now())
	_, exists := b.byNode[nodeName][queuePodKey(pod)]
	b.mu.Unlock()
	return exists
}

func (b *blockedPodIndex) takeNode(nodeName string) map[string]*v1.Pod {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	b.pruneLocked(b.now())
	entries := b.byNode[nodeName]
	delete(b.byNode, nodeName)
	b.count -= len(entries)
	b.mu.Unlock()
	result := make(map[string]*v1.Pod, len(entries))
	for key, entry := range entries {
		result[key] = entry.pod.DeepCopy()
	}
	observeBlockedPods(b.len())
	return result
}

func (b *blockedPodIndex) len() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count
}

func (b *blockedPodIndex) clear() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.byNode = map[string]map[string]blockedPod{}
	b.count = 0
	b.mu.Unlock()
	observeBlockedPods(0)
}

func (b *blockedPodIndex) pruneLocked(now time.Time) {
	type candidate struct {
		node string
		key  string
		when time.Time
	}
	candidates := make([]candidate, 0, b.count)
	for node, entries := range b.byNode {
		for key, entry := range entries {
			if now.Sub(entry.lastSeen) >= b.ttl {
				b.removeLocked(node, key)
				continue
			}
			candidates = append(candidates, candidate{node: node, key: key, when: entry.lastSeen})
		}
	}
	if b.count <= b.maxPods {
		return
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].when.Equal(candidates[j].when) {
			if candidates[i].node == candidates[j].node {
				return candidates[i].key < candidates[j].key
			}
			return candidates[i].node < candidates[j].node
		}
		return candidates[i].when.Before(candidates[j].when)
	})
	for _, candidate := range candidates[:b.count-b.maxPods] {
		b.removeLocked(candidate.node, candidate.key)
	}
}

func (b *blockedPodIndex) removeLocked(nodeName, key string) {
	entries := b.byNode[nodeName]
	if _, exists := entries[key]; !exists {
		return
	}
	delete(entries, key)
	b.count--
	if len(entries) == 0 {
		delete(b.byNode, nodeName)
	}
}

func queuePodKey(pod *v1.Pod) string {
	if pod.UID != "" {
		return string(pod.UID)
	}
	return pod.Namespace + "/" + pod.Name
}

func (p *Plugin) activateForSnapshot(nodeName string, _ uint64) {
	if p == nil || p.handle == nil || p.blocked == nil {
		return
	}
	pods := p.blocked.takeNode(nodeName)
	if len(pods) == 0 {
		return
	}
	p.handle.Activate(klog.Background(), pods)
	observeSnapshotActivations(len(pods))
}

func (p *Plugin) EventsToRegister(context.Context) ([]framework.ClusterEventWithHint, error) {
	return []framework.ClusterEventWithHint{{
		Event: framework.ClusterEvent{
			Resource: framework.Node,
			ActionType: framework.Add | framework.UpdateNodeAllocatable |
				framework.UpdateNodeLabel | framework.UpdateNodeAnnotation,
		},
		QueueingHintFn: p.queueOnNodeChange,
	}}, nil
}

func (p *Plugin) queueOnNodeChange(_ klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	requirement, err := p.schedulingRequirement(pod)
	if err != nil {
		return framework.Queue, err
	}
	if !requirement.Required {
		return framework.QueueSkip, nil
	}
	newNode, newOK := newObj.(*v1.Node)
	if !newOK || newNode == nil {
		return framework.QueueSkip, nil
	}
	oldNode, oldOK := oldObj.(*v1.Node)
	if !oldOK || oldNode == nil {
		if nodeCouldSupply(newNode, requirement) {
			return framework.Queue, nil
		}
		return framework.QueueSkip, nil
	}
	if p.blocked == nil || !p.blocked.blocked(newNode.Name, pod) {
		return framework.QueueSkip, nil
	}
	if relevantNodeChange(oldNode, newNode, requirement) {
		return framework.Queue, nil
	}
	return framework.QueueSkip, nil
}

func nodeCouldSupply(node *v1.Node, requirement *workloadRequirement) bool {
	if requirement.Space != nil {
		target := requirement.Space.Placement.Spec.Target
		if node.Labels[spacev1.LabelDomain] != target.Name || node.Labels[spacev1.LabelOrbitClass] != string(target.OrbitClass) {
			return false
		}
	}
	for name, demand := range requirement.Resources {
		quantity, exists := node.Status.Allocatable[name]
		if !exists || quantity.Value() < demand {
			return false
		}
	}
	return labelsMatch(node.Labels, requirement.RequiredNodeLabels)
}

func relevantNodeChange(oldNode, newNode *v1.Node, requirement *workloadRequirement) bool {
	for name := range requirement.Resources {
		oldQuantity := oldNode.Status.Allocatable[name]
		newQuantity := newNode.Status.Allocatable[name]
		if oldQuantity.Cmp(newQuantity) != 0 {
			return true
		}
	}
	if !equality.Semantic.DeepEqual(oldNode.Status.Addresses, newNode.Status.Addresses) {
		return true
	}
	for _, key := range []string{AnnotationExporterPort, AnnotationExporterPath, AnnotationExporterScheme, AnnotationExporterProfile} {
		if oldNode.Annotations[key] != newNode.Annotations[key] {
			return true
		}
	}
	if requirement.Space != nil {
		for _, key := range []string{spacev1.AnnotationLinkProjection} {
			if oldNode.Annotations[key] != newNode.Annotations[key] {
				return true
			}
		}
		for _, key := range []string{spacev1.LabelDomain, spacev1.LabelOrbitClass, spacev1.LabelPlacementID} {
			if oldNode.Labels[key] != newNode.Labels[key] {
				return true
			}
		}
	}
	for key := range requirement.RequiredNodeLabels {
		if oldNode.Labels[key] != newNode.Labels[key] {
			return true
		}
	}
	return false
}

func labelsMatch(actual, required map[string]string) bool {
	for key, value := range required {
		if actual[key] != value {
			return false
		}
	}
	return true
}

func labelMismatchReason(actual, required map[string]string) string {
	keys := make([]string, 0, len(required))
	for key := range required {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if actual[key] != required[key] {
			return fmt.Sprintf("node label %q does not match required value", key)
		}
	}
	return ""
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func copyStringSet(in map[string]struct{}) map[string]struct{} {
	if in == nil {
		return nil
	}
	out := make(map[string]struct{}, len(in))
	for key := range in {
		out[key] = struct{}{}
	}
	return out
}
