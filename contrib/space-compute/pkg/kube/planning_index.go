package kube

import (
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	"github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"
)

// PlanningIndex converts informer objects once at event time and retains an
// immutable typed generation. PlanningSnapshot therefore reuses prepared
// indexes/canonical fragments across mission reconciles instead of List()+deep
// copying the complete informer stores for every mission.
type PlanningIndex struct {
	mu        sync.Mutex
	resources map[string]*spacev1.SpaceDomainResourceSummary
	links     map[string]*spacev1.SpaceLinkSnapshot
	dirty     bool
	prepared  *planner.PreparedPlanningInputs
	snapshot  *planner.PlanningInputSnapshot
	lastErr   error
}

func NewPlanningIndex() *PlanningIndex {
	return &PlanningIndex{
		resources: make(map[string]*spacev1.SpaceDomainResourceSummary),
		links:     make(map[string]*spacev1.SpaceLinkSnapshot),
		dirty:     true,
	}
}

func (i *PlanningIndex) UpsertResource(object interface{}) error {
	value, err := immutableResourceSummary(object)
	if err != nil {
		i.setError(err)
		return err
	}
	i.mu.Lock()
	i.resources[value.Name] = value
	i.dirty = true
	i.snapshot = nil
	i.lastErr = nil
	i.mu.Unlock()
	return nil
}

func (i *PlanningIndex) DeleteResource(object interface{}) {
	if name := clusterObjectName(object); name != "" {
		i.mu.Lock()
		delete(i.resources, name)
		i.dirty = true
		i.snapshot = nil
		i.lastErr = nil
		i.mu.Unlock()
	}
}

func (i *PlanningIndex) UpsertLink(object interface{}) error {
	value, err := immutableLinkSnapshot(object)
	if err != nil {
		i.setError(err)
		return err
	}
	i.mu.Lock()
	i.links[value.Name] = value
	i.dirty = true
	i.snapshot = nil
	i.lastErr = nil
	i.mu.Unlock()
	return nil
}

func (i *PlanningIndex) DeleteLink(object interface{}) {
	if name := clusterObjectName(object); name != "" {
		i.mu.Lock()
		delete(i.links, name)
		i.dirty = true
		i.snapshot = nil
		i.lastErr = nil
		i.mu.Unlock()
	}
}

func (i *PlanningIndex) Snapshot(versions map[string]string) (*planner.PlanningInputSnapshot, error) {
	if i == nil {
		return nil, fmt.Errorf("planning index is required")
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.lastErr != nil {
		return nil, i.lastErr
	}
	if i.dirty || i.prepared == nil {
		summaries := make([]*spacev1.SpaceDomainResourceSummary, 0, len(i.resources))
		for _, value := range i.resources {
			summaries = append(summaries, value)
		}
		links := make([]*spacev1.SpaceLinkSnapshot, 0, len(i.links))
		for _, value := range i.links {
			links = append(links, value)
		}
		prepared, err := planner.PreparePlanningInputs(summaries, links)
		if err != nil {
			return nil, err
		}
		i.prepared = prepared
		i.dirty = false
		i.snapshot = nil
	}
	if i.snapshot != nil && sameStringMap(i.snapshot.CacheVersions, versions) {
		return i.snapshot, nil
	}
	i.snapshot = &planner.PlanningInputSnapshot{
		ResourceSummaries: i.prepared.ResourceSummaries(),
		LinkSnapshots:     i.prepared.LinkSnapshots(),
		Prepared:          i.prepared,
		CacheVersions:     cloneStringMapLocal(versions),
		InputDigest:       i.prepared.InputDigest(),
	}
	return i.snapshot, nil
}

func (i *PlanningIndex) setError(err error) {
	i.mu.Lock()
	i.lastErr = err
	i.mu.Unlock()
}

func immutableResourceSummary(object interface{}) (*spacev1.SpaceDomainResourceSummary, error) {
	switch value := object.(type) {
	case *spacev1.SpaceDomainResourceSummary:
		return value.DeepCopy(), nil
	case *unstructured.Unstructured:
		out := &spacev1.SpaceDomainResourceSummary{}
		if err := fromUnstructured(value, out); err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unexpected resource summary informer object %T", object)
	}
}

func immutableLinkSnapshot(object interface{}) (*spacev1.SpaceLinkSnapshot, error) {
	switch value := object.(type) {
	case *spacev1.SpaceLinkSnapshot:
		return value.DeepCopy(), nil
	case *unstructured.Unstructured:
		out := &spacev1.SpaceLinkSnapshot{}
		if err := fromUnstructured(value, out); err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unexpected link snapshot informer object %T", object)
	}
}

func clusterObjectName(object interface{}) string {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(object)
	if err != nil {
		return ""
	}
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return ""
	}
	return name
}

func cloneStringMapLocal(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
