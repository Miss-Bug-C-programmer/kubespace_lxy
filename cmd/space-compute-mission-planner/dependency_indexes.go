package main

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	spacekube "github.com/k3s-io/k3s/contrib/space-compute/pkg/kube"
)

const (
	missionUIDIndex               = "spacecompute.missionUID"
	missionDomainIndex            = "spacecompute.missionDomain"
	missionInputDomainIndex       = "spacecompute.inputDomain"
	missionInputLocationIndex     = "spacecompute.inputLocation"
	missionResultDomainIndex      = "spacecompute.resultDomain"
	missionResultDestinationIndex = "spacecompute.resultDestination"
	missionCapabilityIndex        = "spacecompute.capabilityClass"
	placementTargetIndex          = "spacecompute.placementTarget"
)

func addPlannerDependencyIndexers(missions, placements cache.SharedIndexInformer) error {
	if err := missions.AddIndexers(cache.Indexers{
		missionUIDIndex:               indexMissionUID,
		missionDomainIndex:            indexMissionDomains,
		missionInputDomainIndex:       indexMissionInputDomains,
		missionInputLocationIndex:     indexMissionInputLocations,
		missionResultDomainIndex:      indexMissionResultDomains,
		missionResultDestinationIndex: indexMissionResultDestinations,
		missionCapabilityIndex:        indexMissionCapabilityClasses,
	}); err != nil {
		return fmt.Errorf("add mission dependency indexes: %w", err)
	}
	if err := placements.AddIndexers(cache.Indexers{
		spacekube.PlacementMissionUIDIndex: indexPlacementMissionUID,
		placementTargetIndex:               indexPlacementTarget,
	}); err != nil {
		return fmt.Errorf("add placement dependency indexes: %w", err)
	}
	return nil
}

func indexMissionUID(object interface{}) ([]string, error) {
	mission, err := missionForIndex(object)
	if err != nil || mission == nil || mission.UID == "" {
		return nil, err
	}
	return []string{string(mission.UID)}, nil
}
func indexMissionDomains(object interface{}) ([]string, error) {
	mission, err := missionForIndex(object)
	if err != nil || mission == nil {
		return nil, err
	}
	set := map[string]struct{}{}
	for _, input := range mission.Spec.Inputs {
		for _, location := range input.Locations {
			set[domainDependencyKey(location.Domain)] = struct{}{}
		}
	}
	for _, location := range mission.Spec.ResultDestinations {
		set[domainDependencyKey(location.Domain)] = struct{}{}
	}
	return sortedSetKeys(set), nil
}
func indexMissionInputDomains(object interface{}) ([]string, error) {
	mission, err := missionForIndex(object)
	if err != nil || mission == nil {
		return nil, err
	}
	set := map[string]struct{}{}
	for _, input := range mission.Spec.Inputs {
		for _, location := range input.Locations {
			set[domainDependencyKey(location.Domain)] = struct{}{}
		}
	}
	return sortedSetKeys(set), nil
}

func indexMissionInputLocations(object interface{}) ([]string, error) {
	mission, err := missionForIndex(object)
	if err != nil || mission == nil {
		return nil, err
	}
	set := map[string]struct{}{}
	for _, input := range mission.Spec.Inputs {
		for _, location := range input.Locations {
			set[dataLocationDependencyKey(location)] = struct{}{}
		}
	}
	return sortedSetKeys(set), nil
}

func indexMissionResultDomains(object interface{}) ([]string, error) {
	mission, err := missionForIndex(object)
	if err != nil || mission == nil {
		return nil, err
	}
	set := map[string]struct{}{}
	for _, location := range mission.Spec.ResultDestinations {
		set[domainDependencyKey(location.Domain)] = struct{}{}
	}
	return sortedSetKeys(set), nil
}

func indexMissionResultDestinations(object interface{}) ([]string, error) {
	mission, err := missionForIndex(object)
	if err != nil || mission == nil {
		return nil, err
	}
	set := map[string]struct{}{}
	for _, location := range mission.Spec.ResultDestinations {
		set[dataLocationDependencyKey(location)] = struct{}{}
	}
	return sortedSetKeys(set), nil
}

func indexMissionCapabilityClasses(object interface{}) ([]string, error) {
	mission, err := missionForIndex(object)
	if err != nil || mission == nil {
		return nil, err
	}
	set := map[string]struct{}{}
	for _, requirement := range mission.Spec.RequiredCapabilities {
		if requirement.Class != "" {
			set[requirement.Class] = struct{}{}
		}
	}
	for _, alternative := range mission.Spec.AlternativeCapabilities {
		for _, requirement := range alternative.AllOf {
			if requirement.Class != "" {
				set[requirement.Class] = struct{}{}
			}
		}
	}
	return sortedSetKeys(set), nil
}
func indexPlacementMissionUID(object interface{}) ([]string, error) {
	placement, err := placementForIndex(object)
	if err != nil || placement == nil || placement.Spec.MissionRef.UID == "" {
		return nil, err
	}
	return []string{string(placement.Spec.MissionRef.UID)}, nil
}
func indexPlacementTarget(object interface{}) ([]string, error) {
	placement, err := placementForIndex(object)
	if err != nil || placement == nil || placement.Spec.Target.Name == "" {
		return nil, err
	}
	return []string{domainDependencyKey(placement.Spec.Target)}, nil
}

func enqueueResourceDependents(value interface{}, missions, placements cache.Indexer, queue workqueue.RateLimitingInterface) {
	u := unstructuredFromEvent(value)
	if u == nil {
		return
	}
	summary := &spacev1.SpaceDomainResourceSummary{}
	if runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, summary) != nil {
		return
	}
	keys := map[string]struct{}{}
	collectMissionKeysByIndex(missions, missionDomainIndex, domainDependencyKey(summary.Spec.Domain), keys)
	collectPlacementMissionKeysByTarget(placements, domainDependencyKey(summary.Spec.Domain), keys)
	for _, device := range summary.Spec.Devices {
		collectMissionKeysByIndex(missions, missionCapabilityIndex, device.Class, keys)
	}
	addMissionKeys(queue, keys)
}

func enqueueLinkDependents(value interface{}, missions, placements cache.Indexer, queue workqueue.RateLimitingInterface) {
	u := unstructuredFromEvent(value)
	if u == nil {
		return
	}
	link := &spacev1.SpaceLinkSnapshot{}
	if runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, link) != nil {
		return
	}
	keys := map[string]struct{}{}
	for _, domain := range []spacev1.DomainReference{link.Spec.Source, link.Spec.Destination} {
		domainKey := domainDependencyKey(domain)
		collectMissionKeysByIndex(missions, missionInputDomainIndex, domainKey, keys)
		collectMissionKeysByIndex(missions, missionResultDomainIndex, domainKey, keys)
		collectPlacementMissionKeysByTarget(placements, domainKey, keys)
	}
	addMissionKeys(queue, keys)
}

func enqueueEvidenceDependent(value interface{}, missions cache.Indexer, queue workqueue.RateLimitingInterface) {
	u := unstructuredFromEvent(value)
	if u == nil {
		return
	}
	uid, _, _ := unstructured.NestedString(u.Object, "spec", "missionUID")
	if uid == "" {
		return
	}
	keys := map[string]struct{}{}
	collectMissionKeysByIndex(missions, missionUIDIndex, uid, keys)
	addMissionKeys(queue, keys)
}

func collectMissionKeysByIndex(indexer cache.Indexer, indexName, value string, keys map[string]struct{}) {
	if value == "" {
		return
	}
	objects, err := indexer.ByIndex(indexName, value)
	if err != nil {
		return
	}
	for _, object := range objects {
		if key, err := cache.MetaNamespaceKeyFunc(object); err == nil {
			keys[key] = struct{}{}
		}
	}
}
func collectPlacementMissionKeysByTarget(indexer cache.Indexer, target string, keys map[string]struct{}) {
	objects, err := indexer.ByIndex(placementTargetIndex, target)
	if err != nil {
		return
	}
	for _, object := range objects {
		placement, err := placementForIndex(object)
		if err == nil && placement != nil && placement.Spec.MissionRef.Namespace != "" && placement.Spec.MissionRef.Name != "" {
			keys[placement.Spec.MissionRef.Namespace+"/"+placement.Spec.MissionRef.Name] = struct{}{}
		}
	}
}
func addMissionKeys(queue workqueue.RateLimitingInterface, keys map[string]struct{}) {
	for key := range keys {
		queue.Add(key)
	}
}

func missionForIndex(object interface{}) (*spacev1.SpaceMission, error) {
	u := unstructuredFromEvent(object)
	if u == nil {
		return nil, nil
	}
	mission := &spacev1.SpaceMission{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, mission); err != nil {
		return nil, err
	}
	return mission, nil
}
func placementForIndex(object interface{}) (*spacev1.SpacePlacementIntent, error) {
	u := unstructuredFromEvent(object)
	if u == nil {
		return nil, nil
	}
	placement := &spacev1.SpacePlacementIntent{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, placement); err != nil {
		return nil, err
	}
	return placement, nil
}
func unstructuredFromEvent(value interface{}) *unstructured.Unstructured {
	if object, ok := value.(*unstructured.Unstructured); ok {
		return object
	}
	if tombstone, ok := value.(cache.DeletedFinalStateUnknown); ok {
		object, _ := tombstone.Obj.(*unstructured.Unstructured)
		return object
	}
	return nil
}
func domainDependencyKey(domain spacev1.DomainReference) string {
	return domain.ClusterID + "/" + domain.Name + "/" + string(domain.OrbitClass)
}

func dataLocationDependencyKey(location spacev1.DataLocation) string {
	return domainDependencyKey(location.Domain) + "|" + location.URI
}

func sortedSetKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	// Informer indexes do not depend on order, but deterministic output simplifies
	// replay tests and avoids unnecessary index churn.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
