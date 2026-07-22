package gpustability

import (
	"sort"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

type snapshotConfidence string

const (
	confidenceValidated snapshotConfidence = "validated"
	confidenceDegraded  snapshotConfidence = "degraded"
	confidenceStale     snapshotConfidence = "stale"
	confidenceMissing   snapshotConfidence = "missing"
	confidenceFailed    snapshotConfidence = "failed"
)

type nodeIdentity struct {
	Name string
	UID  types.UID
}

func identityForNode(node *v1.Node) nodeIdentity {
	if node == nil {
		return nodeIdentity{}
	}
	return nodeIdentity{Name: node.Name, UID: node.UID}
}

type nodeResourceContext struct {
	Allocatable map[v1.ResourceName]int64
	Requested   map[v1.ResourceName]int64
}

type unifiedSnapshot struct {
	Identity         nodeIdentity
	TargetKey        string
	TargetGeneration uint64
	Profile          string
	Endpoint         string
	Metrics          nodeMetrics
	Resources        nodeResourceContext
	ObservedAt       time.Time
	ValidUntil       time.Time
	CollectionError  string
	Confidence       snapshotConfidence
	LastAccess       time.Time
}

type snapshotStore struct {
	mu         sync.RWMutex
	maxEntries int
	records    map[string]unifiedSnapshot
}

func newSnapshotStore(maxEntries int) *snapshotStore {
	return &snapshotStore{maxEntries: maxEntries, records: make(map[string]unifiedSnapshot)}
}

func (s *snapshotStore) transition(target scrapeTarget, resources nodeResourceContext, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.records[target.NodeName]
	if exists && record.Identity == target.Identity && record.TargetGeneration == target.Generation && record.TargetKey == target.Key {
		record.Resources = cloneResourceContext(resources)
		record.LastAccess = now
		s.records[target.NodeName] = record
		return
	}
	s.records[target.NodeName] = unifiedSnapshot{
		Identity: target.Identity, TargetKey: target.Key, TargetGeneration: target.Generation,
		Profile: target.Profile, Endpoint: target.Endpoint, Resources: cloneResourceContext(resources),
		Confidence: confidenceMissing, LastAccess: now,
	}
	s.pruneLocked()
}

func (s *snapshotStore) publish(target scrapeTarget, metrics nodeMetrics, observedAt, validUntil time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.records[target.NodeName]
	if !exists || record.Identity != target.Identity || record.TargetGeneration != target.Generation || record.TargetKey != target.Key {
		return false
	}
	record.Metrics = cloneNodeMetrics(metrics)
	record.Profile = metrics.Profile
	record.ObservedAt = observedAt
	record.ValidUntil = validUntil
	record.CollectionError = ""
	record.Confidence = confidenceValidated
	record.LastAccess = observedAt
	s.records[target.NodeName] = record
	return true
}

func (s *snapshotStore) recordFailure(target scrapeTarget, message string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.records[target.NodeName]
	if !exists || record.Identity != target.Identity || record.TargetGeneration != target.Generation || record.TargetKey != target.Key {
		return false
	}
	record.CollectionError = message
	if record.ObservedAt.IsZero() {
		record.Confidence = confidenceFailed
	}
	record.LastAccess = now
	s.records[target.NodeName] = record
	return true
}

func (s *snapshotStore) lookup(target scrapeTarget, now time.Time) snapshotResult {
	s.mu.RLock()
	record, exists := s.records[target.NodeName]
	s.mu.RUnlock()
	if !exists || record.Identity != target.Identity || record.TargetGeneration != target.Generation || record.TargetKey != target.Key {
		return snapshotResult{State: snapshotMissing, Confidence: confidenceMissing, Reason: "telemetry snapshot is not available"}
	}
	result := snapshotResult{
		Metrics: cloneNodeMetrics(record.Metrics), Resources: cloneResourceContext(record.Resources),
		ObservedAt: record.ObservedAt, ValidUntil: record.ValidUntil, Profile: record.Profile,
		TargetGeneration: record.TargetGeneration, Confidence: record.Confidence,
	}
	if record.ObservedAt.IsZero() {
		if record.CollectionError != "" {
			result.State = snapshotFailed
			result.Confidence = confidenceFailed
			result.Reason = record.CollectionError
			return result
		}
		result.State = snapshotMissing
		result.Confidence = confidenceMissing
		result.Reason = "telemetry snapshot is not available"
		return result
	}
	if now.After(record.ValidUntil) {
		result.State = snapshotStale
		result.Confidence = confidenceStale
		result.Reason = "telemetry snapshot is stale"
		if record.CollectionError != "" {
			result.Reason += ": " + record.CollectionError
		}
		return result
	}
	result.State = snapshotFresh
	if record.CollectionError != "" {
		result.Confidence = confidenceDegraded
		result.Reason = "using last valid telemetry after collection failure: " + record.CollectionError
	} else {
		result.Confidence = confidenceValidated
	}
	return result
}

func (s *snapshotStore) updateResources(target scrapeTarget, resources nodeResourceContext) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.records[target.NodeName]
	if !exists || record.Identity != target.Identity || record.TargetGeneration != target.Generation || record.TargetKey != target.Key {
		return
	}
	record.Resources = cloneResourceContext(resources)
	s.records[target.NodeName] = record
}

func (s *snapshotStore) delete(identity nodeIdentity) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.records[identity.Name]
	if !exists || (identity.UID != "" && record.Identity.UID != identity.UID) {
		return false
	}
	delete(s.records, identity.Name)
	return true
}

func (s *snapshotStore) remove(nodeName string) {
	s.mu.Lock()
	delete(s.records, nodeName)
	s.mu.Unlock()
}

func (s *snapshotStore) len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.records)
}

func (s *snapshotStore) clear() {
	s.mu.Lock()
	s.records = make(map[string]unifiedSnapshot)
	s.mu.Unlock()
}

func (s *snapshotStore) pruneLocked() {
	remove := len(s.records) - s.maxEntries
	if remove <= 0 {
		return
	}
	type candidate struct {
		name string
		time time.Time
	}
	candidates := make([]candidate, 0, len(s.records))
	for name, record := range s.records {
		when := record.LastAccess
		if when.IsZero() {
			when = record.ObservedAt
		}
		candidates = append(candidates, candidate{name: name, time: when})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].time.Equal(candidates[j].time) {
			return candidates[i].name < candidates[j].name
		}
		return candidates[i].time.Before(candidates[j].time)
	})
	for _, candidate := range candidates[:remove] {
		delete(s.records, candidate.name)
	}
}

func cloneResourceContext(in nodeResourceContext) nodeResourceContext {
	return nodeResourceContext{Allocatable: cloneResourceMap(in.Allocatable), Requested: cloneResourceMap(in.Requested)}
}

func cloneResourceMap(in map[v1.ResourceName]int64) map[v1.ResourceName]int64 {
	if in == nil {
		return nil
	}
	out := make(map[v1.ResourceName]int64, len(in))
	for name, value := range in {
		out[name] = value
	}
	return out
}

func cloneNodeMetrics(in nodeMetrics) nodeMetrics {
	out := in
	out.Fields = make(map[deviceMetricField]struct{}, len(in.Fields))
	for field := range in.Fields {
		out.Fields[field] = struct{}{}
	}
	out.Devices = make([]deviceMetrics, len(in.Devices))
	for i, device := range in.Devices {
		out.Devices[i] = device
		out.Devices[i].Fields = make(map[deviceMetricField]struct{}, len(device.Fields))
		for field := range device.Fields {
			out.Devices[i].Fields[field] = struct{}{}
		}
	}
	return out
}
