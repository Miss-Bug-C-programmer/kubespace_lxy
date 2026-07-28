package gpustability

import (
	"container/list"
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
	Allocatable      map[v1.ResourceName]int64
	ObservedAt       time.Time
	ValidUntil       time.Time
	CollectionError  string
	Confidence       snapshotConfidence
	LastAccess       time.Time
}

type snapshotEntry struct {
	name   string
	record unifiedSnapshot
}

type snapshotShard struct {
	mu      sync.Mutex
	records map[string]*list.Element
	lru     list.List
}

// snapshotStore is a bounded sharded LRU. Reads update only eviction weight;
// observation and validity timestamps are immutable on lookup, so cache access
// can never make telemetry fresher than the exporter observation.
type snapshotStore struct {
	maxEntries int
	shards     []snapshotShard

	evictionMu sync.Mutex
	size       int
}

func newSnapshotStore(maxEntries int) *snapshotStore {
	if maxEntries < 1 {
		maxEntries = 1
	}
	shardCount := 1
	if maxEntries >= 64 {
		shardCount = 16
		if maxEntries < shardCount {
			shardCount = maxEntries
		}
	}
	s := &snapshotStore{maxEntries: maxEntries, shards: make([]snapshotShard, shardCount)}
	for i := range s.shards {
		s.shards[i] = snapshotShard{records: make(map[string]*list.Element)}
	}
	return s
}

func (s *snapshotStore) shardFor(name string) *snapshotShard {
	if len(s.shards) == 1 {
		return &s.shards[0]
	}
	// Inline FNV-1a avoids allocating a hash.Hash object on the scheduler read path.
	var hash uint32 = 2166136261
	for index := 0; index < len(name); index++ {
		hash ^= uint32(name[index])
		hash *= 16777619
	}
	return &s.shards[int(hash%uint32(len(s.shards)))]
}

func (s *snapshotStore) transition(target scrapeTarget, resources nodeResourceContext, now time.Time) {
	shard := s.shardFor(target.NodeName)
	shard.mu.Lock()
	if element := shard.records[target.NodeName]; element != nil {
		entry := element.Value.(*snapshotEntry)
		if entry.record.Identity == target.Identity && entry.record.TargetGeneration == target.Generation && entry.record.TargetKey == target.Key {
			entry.record.Allocatable = cloneResourceMap(resources.Allocatable)
			entry.record.LastAccess = now
			shard.lru.MoveToFront(element)
			shard.mu.Unlock()
			return
		}
		entry.record = unifiedSnapshot{
			Identity: target.Identity, TargetKey: target.Key, TargetGeneration: target.Generation,
			Profile: target.Profile, Endpoint: target.Endpoint, Allocatable: cloneResourceMap(resources.Allocatable),
			Confidence: confidenceMissing, LastAccess: now,
		}
		shard.lru.MoveToFront(element)
		shard.mu.Unlock()
		return
	}
	shard.mu.Unlock()

	// Inserts/deletes serialize only at the eviction boundary. Ordinary lookups and
	// updates remain shard-local. When globally full, inspect only the tail of each
	// fixed shard (never every entry) and evict the oldest tail before inserting.
	s.evictionMu.Lock()
	defer s.evictionMu.Unlock()
	shard.mu.Lock()
	if element := shard.records[target.NodeName]; element != nil {
		entry := element.Value.(*snapshotEntry)
		entry.record = unifiedSnapshot{
			Identity: target.Identity, TargetKey: target.Key, TargetGeneration: target.Generation,
			Profile: target.Profile, Endpoint: target.Endpoint, Allocatable: cloneResourceMap(resources.Allocatable),
			Confidence: confidenceMissing, LastAccess: now,
		}
		shard.lru.MoveToFront(element)
		shard.mu.Unlock()
		return
	}
	shard.mu.Unlock()
	if s.size >= s.maxEntries {
		s.evictOldestLocked()
	}
	shard.mu.Lock()
	entry := &snapshotEntry{name: target.NodeName, record: unifiedSnapshot{
		Identity: target.Identity, TargetKey: target.Key, TargetGeneration: target.Generation,
		Profile: target.Profile, Endpoint: target.Endpoint, Allocatable: cloneResourceMap(resources.Allocatable),
		Confidence: confidenceMissing, LastAccess: now,
	}}
	shard.records[target.NodeName] = shard.lru.PushFront(entry)
	shard.mu.Unlock()
	s.size++
}

func (s *snapshotStore) publish(target scrapeTarget, metrics nodeMetrics, observedAt, validUntil time.Time) bool {
	shard := s.shardFor(target.NodeName)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	element := shard.records[target.NodeName]
	if element == nil {
		return false
	}
	entry := element.Value.(*snapshotEntry)
	if entry.record.Identity != target.Identity || entry.record.TargetGeneration != target.Generation || entry.record.TargetKey != target.Key {
		return false
	}
	// collectTarget transfers ownership of metrics to the store and never mutates
	// it after publication. Scheduler reads clone it into cycle-local state.
	entry.record.Metrics = metrics
	entry.record.Profile = metrics.Profile
	entry.record.ObservedAt = observedAt
	entry.record.ValidUntil = validUntil
	entry.record.CollectionError = ""
	entry.record.Confidence = confidenceValidated
	entry.record.LastAccess = observedAt
	shard.lru.MoveToFront(element)
	return true
}

func (s *snapshotStore) recordFailure(target scrapeTarget, message string, now time.Time) bool {
	shard := s.shardFor(target.NodeName)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	element := shard.records[target.NodeName]
	if element == nil {
		return false
	}
	entry := element.Value.(*snapshotEntry)
	if entry.record.Identity != target.Identity || entry.record.TargetGeneration != target.Generation || entry.record.TargetKey != target.Key {
		return false
	}
	entry.record.CollectionError = message
	if entry.record.ObservedAt.IsZero() {
		entry.record.Confidence = confidenceFailed
	}
	entry.record.LastAccess = now
	shard.lru.MoveToFront(element)
	return true
}

func (s *snapshotStore) lookup(target scrapeTarget, now time.Time) snapshotResult {
	shard := s.shardFor(target.NodeName)
	shard.mu.Lock()
	element := shard.records[target.NodeName]
	if element == nil {
		shard.mu.Unlock()
		return snapshotResult{State: snapshotMissing, Confidence: confidenceMissing, Reason: "telemetry snapshot is not available"}
	}
	entry := element.Value.(*snapshotEntry)
	if entry.record.Identity != target.Identity || entry.record.TargetGeneration != target.Generation || entry.record.TargetKey != target.Key {
		shard.mu.Unlock()
		return snapshotResult{State: snapshotMissing, Confidence: confidenceMissing, Reason: "telemetry snapshot is not available"}
	}
	entry.record.LastAccess = now
	shard.lru.MoveToFront(element)
	record := entry.record
	shard.mu.Unlock()

	result := snapshotResult{
		Metrics: cloneNodeMetrics(record.Metrics), Resources: nodeResourceContext{Allocatable: cloneResourceMap(record.Allocatable)},
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
	shard := s.shardFor(target.NodeName)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	element := shard.records[target.NodeName]
	if element == nil {
		return
	}
	entry := element.Value.(*snapshotEntry)
	if entry.record.Identity != target.Identity || entry.record.TargetGeneration != target.Generation || entry.record.TargetKey != target.Key {
		return
	}
	entry.record.Allocatable = cloneResourceMap(resources.Allocatable)
	shard.lru.MoveToFront(element)
}

func (s *snapshotStore) delete(identity nodeIdentity) bool {
	s.evictionMu.Lock()
	defer s.evictionMu.Unlock()
	shard := s.shardFor(identity.Name)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	element := shard.records[identity.Name]
	if element == nil {
		return false
	}
	entry := element.Value.(*snapshotEntry)
	if identity.UID != "" && entry.record.Identity.UID != identity.UID {
		return false
	}
	delete(shard.records, identity.Name)
	shard.lru.Remove(element)
	s.size--
	return true
}

func (s *snapshotStore) remove(nodeName string) {
	s.evictionMu.Lock()
	defer s.evictionMu.Unlock()
	shard := s.shardFor(nodeName)
	shard.mu.Lock()
	if element := shard.records[nodeName]; element != nil {
		delete(shard.records, nodeName)
		shard.lru.Remove(element)
		s.size--
	}
	shard.mu.Unlock()
}

func (s *snapshotStore) len() int {
	s.evictionMu.Lock()
	defer s.evictionMu.Unlock()
	return s.size
}

func (s *snapshotStore) contains(nodeName string) bool {
	shard := s.shardFor(nodeName)
	shard.mu.Lock()
	_, exists := shard.records[nodeName]
	shard.mu.Unlock()
	return exists
}

func (s *snapshotStore) rangeRecords(fn func(string, unifiedSnapshot)) {
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		for name, element := range shard.records {
			fn(name, element.Value.(*snapshotEntry).record)
		}
		shard.mu.Unlock()
	}
}

func (s *snapshotStore) clear() {
	s.evictionMu.Lock()
	defer s.evictionMu.Unlock()
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		shard.records = make(map[string]*list.Element)
		shard.lru.Init()
		shard.mu.Unlock()
	}
	s.size = 0
}

// evictOldestLocked chooses the globally oldest LRU tail while examining at
// most one entry per shard. evictionMu must be held by the caller.
func (s *snapshotStore) evictOldestLocked() {
	for i := range s.shards {
		s.shards[i].mu.Lock()
	}
	oldestShard := -1
	var oldest *list.Element
	for i := range s.shards {
		element := s.shards[i].lru.Back()
		if element == nil {
			continue
		}
		if oldest == nil || element.Value.(*snapshotEntry).record.LastAccess.Before(oldest.Value.(*snapshotEntry).record.LastAccess) {
			oldest = element
			oldestShard = i
		}
	}
	if oldestShard >= 0 {
		shard := &s.shards[oldestShard]
		entry := oldest.Value.(*snapshotEntry)
		delete(shard.records, entry.name)
		shard.lru.Remove(oldest)
		s.size--
	}
	for i := len(s.shards) - 1; i >= 0; i-- {
		s.shards[i].mu.Unlock()
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

func cloneSnapshotResult(in snapshotResult) snapshotResult {
	out := in
	out.Metrics = cloneNodeMetrics(in.Metrics)
	out.Resources = cloneResourceContext(in.Resources)
	return out
}

func cloneNodeMetrics(in nodeMetrics) nodeMetrics {
	out := in
	out.Fields = cloneDeviceFieldSet(in.Fields)
	out.Devices = cloneDeviceMetricsSlice(in.Devices)
	return out
}

func cloneDeviceMetricsSlice(in []deviceMetrics) []deviceMetrics {
	if in == nil {
		return nil
	}
	out := make([]deviceMetrics, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].Fields = cloneDeviceFieldSet(in[i].Fields)
	}
	return out
}

func cloneDeviceFieldSet(in map[deviceMetricField]struct{}) map[deviceMetricField]struct{} {
	if in == nil {
		return nil
	}
	out := make(map[deviceMetricField]struct{}, len(in))
	for field := range in {
		out[field] = struct{}{}
	}
	return out
}

func resourceContextsEqual(left, right nodeResourceContext) bool {
	return resourceMapsEqual(left.Allocatable, right.Allocatable) && resourceMapsEqual(left.Requested, right.Requested)
}

func resourceMapsEqual(left, right map[v1.ResourceName]int64) bool {
	if len(left) != len(right) {
		return false
	}
	for name, value := range left {
		if right[name] != value {
			return false
		}
	}
	return true
}
