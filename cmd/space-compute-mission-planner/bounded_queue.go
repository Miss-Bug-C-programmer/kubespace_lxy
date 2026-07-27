package main

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	spaceplanner "github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"
)

type boundedRateLimitingQueue struct {
	workqueue.RateLimitingInterface
	name       string
	maxPending int
	observer   spaceplanner.PrometheusObserver
	now        func() time.Time

	mu        sync.Mutex
	pending   map[interface{}]time.Time
	oldest    time.Time
	saturated chan struct{}
	once      sync.Once
}

func newBoundedRateLimitingQueue(name string, maxPending int, observer spaceplanner.PrometheusObserver) *boundedRateLimitingQueue {
	if maxPending < 1 {
		panic("maxPending must be positive")
	}
	q := &boundedRateLimitingQueue{
		RateLimitingInterface: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), name),
		name:                  name,
		maxPending:            maxPending,
		observer:              observer,
		now:                   time.Now,
		pending:               make(map[interface{}]time.Time, maxPending),
		saturated:             make(chan struct{}),
	}
	observer.QueueCapacity(name, maxPending)
	return q
}

func (q *boundedRateLimitingQueue) Add(item interface{}) {
	if q.track(item) {
		q.RateLimitingInterface.Add(item)
	}
}
func (q *boundedRateLimitingQueue) AddAfter(item interface{}, duration time.Duration) {
	if q.track(item) {
		q.RateLimitingInterface.AddAfter(item, duration)
	}
}
func (q *boundedRateLimitingQueue) AddRateLimited(item interface{}) {
	if q.track(item) {
		q.RateLimitingInterface.AddRateLimited(item)
	}
}
func (q *boundedRateLimitingQueue) Get() (item interface{}, shutdown bool) {
	item, shutdown = q.RateLimitingInterface.Get()
	if !shutdown {
		q.mu.Lock()
		removedAt, existed := q.pending[item]
		delete(q.pending, item)
		if existed && removedAt.Equal(q.oldest) {
			q.recomputeOldestLocked()
		}
		q.observeLocked()
		q.mu.Unlock()
	}
	return item, shutdown
}
func (q *boundedRateLimitingQueue) Saturated() <-chan struct{} { return q.saturated }
func (q *boundedRateLimitingQueue) PendingUnique() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}
func (q *boundedRateLimitingQueue) Observe() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.observeLocked()
}

func (q *boundedRateLimitingQueue) track(item interface{}) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, exists := q.pending[item]; exists {
		q.observeLocked()
		return true
	}
	if len(q.pending) >= q.maxPending {
		q.observer.QueueSaturated(q.name)
		q.observer.QueueDepth(q.name, len(q.pending))
		q.once.Do(func() { close(q.saturated) })
		klog.ErrorS(fmt.Errorf("pending unique key capacity reached"), "controller queue saturated; stopping reconciliation and failing readiness without silently dropping work", "queue", q.name, "capacity", q.maxPending)
		return false
	}
	when := q.now()
	q.pending[item] = when
	if q.oldest.IsZero() || when.Before(q.oldest) {
		q.oldest = when
	}
	q.observeLocked()
	return true
}
func (q *boundedRateLimitingQueue) observeLocked() {
	q.observer.QueueDepth(q.name, len(q.pending))
	if len(q.pending) == 0 || q.oldest.IsZero() {
		q.observer.QueueOldestAge(q.name, 0)
		return
	}
	q.observer.QueueOldestAge(q.name, q.now().Sub(q.oldest))
}

// recomputeOldestLocked is called only by workers after dequeueing the prior
// oldest item. Enqueue/event-handler paths remain O(1).
func (q *boundedRateLimitingQueue) recomputeOldestLocked() {
	q.oldest = time.Time{}
	for _, when := range q.pending {
		if q.oldest.IsZero() || when.Before(q.oldest) {
			q.oldest = when
		}
	}
}
