package gpustability

import (
	"container/heap"
	"time"
)

type refreshDeadline struct {
	nodeName   string
	key        string
	generation uint64
	at         time.Time
	index      int
}

type refreshDeadlineHeap []*refreshDeadline

func (h refreshDeadlineHeap) Len() int { return len(h) }
func (h refreshDeadlineHeap) Less(i, j int) bool {
	if h[i].at.Equal(h[j].at) {
		if h[i].nodeName == h[j].nodeName {
			return h[i].generation < h[j].generation
		}
		return h[i].nodeName < h[j].nodeName
	}
	return h[i].at.Before(h[j].at)
}
func (h refreshDeadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *refreshDeadlineHeap) Push(value interface{}) {
	item := value.(*refreshDeadline)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *refreshDeadlineHeap) Pop() interface{} {
	old := *h
	last := len(old) - 1
	item := old[last]
	old[last] = nil
	item.index = -1
	*h = old[:last]
	return item
}

func (c *collector) scheduleRefreshLocked(target scrapeTarget, at time.Time) {
	current, exists := c.targets[target.NodeName]
	if !exists || current.Key != target.Key || current.Generation != target.Generation {
		return
	}
	current.NextRefresh = at
	c.targets[target.NodeName] = current
	if item := c.deadlineByNode[target.NodeName]; item != nil {
		item.key = target.Key
		item.generation = target.Generation
		item.at = at
		heap.Fix(&c.deadlines, item.index)
	} else {
		item := &refreshDeadline{nodeName: target.NodeName, key: target.Key, generation: target.Generation, at: at, index: -1}
		heap.Push(&c.deadlines, item)
		c.deadlineByNode[target.NodeName] = item
	}
	c.wakeRefreshLoop()
}

func (c *collector) removeRefreshDeadlineLocked(nodeName string) {
	item := c.deadlineByNode[nodeName]
	if item == nil {
		return
	}
	delete(c.deadlineByNode, nodeName)
	if item.index >= 0 && item.index < len(c.deadlines) {
		heap.Remove(&c.deadlines, item.index)
	}
	c.wakeRefreshLoop()
}

func (c *collector) wakeRefreshLoop() {
	select {
	case c.deadlineWake <- struct{}{}:
	default:
	}
}

func (c *collector) nextRefreshDeadline() (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.deadlines) == 0 {
		return time.Time{}, false
	}
	return c.deadlines[0].at, true
}

func (c *collector) popDueRefreshes(now time.Time) []scrapeTarget {
	c.mu.Lock()
	defer c.mu.Unlock()
	var due []scrapeTarget
	for len(c.deadlines) > 0 {
		item := c.deadlines[0]
		if item.at.After(now) {
			break
		}
		heap.Pop(&c.deadlines)
		delete(c.deadlineByNode, item.nodeName)
		target, exists := c.targets[item.nodeName]
		if !exists || target.Key != item.key || target.Generation != item.generation {
			continue
		}
		due = append(due, target)
	}
	return due
}

func (c *collector) rescheduleSuppressed(target scrapeTarget, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, exists := c.targets[target.NodeName]
	if !exists || current.Key != target.Key || current.Generation != target.Generation {
		return
	}
	if _, pending := c.pending[target.Key]; pending {
		// The in-flight worker owns the next deadline.
		return
	}
	next := now.Add(100 * time.Millisecond)
	if failure := c.failures[target.Key]; !failure.OpenUntil.IsZero() && failure.OpenUntil.After(now) {
		next = failure.OpenUntil
	} else if failure.NextTry.After(now) {
		next = failure.NextTry
	}
	c.scheduleRefreshLocked(current, next)
}
