package gpustability

import (
	"context"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

func TestPhase10DeadlineHeapSelectsOnlyDueTarget(t *testing.T) {
	cfg := testConfig(t)
	cfg.RefreshInterval = time.Hour
	cfg.CacheMaxEntries = 2048
	collector, err := newCollector(context.Background(), cfg, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return metricsResponse(iluvatarMetrics), nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer collector.Close()
	now := time.Now()
	var selected scrapeTarget
	for i := 0; i < 1000; i++ {
		node := phase2Node(t, "http://127.0.0.1:32021", "deadline-"+formatIndex(i), types.UID("uid-"+formatIndex(i)), "iluvatar.com/gpu")
		node.Annotations[AnnotationExporterPath] = "/metrics/" + node.Name
		target, _, err := collector.ensureTarget(node)
		if err != nil {
			t.Fatal(err)
		}
		if i == 437 {
			selected = target
		}
	}
	collector.mu.Lock()
	collector.scheduleRefreshLocked(selected, now.Add(-time.Millisecond))
	collector.mu.Unlock()
	due := collector.popDueRefreshes(now)
	if len(due) != 1 || due[0].Key != selected.Key {
		t.Fatalf("due targets = %+v, want only %s", due, selected.Key)
	}
	collector.mu.RLock()
	remaining := len(collector.deadlines)
	collector.mu.RUnlock()
	if remaining != 999 {
		t.Fatalf("deadline heap size = %d, want 999", remaining)
	}
}

func formatIndex(value int) string {
	const digits = "0123456789"
	buf := [4]byte{'0', '0', '0', '0'}
	for i := len(buf) - 1; i >= 0 && value > 0; i-- {
		buf[i] = digits[value%10]
		value /= 10
	}
	return string(buf[:])
}

func TestPhase10RefreshNodeUsesSingleflightQueue(t *testing.T) {
	cfg := testConfig(t)
	cfg.RefreshInterval = time.Hour
	started := make(chan struct{})
	release := make(chan struct{})
	var requests atomic.Int32
	collector, err := newCollector(context.Background(), cfg, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		if requests.Add(1) == 1 {
			close(started)
		}
		<-release
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(newStringReader(iluvatarMetrics)), Header: make(http.Header)}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer collector.Close()
	node := nodeInfoWithEndpoint("http://singleflight-node:32021/metrics").Node()
	node.Name = "singleflight-node"
	node.Status.Addresses[0].Address = "singleflight-node"
	const callers = 32
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- collector.refreshNode(context.Background(), node)
		}()
	}
	<-started
	// Do not release the exporter until every caller is actually attached to the
	// same in-flight collection. Without this barrier, the scheduler may start a
	// goroutine only after the first HTTP request has already completed, which is
	// a sequential refresh rather than a singleflight participant.
	waitFor(t, time.Second, func() bool {
		collector.mu.RLock()
		defer collector.mu.RUnlock()
		target, exists := collector.targets[node.Name]
		if !exists {
			return false
		}
		flight := collector.pending[target.Key]
		return flight != nil && len(flight.waiters) == callers
	})
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("refreshNode: %v", err)
		}
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("exporter requests = %d, want exactly one coalesced request", got)
	}
}

func newStringReader(value string) io.Reader { return &stringReader{value: value} }

type stringReader struct{ value string }

func (r *stringReader) Read(p []byte) (int, error) {
	if r.value == "" {
		return 0, io.EOF
	}
	n := copy(p, r.value)
	r.value = r.value[n:]
	return n, nil
}

func TestPhase10SnapshotLRUReadChangesEvictionWithoutFreshness(t *testing.T) {
	store := newSnapshotStore(2)
	now := time.Now().UTC()
	makeTarget := func(name string, generation uint64) scrapeTarget {
		return scrapeTarget{NodeName: name, Identity: nodeIdentity{Name: name}, Key: name, Generation: generation}
	}
	a, b, c := makeTarget("a", 1), makeTarget("b", 2), makeTarget("c", 3)
	store.transition(a, nodeResourceContext{}, now)
	store.transition(b, nodeResourceContext{}, now.Add(time.Second))
	observed := now.Add(-time.Minute)
	metrics := nodeMetrics{Profile: "iluvatar", FetchedAt: observed, ValidUntil: now.Add(time.Hour)}
	if !store.publish(a, metrics, observed, now.Add(time.Hour)) {
		t.Fatal("publish a failed")
	}
	before := store.lookup(a, now.Add(2*time.Second))
	if !before.ObservedAt.Equal(observed) {
		t.Fatalf("lookup changed freshness: observedAt=%v want=%v", before.ObservedAt, observed)
	}
	store.transition(c, nodeResourceContext{}, now.Add(3*time.Second))
	if !store.contains("a") || !store.contains("c") || store.contains("b") {
		t.Fatalf("LRU eviction mismatch: a=%v b=%v c=%v", store.contains("a"), store.contains("b"), store.contains("c"))
	}
	after := store.lookup(a, now.Add(4*time.Second))
	if !after.ObservedAt.Equal(observed) || !after.ValidUntil.Equal(before.ValidUntil) {
		t.Fatalf("LRU access altered freshness: before=%+v after=%+v", before, after)
	}
}

func TestPhase10DuplicateTargetUpdateKeepsGenerationAndDeadline(t *testing.T) {
	cfg := testConfig(t)
	cfg.RefreshInterval = time.Hour
	collector, err := newCollector(context.Background(), cfg, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return metricsResponse(iluvatarMetrics), nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer collector.Close()
	node := nodeInfoWithEndpoint("http://duplicate-node:32021/metrics").Node()
	node.Name = "duplicate-node"
	node.ResourceVersion = "7"
	node.Status.Addresses[0].Address = "duplicate-node"
	first, changed, err := collector.ensureTarget(node)
	if err != nil || !changed {
		t.Fatalf("first target changed=%v err=%v", changed, err)
	}
	second, changed, err := collector.ensureTarget(node)
	if err != nil || changed {
		t.Fatalf("duplicate target changed=%v err=%v", changed, err)
	}
	if first.Generation != second.Generation || first.Key != second.Key {
		t.Fatalf("duplicate update created generation: first=%+v second=%+v", first, second)
	}
	collector.mu.RLock()
	deadlines := len(collector.deadlines)
	owners := len(collector.endpointOwners)
	collector.mu.RUnlock()
	if deadlines != 1 || owners != 1 {
		t.Fatalf("duplicate update state deadlines=%d endpointOwners=%d, want 1/1", deadlines, owners)
	}
}
