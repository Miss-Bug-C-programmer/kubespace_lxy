package gpustability

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/idna"
	v1 "k8s.io/api/core/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type snapshotState string

const (
	snapshotFresh   snapshotState = "fresh"
	snapshotStale   snapshotState = "stale"
	snapshotMissing snapshotState = "missing"
	snapshotFailed  snapshotState = "failed"
)

type scrapeTarget struct {
	NodeName       string
	Identity       nodeIdentity
	Endpoint       string
	Profile        string
	ProfileVersion uint64
	Generation     uint64
	Key            string
	SeenAt         time.Time
	NextRefresh    time.Time
}

type failureState struct {
	Count     int
	LastError string
	NextTry   time.Time
	OpenUntil time.Time
}

type snapshotResult struct {
	State            snapshotState
	Metrics          nodeMetrics
	Resources        nodeResourceContext
	ObservedAt       time.Time
	ValidUntil       time.Time
	Profile          string
	TargetGeneration uint64
	Confidence       snapshotConfidence
	Reason           string
}

type collector struct {
	config Config
	client *http.Client
	now    func() time.Time

	ctx       context.Context
	cancel    context.CancelFunc
	queue     chan scrapeTarget
	wg        sync.WaitGroup
	closeOnce sync.Once
	registry  *profileRegistry
	store     *snapshotStore

	mu             sync.RWMutex
	targets        map[string]scrapeTarget
	nodes          map[string]*v1.Node
	pending        map[string]struct{}
	failures       map[string]failureState
	active         map[string]context.CancelFunc
	nextGeneration uint64
	listenerMu     sync.RWMutex
	snapshotReady  func(string, uint64)
}

func (c *collector) setSnapshotListener(listener func(string, uint64)) {
	c.listenerMu.Lock()
	c.snapshotReady = listener
	c.listenerMu.Unlock()
}

func (c *collector) notifySnapshotReady(nodeName string, generation uint64) {
	c.listenerMu.RLock()
	listener := c.snapshotReady
	c.listenerMu.RUnlock()
	if listener != nil {
		listener(nodeName, generation)
	}
}

func newCollector(ctx context.Context, cfg Config, client *http.Client) (*collector, error) {
	registry, err := newProfileRegistry(cfg)
	if err != nil {
		return nil, fmt.Errorf("create metric profile registry: %w", err)
	}
	if client == nil {
		var err error
		client, err = newSecureHTTPClient(cfg)
		if err != nil {
			return nil, err
		}
	} else {
		clone := *client
		client = &clone
	}
	client.Timeout = cfg.Timeout
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return errors.New("exporter redirects are disabled")
	}

	collectorCtx, cancel := context.WithCancel(ctx)
	c := &collector{
		config: cfg, client: client, now: time.Now,
		ctx: collectorCtx, cancel: cancel, queue: make(chan scrapeTarget, cfg.QueueSize),
		registry: registry, store: newSnapshotStore(cfg.CacheMaxEntries),
		targets: map[string]scrapeTarget{}, nodes: map[string]*v1.Node{},
		pending: map[string]struct{}{}, failures: map[string]failureState{}, active: map[string]context.CancelFunc{},
	}
	for i := 0; i < cfg.Workers; i++ {
		c.wg.Add(1)
		go c.worker()
	}
	c.wg.Add(1)
	go c.refreshLoop()
	if cfg.MetricProfilesFile != "" {
		c.wg.Add(1)
		go c.profileReloadLoop()
	}
	return c, nil
}

func newSecureHTTPClient(cfg Config) (*http.Client, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: strings.TrimSpace(cfg.ServerName)}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read exporter CA file: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("exporter CA file contains no valid certificates")
		}
		tlsConfig.RootCAs = roots
	}
	if cfg.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load exporter client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return &http.Client{Transport: transport, Timeout: cfg.Timeout}, nil
}

func (c *collector) Close() {
	c.closeOnce.Do(func() {
		c.cancel()
		c.wg.Wait()
		c.setSnapshotListener(nil)
		c.mu.Lock()
		c.targets = map[string]scrapeTarget{}
		c.nodes = map[string]*v1.Node{}
		c.pending = map[string]struct{}{}
		c.failures = map[string]failureState{}
		c.active = map[string]context.CancelFunc{}
		c.mu.Unlock()
		c.store.clear()
		c.client.CloseIdleConnections()
		observeTargetCount(0)
		observeQueueDepth(0)
		observeBackoffTargets(0)
		observeCircuitTargets(0)
	})
}

func (c *collector) registerNodeInformer(handle framework.Handle) error {
	if handle == nil || handle.SharedInformerFactory() == nil {
		return nil
	}
	informer := handle.SharedInformerFactory().Core().V1().Nodes().Informer()
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if node, ok := obj.(*v1.Node); ok {
				c.observeNode(node)
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			if node, ok := newObj.(*v1.Node); ok {
				c.observeNode(node)
			}
		},
		DeleteFunc: func(obj interface{}) {
			switch value := obj.(type) {
			case *v1.Node:
				c.deleteNode(identityForNode(value))
			case cache.DeletedFinalStateUnknown:
				if node, ok := value.Obj.(*v1.Node); ok {
					c.deleteNode(identityForNode(node))
				}
			}
		},
	})
	return err
}

func (c *collector) observeNode(node *v1.Node) {
	if node == nil {
		return
	}
	// Background collection is limited to Nodes that advertise a configured
	// physical resource or explicit exporter metadata. This keeps ordinary
	// agents and control-plane Nodes out of the scrape/cache budget. An
	// annotation-only workload can still resolve a target on demand.
	if !c.isBackgroundTarget(node) {
		c.deleteNode(identityForNode(node))
		return
	}
	target, changed, err := c.ensureTarget(node)
	if err != nil {
		c.recordTargetError(node.Name, err)
		return
	}
	if changed || c.store.lookup(target, c.now()).State != snapshotFresh {
		c.enqueue(target)
	}
}

func (c *collector) isBackgroundTarget(node *v1.Node) bool {
	if node == nil {
		return false
	}
	for resourceName := range c.config.ResourceMappings {
		if quantity, ok := node.Status.Allocatable[resourceName]; ok && quantity.Sign() > 0 {
			return true
		}
		if quantity, ok := node.Status.Capacity[resourceName]; ok && quantity.Sign() > 0 {
			return true
		}
	}
	for _, key := range []string{AnnotationExporterPort, AnnotationExporterPath, AnnotationExporterScheme, AnnotationExporterProfile} {
		if strings.TrimSpace(node.Annotations[key]) != "" {
			return true
		}
	}
	return false
}

func (c *collector) deleteNode(identity nodeIdentity) {
	c.mu.Lock()
	target, ok := c.targets[identity.Name]
	if !ok || (identity.UID != "" && target.Identity.UID != identity.UID) {
		c.mu.Unlock()
		return
	}
	if ok {
		if cancel := c.active[target.Key]; cancel != nil {
			cancel()
			delete(c.active, target.Key)
		}
		delete(c.failures, target.Key)
		delete(c.pending, target.Key)
	}
	delete(c.targets, identity.Name)
	delete(c.nodes, identity.Name)
	observeTargetCount(len(c.targets))
	c.mu.Unlock()
	c.store.delete(identity)
}

func (c *collector) recordTargetError(nodeName string, err error) {
	c.mu.Lock()
	if old, ok := c.targets[nodeName]; ok {
		if cancel := c.active[old.Key]; cancel != nil {
			cancel()
			delete(c.active, old.Key)
		}
		delete(c.failures, old.Key)
		delete(c.pending, old.Key)
	}
	delete(c.targets, nodeName)
	delete(c.nodes, nodeName)
	observeTargetCount(len(c.targets))
	c.mu.Unlock()
	c.store.remove(nodeName)
	observeCollectorFailure("invalid_target")
}

func (c *collector) snapshotForNode(node *v1.Node) snapshotResult {
	if node == nil {
		return snapshotResult{State: snapshotMissing, Confidence: confidenceMissing, Reason: "node information is unavailable"}
	}
	target, _, err := c.ensureTarget(node)
	if err != nil {
		c.recordTargetError(node.Name, err)
		return snapshotResult{State: snapshotFailed, Confidence: confidenceFailed, Reason: err.Error()}
	}
	now := c.now()
	result := c.store.lookup(target, now)
	if result.State != snapshotFresh {
		c.enqueue(target)
	}
	if result.State == snapshotFresh {
		observeSnapshotAge(now.Sub(result.ObservedAt))
	}
	observeSnapshotRead(result.State)
	return result
}

func (c *collector) snapshotForNodeInfo(nodeInfo *framework.NodeInfo) snapshotResult {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return snapshotResult{State: snapshotMissing, Confidence: confidenceMissing, Reason: "node information is unavailable"}
	}
	result := c.snapshotForNode(nodeInfo.Node())
	c.mu.RLock()
	target, exists := c.targets[nodeInfo.Node().Name]
	c.mu.RUnlock()
	if !exists {
		return result
	}
	resources := resourceContextForNodeInfo(nodeInfo, c.config.ResourceMappings)
	c.store.updateResources(target, resources)
	result.Resources = resources
	return result
}

func (c *collector) enqueue(target scrapeTarget) bool {
	now := c.now()
	c.mu.Lock()
	if _, exists := c.pending[target.Key]; exists {
		c.mu.Unlock()
		observeRefreshSuppressed("coalesced")
		return false
	}
	if failure := c.failures[target.Key]; now.Before(failure.NextTry) || now.Before(failure.OpenUntil) {
		c.mu.Unlock()
		observeRefreshSuppressed("backoff")
		return false
	}
	c.pending[target.Key] = struct{}{}
	c.mu.Unlock()

	select {
	case c.queue <- target:
		observeQueueDepth(len(c.queue))
		return true
	default:
		c.mu.Lock()
		delete(c.pending, target.Key)
		c.mu.Unlock()
		observeRefreshSuppressed("queue_full")
		return false
	}
}

func (c *collector) worker() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case target := <-c.queue:
			observeQueueDepth(len(c.queue))
			observeWorkerActive(1)
			start := time.Now()
			requestContext, finish, isCurrent := c.beginCollection(target)
			var err error
			if !isCurrent {
				err = fmt.Errorf("discarded scrape for obsolete target")
			} else {
				err = c.collectTarget(requestContext, target)
				finish()
			}
			observeWorkerActive(-1)
			observeCollection(time.Since(start), err)
			if err != nil {
				observeCollectorFailure(collectionFailureReason(err))
			}
			c.mu.Lock()
			delete(c.pending, target.Key)
			current, stillCurrent := c.targets[target.NodeName]
			if err != nil && stillCurrent && current.Key == target.Key {
				c.recordFailureLocked(target, err)
				c.store.recordFailure(target, sanitizeCollectionError(err), c.now())
			} else if err == nil && stillCurrent && current.Key == target.Key {
				delete(c.failures, target.Key)
			}
			c.observeFailureStateLocked()
			c.mu.Unlock()
		}
	}
}

func (c *collector) beginCollection(target scrapeTarget) (context.Context, func(), bool) {
	c.mu.Lock()
	current, exists := c.targets[target.NodeName]
	if !exists || current.Key != target.Key || current.Generation != target.Generation || current.Identity != target.Identity || c.registry.snapshot().Version != target.ProfileVersion {
		c.mu.Unlock()
		return nil, func() {}, false
	}
	requestContext, cancel := context.WithCancel(c.ctx)
	c.active[target.Key] = cancel
	c.mu.Unlock()
	return requestContext, func() {
		c.mu.Lock()
		if _, exists := c.active[target.Key]; exists {
			delete(c.active, target.Key)
		}
		c.mu.Unlock()
		cancel()
	}, true
}

func (c *collector) refreshLoop() {
	defer c.wg.Done()
	scanInterval := c.config.RefreshInterval / 10
	if scanInterval > time.Second {
		scanInterval = time.Second
	}
	if scanInterval < 10*time.Millisecond {
		scanInterval = 10 * time.Millisecond
	}
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case now := <-ticker.C:
			c.scheduleDue(now)
		}
	}
}

func (c *collector) scheduleDue(now time.Time) {
	c.mu.Lock()
	targets := make([]scrapeTarget, 0)
	for name, target := range c.targets {
		if target.NextRefresh.After(now) {
			continue
		}
		target.NextRefresh = now.Add(jitteredInterval(target.Key, c.config.RefreshInterval, c.config.JitterFraction))
		c.targets[name] = target
		targets = append(targets, target)
	}
	c.mu.Unlock()
	sort.Slice(targets, func(i, j int) bool { return targets[i].Key < targets[j].Key })
	for _, target := range targets {
		c.enqueue(target)
	}
}

func (c *collector) profileReloadLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.config.ProfileReloadInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			_, _ = c.reloadProfiles()
		}
	}
}

func (c *collector) reloadProfiles() (bool, error) {
	changed, err := c.registry.reload()
	observeProfileReload(changed, err)
	if err != nil || !changed {
		return changed, err
	}
	c.mu.RLock()
	nodes := make([]*v1.Node, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node.DeepCopy())
	}
	c.mu.RUnlock()
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	for _, node := range nodes {
		c.observeNode(node)
	}
	return true, nil
}

func jitteredInterval(key string, interval time.Duration, fraction float64) time.Duration {
	if fraction <= 0 {
		return interval
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key))
	unit := float64(hash.Sum64()%1_000_001) / 1_000_000
	factor := 1 - fraction + 2*fraction*unit
	return time.Duration(float64(interval) * factor)
}

func (c *collector) refreshNode(ctx context.Context, node *v1.Node) error {
	target, _, err := c.ensureTarget(node)
	if err != nil {
		observeCollectorFailure("invalid_target")
		return err
	}
	start := time.Now()
	err = c.collectTarget(ctx, target)
	observeCollection(time.Since(start), err)
	if err != nil {
		observeCollectorFailure(collectionFailureReason(err))
	}
	c.mu.Lock()
	current, stillCurrent := c.targets[target.NodeName]
	if err != nil && stillCurrent && current.Key == target.Key {
		c.recordFailureLocked(target, err)
		c.store.recordFailure(target, sanitizeCollectionError(err), c.now())
	} else if err == nil && stillCurrent && current.Key == target.Key {
		delete(c.failures, target.Key)
	}
	c.observeFailureStateLocked()
	c.mu.Unlock()
	return err
}

func (c *collector) collectTarget(ctx context.Context, target scrapeTarget) error {
	reqCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, target.Endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("exporter returned HTTP status %d", resp.StatusCode)
	}
	if resp.ContentLength > c.config.MaxResponseBytes {
		return fmt.Errorf("exporter response exceeds %d bytes", c.config.MaxResponseBytes)
	}
	limited := io.LimitReader(resp.Body, c.config.MaxResponseBytes+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("read exporter response: %w", err)
	}
	if int64(len(raw)) > c.config.MaxResponseBytes {
		return fmt.Errorf("exporter response exceeds %d bytes", c.config.MaxResponseBytes)
	}
	parseStart := time.Now()
	metrics, err := c.registry.parseVersion(strings.NewReader(string(raw)), target.Profile, target.ProfileVersion, parserLimits{
		MaxMetricFamilies: c.config.MaxMetricFamilies, MaxSamples: c.config.MaxSamples,
		MaxLabelsPerSample: c.config.MaxLabelsPerSample, MaxDevices: c.config.MaxDevicesPerNode,
	})
	observeParse(time.Since(parseStart), err)
	if err != nil {
		return fmt.Errorf("parse exporter response: %w", err)
	}
	observedAt := c.now()
	metrics.Endpoint = target.Endpoint
	metrics.FetchedAt = observedAt
	metrics.ValidUntil = observedAt.Add(c.config.SnapshotTTL)

	c.mu.RLock()
	current, exists := c.targets[target.NodeName]
	c.mu.RUnlock()
	if !exists || current.Key != target.Key || current.Generation != target.Generation || current.Identity != target.Identity || c.registry.snapshot().Version != target.ProfileVersion {
		observeDiscardedGeneration()
		return fmt.Errorf("discarded snapshot for obsolete target")
	}
	if !c.store.publish(target, metrics, observedAt, metrics.ValidUntil) {
		observeDiscardedGeneration()
		return fmt.Errorf("discarded snapshot for obsolete target generation")
	}
	c.notifySnapshotReady(target.NodeName, target.Generation)
	return nil
}

func (c *collector) recordFailureLocked(target scrapeTarget, err error) {
	failure := c.failures[target.Key]
	failure.Count++
	failure.LastError = sanitizeCollectionError(err)
	delay := c.config.BackoffBase
	for i := 1; i < failure.Count && delay < c.config.BackoffMax; i++ {
		delay *= 2
		if delay > c.config.BackoffMax {
			delay = c.config.BackoffMax
		}
	}
	failure.NextTry = c.now().Add(delay)
	if failure.Count >= c.config.CircuitFailures {
		failure.OpenUntil = c.now().Add(c.config.CircuitOpenDuration)
	}
	c.failures[target.Key] = failure
}

func (c *collector) observeFailureStateLocked() {
	backoff := 0
	circuit := 0
	now := c.now()
	for _, failure := range c.failures {
		if now.Before(failure.OpenUntil) {
			circuit++
		} else if now.Before(failure.NextTry) {
			backoff++
		}
	}
	observeBackoffTargets(backoff)
	observeCircuitTargets(circuit)
}

func sanitizeCollectionError(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if len(message) > 256 {
		message = message[:256]
	}
	return message
}

func collectionFailureReason(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case strings.Contains(err.Error(), "redirects are disabled"):
		return "redirect"
	case strings.Contains(err.Error(), "exceeds"):
		return "oversized"
	case strings.Contains(err.Error(), "parse exporter response"):
		return "invalid_metrics"
	case strings.Contains(err.Error(), "HTTP status"):
		return "http_status"
	case strings.Contains(err.Error(), "obsolete target"):
		return "obsolete_target"
	default:
		return "transport"
	}
}

func (c *collector) pruneLocked(protectedNode string) {
	if len(c.targets) <= c.config.CacheMaxEntries {
		return
	}
	type candidate struct {
		name string
		time time.Time
	}
	candidates := make([]candidate, 0, len(c.targets))
	for name, target := range c.targets {
		if name == protectedNode {
			continue
		}
		candidates = append(candidates, candidate{name: name, time: target.SeenAt})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].time.Equal(candidates[j].time) {
			return candidates[i].name < candidates[j].name
		}
		return candidates[i].time.Before(candidates[j].time)
	})
	remove := len(c.targets) - c.config.CacheMaxEntries
	for _, item := range candidates[:remove] {
		target := c.targets[item.name]
		if cancel := c.active[target.Key]; cancel != nil {
			cancel()
			delete(c.active, target.Key)
		}
		delete(c.targets, item.name)
		delete(c.nodes, item.name)
		delete(c.failures, target.Key)
		delete(c.pending, target.Key)
		c.store.remove(item.name)
	}
}

func (c *collector) targetForNode(node *v1.Node) (scrapeTarget, error) {
	target, _, err := c.ensureTarget(node)
	return target, err
}

func (c *collector) ensureTarget(node *v1.Node) (scrapeTarget, bool, error) {
	resolved, err := c.resolveTarget(node)
	if err != nil {
		return scrapeTarget{}, false, err
	}
	now := c.now()
	c.mu.Lock()
	old, exists := c.targets[node.Name]
	if exists && sameResolvedTarget(old, resolved) {
		old.SeenAt = now
		c.targets[node.Name] = old
		c.nodes[node.Name] = node.DeepCopy()
		resources := resourceContextForNode(node, c.config.ResourceMappings)
		c.mu.Unlock()
		c.store.updateResources(old, resources)
		return old, false, nil
	}
	for otherName, target := range c.targets {
		if otherName != node.Name && target.Endpoint == resolved.Endpoint {
			c.mu.Unlock()
			return scrapeTarget{}, false, fmt.Errorf("node %q exporter endpoint conflicts with node %q", node.Name, otherName)
		}
	}
	c.nextGeneration++
	resolved.Generation = c.nextGeneration
	resolved.SeenAt = now
	resolved.NextRefresh = now.Add(jitteredInterval(resolved.fingerprint(), c.config.RefreshInterval, c.config.JitterFraction))
	resolved.Key = fmt.Sprintf("%s|%s|%d|%s", resolved.Identity.Name, resolved.Identity.UID, resolved.Generation, resolved.fingerprint())
	if exists {
		if cancel := c.active[old.Key]; cancel != nil {
			cancel()
			delete(c.active, old.Key)
		}
		delete(c.failures, old.Key)
		delete(c.pending, old.Key)
	}
	c.targets[node.Name] = resolved
	c.nodes[node.Name] = node.DeepCopy()
	c.pruneLocked(node.Name)
	observeTargetCount(len(c.targets))
	c.observeFailureStateLocked()
	c.mu.Unlock()
	c.store.transition(resolved, resourceContextForNode(node, c.config.ResourceMappings), now)
	return resolved, true, nil
}

func sameResolvedTarget(left, right scrapeTarget) bool {
	return left.Identity == right.Identity && left.Endpoint == right.Endpoint && left.Profile == right.Profile && left.ProfileVersion == right.ProfileVersion
}

func (t scrapeTarget) fingerprint() string {
	return t.Endpoint + "|" + t.Profile + "|" + strconv.FormatUint(t.ProfileVersion, 10)
}

func (c *collector) resolveTarget(node *v1.Node) (scrapeTarget, error) {
	if node == nil || strings.TrimSpace(node.Name) == "" {
		return scrapeTarget{}, fmt.Errorf("node identity is unavailable")
	}
	host, err := nodeAddress(node, c.config.AddressTypes, c.config.PreferredIPFamily)
	if err != nil {
		return scrapeTarget{}, err
	}
	port := strings.TrimSpace(node.Annotations[AnnotationExporterPort])
	if port == "" {
		port = c.config.ExporterPort
	}
	if err := validatePort(port); err != nil {
		return scrapeTarget{}, fmt.Errorf("node %q exporter port: %w", node.Name, err)
	}
	path := strings.TrimSpace(node.Annotations[AnnotationExporterPath])
	if path == "" {
		path = c.config.ExporterPath
	}
	if err := validateMetricsPath(path); err != nil {
		return scrapeTarget{}, fmt.Errorf("node %q exporter path: %w", node.Name, err)
	}
	scheme := strings.ToLower(strings.TrimSpace(node.Annotations[AnnotationExporterScheme]))
	if scheme == "" {
		scheme = c.config.Scheme
	}
	if scheme != "https" && scheme != "http" {
		return scrapeTarget{}, fmt.Errorf("node %q exporter scheme must be http or https", node.Name)
	}
	if scheme == "http" && !c.config.AllowInsecureHTTP {
		return scrapeTarget{}, fmt.Errorf("node %q requests insecure HTTP but allowInsecureHTTP is false", node.Name)
	}
	profile := strings.ToLower(strings.TrimSpace(node.Annotations[AnnotationExporterProfile]))
	if profile == "" {
		profile = c.config.MetricProfile
	}
	registrySnapshot := c.registry.snapshot()
	if profile != defaultMetricProfile && !hasMetricProfile(registrySnapshot.Profiles, profile) {
		return scrapeTarget{}, fmt.Errorf("node %q requests unknown exporter profile %q", node.Name, profile)
	}
	endpoint := (&url.URL{Scheme: scheme, Host: net.JoinHostPort(host, port), Path: path}).String()
	return scrapeTarget{
		NodeName: node.Name, Identity: identityForNode(node), Endpoint: endpoint,
		Profile: profile, ProfileVersion: registrySnapshot.Version,
	}, nil
}

func validatePort(raw string) error {
	port, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("must be an integer between 1 and 65535")
	}
	return nil
}

func validateMetricsPath(raw string) error {
	path := strings.TrimSpace(raw)
	if path == "" || !strings.HasPrefix(path, "/") {
		return fmt.Errorf("must be an absolute path")
	}
	parsed, err := url.Parse(path)
	if err != nil || parsed.IsAbs() || parsed.Host != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("must not contain a host, query, or fragment")
	}
	for _, segment := range strings.Split(parsed.Path, "/") {
		if segment == ".." {
			return fmt.Errorf("must not contain parent-directory traversal")
		}
	}
	return nil
}

func nodeAddress(node *v1.Node, addressTypes []v1.NodeAddressType, preferredFamily string) (string, error) {
	for _, addressType := range addressTypes {
		seen := map[string]struct{}{}
		candidates := make([]string, 0, 2)
		for _, address := range node.Status.Addresses {
			value := strings.TrimSpace(address.Address)
			if address.Type != addressType || value == "" {
				continue
			}
			if err := validateNodeHost(value); err != nil {
				return "", fmt.Errorf("node %q has invalid %s: %w", node.Name, addressType, err)
			}
			if _, duplicate := seen[value]; !duplicate {
				seen[value] = struct{}{}
				candidates = append(candidates, value)
			}
		}
		if len(candidates) == 0 {
			continue
		}
		sort.Strings(candidates)
		preferred := make([]string, 0, len(candidates))
		fallback := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			family := ipFamily(candidate)
			if preferredFamily == "any" || family == preferredFamily {
				preferred = append(preferred, candidate)
			} else {
				fallback = append(fallback, candidate)
			}
		}
		if preferredFamily == "any" {
			if len(preferred) == 1 {
				return preferred[0], nil
			}
			return "", fmt.Errorf("node %q has ambiguous %s addresses: %s", node.Name, addressType, strings.Join(preferred, ", "))
		}
		if len(preferred) == 1 {
			return preferred[0], nil
		}
		if len(preferred) > 1 {
			return "", fmt.Errorf("node %q has ambiguous %s %s addresses: %s", node.Name, addressType, preferredFamily, strings.Join(preferred, ", "))
		}
		if len(fallback) == 1 {
			return fallback[0], nil
		}
		return "", fmt.Errorf("node %q has ambiguous fallback %s addresses: %s", node.Name, addressType, strings.Join(fallback, ", "))
	}
	return "", fmt.Errorf("node %q has no approved exporter address", node.Name)
}

func ipFamily(host string) string {
	ip := net.ParseIP(host)
	if ip == nil {
		return "hostname"
	}
	if ip.To4() != nil {
		return "ipv4"
	}
	return "ipv6"
}

func validateNodeHost(host string) error {
	if net.ParseIP(host) != nil {
		return nil
	}
	ascii, err := idna.Lookup.ToASCII(host)
	if err != nil {
		return fmt.Errorf("invalid IDNA hostname: %w", err)
	}
	// A DNS-1123 hostname may contain an ASCII Punycode label, but an
	// ASCII-only decoded label is never a valid IDNA representation. The
	// explicit round-trip check keeps old x/net versions from accepting
	// labels such as xn--example-, which were previously normalized to an
	// unrelated ASCII hostname and could bypass host policy checks.
	if strings.Contains(strings.ToLower(host), "xn--") {
		unicode, err := idna.Lookup.ToUnicode(ascii)
		if err != nil || isASCIIOnly(unicode) {
			return fmt.Errorf("invalid Punycode hostname")
		}
	}
	if errs := utilvalidation.IsDNS1123Subdomain(host); len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("must be an IP address or DNS-1123 name")
}

func isASCIIOnly(value string) bool {
	for _, r := range value {
		if r > 0x7f {
			return false
		}
	}
	return true
}

func resourceContextForNode(node *v1.Node, mappings map[v1.ResourceName]resourceMapping) nodeResourceContext {
	context := nodeResourceContext{Allocatable: map[v1.ResourceName]int64{}, Requested: map[v1.ResourceName]int64{}}
	if node == nil {
		return context
	}
	for resourceName := range mappings {
		quantity, exists := node.Status.Allocatable[resourceName]
		if !exists {
			continue
		}
		if value, exact := quantity.AsInt64(); exact {
			context.Allocatable[resourceName] = value
		}
	}
	return context
}

func resourceContextForNodeInfo(nodeInfo *framework.NodeInfo, mappings map[v1.ResourceName]resourceMapping) nodeResourceContext {
	if nodeInfo == nil {
		return nodeResourceContext{Allocatable: map[v1.ResourceName]int64{}, Requested: map[v1.ResourceName]int64{}}
	}
	context := resourceContextForNode(nodeInfo.Node(), mappings)
	if nodeInfo.Requested == nil {
		return context
	}
	for resourceName := range mappings {
		if value, exists := nodeInfo.Requested.ScalarResources[resourceName]; exists {
			context.Requested[resourceName] = value
		}
	}
	return context
}
