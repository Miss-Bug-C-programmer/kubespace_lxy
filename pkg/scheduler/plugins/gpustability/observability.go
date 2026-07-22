package gpustability

import (
	"sync"
	"time"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

var (
	registerMetricsOnce sync.Once

	collectionDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: "scheduler_gpustability", Name: "collection_duration_seconds",
			Help: "Exporter collection latency by bounded result category.", StabilityLevel: metrics.ALPHA,
			Buckets: metrics.ExponentialBuckets(0.005, 2, 14),
		}, []string{"result"},
	)
	collectionFailures = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem: "scheduler_gpustability", Name: "collection_failures_total",
			Help: "Exporter collection failures by bounded reason category.", StabilityLevel: metrics.ALPHA,
		}, []string{"reason"},
	)
	targetCount = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem: "scheduler_gpustability", Name: "targets",
			Help: "Current number of dynamically discovered exporter targets.", StabilityLevel: metrics.ALPHA,
		},
	)
	queueDepth = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem: "scheduler_gpustability", Name: "scrape_queue_depth",
			Help: "Current bounded exporter scrape queue depth.", StabilityLevel: metrics.ALPHA,
		},
	)
	workerUtilization = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem: "scheduler_gpustability", Name: "active_workers",
			Help: "Current number of workers collecting exporter telemetry.", StabilityLevel: metrics.ALPHA,
		},
	)
	parseDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: "scheduler_gpustability", Name: "parse_duration_seconds",
			Help: "Bounded exporter parsing latency by result.", StabilityLevel: metrics.ALPHA,
			Buckets: metrics.ExponentialBuckets(0.0005, 2, 14),
		}, []string{"result"},
	)
	profileReloads = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem: "scheduler_gpustability", Name: "profile_reloads_total",
			Help: "Declarative profile reload attempts by bounded result.", StabilityLevel: metrics.ALPHA,
		}, []string{"result"},
	)
	profileReloadError = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem: "scheduler_gpustability", Name: "profile_reload_error",
			Help: "Whether the most recent declarative profile reload failed while last-known-good remained active.", StabilityLevel: metrics.ALPHA,
		},
	)
	discardedGenerations = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem: "scheduler_gpustability", Name: "discarded_stale_generation_responses_total",
			Help: "Exporter responses discarded because the node target generation changed.", StabilityLevel: metrics.ALPHA,
		},
	)
	backoffTargets = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem: "scheduler_gpustability", Name: "targets_in_backoff",
			Help: "Current number of exporter targets in retry backoff.", StabilityLevel: metrics.ALPHA,
		},
	)
	circuitTargets = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem: "scheduler_gpustability", Name: "targets_with_open_circuit",
			Help: "Current number of exporter targets with an open collection circuit.", StabilityLevel: metrics.ALPHA,
		},
	)
	refreshSuppressed = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem: "scheduler_gpustability", Name: "refresh_suppressed_total",
			Help: "Refresh requests suppressed by coalescing, backoff, or queue bounds.", StabilityLevel: metrics.ALPHA,
		}, []string{"reason"},
	)
	snapshotReads = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem: "scheduler_gpustability", Name: "snapshot_reads_total",
			Help: "Scheduler snapshot reads by freshness state.", StabilityLevel: metrics.ALPHA,
		}, []string{"state"},
	)
	snapshotAge = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem: "scheduler_gpustability", Name: "snapshot_age_seconds",
			Help: "Age of fresh telemetry snapshots observed by scheduler callbacks.", StabilityLevel: metrics.ALPHA,
			Buckets: metrics.ExponentialBuckets(0.1, 2, 15),
		},
	)
	filterDecisions = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem: "scheduler_gpustability", Name: "filter_decisions_total",
			Help: "Filter decisions by state policy and bounded reason.", StabilityLevel: metrics.ALPHA,
		}, []string{"policy", "reason"},
	)
	scoreEvaluations = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem: "scheduler_gpustability", Name: "score_evaluations_total",
			Help: "Score evaluations by state policy and snapshot state.", StabilityLevel: metrics.ALPHA,
		}, []string{"policy", "state", "reason"},
	)
	scoreValues = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: "scheduler_gpustability", Name: "score",
			Help: "Final deterministic score by state policy.", StabilityLevel: metrics.ALPHA,
			Buckets: []float64{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
		}, []string{"policy"},
	)
	blockedPodCount = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem: "scheduler_gpustability", Name: "blocked_pods_tracked",
			Help: "Current bounded number of Pods tracked for precise snapshot requeue.", StabilityLevel: metrics.ALPHA,
		},
	)
	snapshotActivations = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem: "scheduler_gpustability", Name: "snapshot_activations_total",
			Help: "Pods precisely activated after a relevant local snapshot publication.", StabilityLevel: metrics.ALPHA,
		},
	)
)

func registerPluginMetrics() {
	registerMetricsOnce.Do(func() {
		legacyregistry.MustRegister(
			collectionDuration, collectionFailures, refreshSuppressed,
			snapshotReads, snapshotAge, filterDecisions, scoreEvaluations, scoreValues,
			targetCount, queueDepth, workerUtilization, parseDuration, profileReloads,
			profileReloadError, discardedGenerations, backoffTargets, circuitTargets,
			blockedPodCount, snapshotActivations,
		)
	})
}

func observeBlockedPods(count int) {
	blockedPodCount.Set(float64(count))
}

func observeSnapshotActivations(count int) {
	snapshotActivations.Add(float64(count))
}

func observeTargetCount(count int) {
	targetCount.Set(float64(count))
}

func observeQueueDepth(depth int) {
	queueDepth.Set(float64(depth))
}

func observeWorkerActive(delta int) {
	workerUtilization.Add(float64(delta))
}

func observeParse(duration time.Duration, err error) {
	result := "success"
	if err != nil {
		result = "failure"
	}
	parseDuration.WithLabelValues(result).Observe(duration.Seconds())
}

func observeProfileReload(changed bool, err error) {
	result := "unchanged"
	if err != nil {
		result = "failure"
		profileReloadError.Set(1)
	} else if changed {
		result = "success"
		profileReloadError.Set(0)
	}
	profileReloads.WithLabelValues(result).Inc()
}

func observeDiscardedGeneration() {
	discardedGenerations.Inc()
}

func observeBackoffTargets(count int) {
	backoffTargets.Set(float64(count))
}

func observeCircuitTargets(count int) {
	circuitTargets.Set(float64(count))
}

func observeCollection(duration time.Duration, err error) {
	result := "success"
	if err != nil {
		result = "failure"
	}
	collectionDuration.WithLabelValues(result).Observe(duration.Seconds())
}

func observeCollectorFailure(reason string) {
	collectionFailures.WithLabelValues(reason).Inc()
}

func observeRefreshSuppressed(reason string) {
	refreshSuppressed.WithLabelValues(reason).Inc()
}

func observeSnapshotRead(state snapshotState) {
	snapshotReads.WithLabelValues(string(state)).Inc()
}

func observeSnapshotAge(age time.Duration) {
	if age < 0 {
		age = 0
	}
	snapshotAge.Observe(age.Seconds())
}

func observeFilterDecision(policy StatePolicy, reason string) {
	filterDecisions.WithLabelValues(string(policy), reason).Inc()
}

func observeScore(policy StatePolicy, state snapshotState, reason string, score int64) {
	scoreEvaluations.WithLabelValues(string(policy), string(state), reason).Inc()
	scoreValues.WithLabelValues(string(policy)).Observe(float64(score))
}
