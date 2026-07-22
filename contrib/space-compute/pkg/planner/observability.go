package planner

import (
	"sync"
	"time"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

var (
	registerMetrics      sync.Once
	planningLatency      = metrics.NewHistogramVec(&metrics.HistogramOpts{Subsystem: "space_compute_planner", Name: "planning_duration_seconds", Help: "Mission planning reconciliation latency by bounded result.", StabilityLevel: metrics.ALPHA, Buckets: metrics.ExponentialBuckets(0.001, 2, 16)}, []string{"result"})
	planningActive       = metrics.NewGauge(&metrics.GaugeOpts{Subsystem: "space_compute_planner", Name: "planning_active", Help: "Currently active planning reconciliations.", StabilityLevel: metrics.ALPHA})
	replans              = metrics.NewCounterVec(&metrics.CounterOpts{Subsystem: "space_compute_planner", Name: "replans_total", Help: "Replans by bounded material-change reason.", StabilityLevel: metrics.ALPHA}, []string{"reason"})
	deadlineSlack        = metrics.NewHistogram(&metrics.HistogramOpts{Subsystem: "space_compute_planner", Name: "deadline_slack_seconds", Help: "Guarded deadline slack for selected plans.", StabilityLevel: metrics.ALPHA, Buckets: []float64{0, 1, 10, 60, 300, 1800, 3600, 21600, 86400}})
	plannerSnapshotAge   = metrics.NewHistogram(&metrics.HistogramOpts{Subsystem: "space_compute_planner", Name: "snapshot_age_seconds", Help: "Age of resource/link snapshots read by planning.", StabilityLevel: metrics.ALPHA, Buckets: metrics.ExponentialBuckets(1, 2, 18)})
	linkRiskDecisions    = metrics.NewCounterVec(&metrics.CounterOpts{Subsystem: "space_compute_planner", Name: "link_risk_decisions_total", Help: "Selected plans by bounded link-risk class.", StabilityLevel: metrics.ALPHA}, []string{"class"})
	reconciliationErrors = metrics.NewCounterVec(&metrics.CounterOpts{Subsystem: "space_compute_planner", Name: "reconciliation_errors_total", Help: "Planner reconciliation errors by bounded stage.", StabilityLevel: metrics.ALPHA}, []string{"stage"})
	controllerQueueDepth = metrics.NewGaugeVec(&metrics.GaugeOpts{Subsystem: "space_compute_planner", Name: "queue_depth", Help: "Current controller work queue depth by bounded queue.", StabilityLevel: metrics.ALPHA}, []string{"queue"})
	retryExhausted       = metrics.NewCounterVec(&metrics.CounterOpts{Subsystem: "space_compute_planner", Name: "retry_exhausted_total", Help: "Controller items dropped after the bounded retry budget.", StabilityLevel: metrics.ALPHA}, []string{"queue"})
	apiWrites            = metrics.NewCounterVec(&metrics.CounterOpts{Subsystem: "space_compute_planner", Name: "api_writes_total", Help: "Controller API writes by bounded resource, operation, and result.", StabilityLevel: metrics.ALPHA}, []string{"resource", "operation", "result"})
)

type PrometheusObserver struct{}

func NewPrometheusObserver() PrometheusObserver {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(planningLatency, planningActive, replans, deadlineSlack, plannerSnapshotAge, linkRiskDecisions, reconciliationErrors, controllerQueueDepth, retryExhausted, apiWrites)
	})
	return PrometheusObserver{}
}
func (PrometheusObserver) PlanningStarted() { planningActive.Inc() }
func (PrometheusObserver) PlanningFinished(duration time.Duration, result string) {
	planningActive.Dec()
	planningLatency.WithLabelValues(boundedResult(result)).Observe(duration.Seconds())
}
func (PrometheusObserver) Replan(reason string) { replans.WithLabelValues(boundedReplan(reason)).Inc() }
func (PrometheusObserver) ReconciliationError(stage string) {
	reconciliationErrors.WithLabelValues(boundedStage(stage)).Inc()
}
func (PrometheusObserver) DeadlineSlack(value time.Duration) {
	if value < 0 {
		value = 0
	}
	deadlineSlack.Observe(value.Seconds())
}
func (PrometheusObserver) SnapshotAge(value time.Duration) {
	if value < 0 {
		value = 0
	}
	plannerSnapshotAge.Observe(value.Seconds())
}
func (PrometheusObserver) LinkRisk(class string) {
	if class != "low" && class != "medium" && class != "high" {
		class = "unknown"
	}
	linkRiskDecisions.WithLabelValues(class).Inc()
}
func (PrometheusObserver) QueueDepth(queue string, depth int) {
	controllerQueueDepth.WithLabelValues(boundedQueue(queue)).Set(float64(depth))
}
func (PrometheusObserver) RetryExhausted(queue string) {
	retryExhausted.WithLabelValues(boundedQueue(queue)).Inc()
}
func (PrometheusObserver) APIWrite(resource, operation, result string) {
	apiWrites.WithLabelValues(boundedResource(resource), boundedOperation(operation), boundedWriteResult(result)).Inc()
}
func boundedQueue(value string) string {
	if value == "missions" || value == "resources" {
		return value
	}
	return "other"
}
func boundedResource(value string) string {
	switch value {
	case "mission", "placement", "pod", "node", "link", "resource_summary":
		return value
	default:
		return "other"
	}
}
func boundedOperation(value string) string {
	switch value {
	case "create", "update", "delete", "status":
		return value
	default:
		return "other"
	}
}
func boundedWriteResult(value string) string {
	if value == "success" || value == "error" || value == "conflict" {
		return value
	}
	return "other"
}
func boundedResult(value string) string {
	switch value {
	case "deleted", "suspended", "invalid", "blocked", "idempotent", "planned", "checkpoint_wait", "failed", "retry_exhausted", "error":
		return value
	default:
		return "other"
	}
}
func boundedReplan(value string) string {
	switch value {
	case "plan_expired", "target_changed", "material_input_changed", "non_checkpointable_failed":
		return value
	default:
		return "other"
	}
}
func boundedStage(value string) string {
	switch value {
	case "mission_read", "mission_status", "resource_list", "link_list", "placement_read", "placement_status", "placement_apply":
		return value
	default:
		return "other"
	}
}
