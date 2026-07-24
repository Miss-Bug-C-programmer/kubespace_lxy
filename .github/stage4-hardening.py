#!/usr/bin/env python3
from pathlib import Path

metrics = Path('pkg/scheduler/plugins/gpustability/metrics_profiles.go')
text = metrics.read_text()
old = '''func parsePrometheusMetricsWithLimits(r io.Reader, limits parserLimits) (*metricStore, error) {
\tparser := expfmt.TextParser{}
\tfamilies, err := parser.TextToMetricFamilies(r)
'''
new = '''func parsePrometheusMetricsWithLimits(r io.Reader, limits parserLimits) (store *metricStore, err error) {
\t// Prometheus text is supplied by an untrusted node-local exporter. Some
\t// malformed inputs can trigger a panic inside expfmt.TextParser instead of
\t// returning an error. Keep that third-party parser failure inside this trust
\t// boundary and fail closed; scheduler collection workers must never crash.
\tdefer func() {
\t\tif recovered := recover(); recovered != nil {
\t\t\tstore = nil
\t\t\terr = fmt.Errorf("parse Prometheus metrics: parser panic: %v", recovered)
\t\t}
\t}()
\n\tparser := expfmt.TextParser{}
\tfamilies, err := parser.TextToMetricFamilies(r)
'''
if old not in text:
    raise SystemExit('metrics parser function marker not found')
text = text.replace(old, new, 1)
old_store = '\tstore := &metricStore{samples: map[string][]metricSample{}, maxDevices: limits.MaxDevices}\n'
new_store = '\tstore = &metricStore{samples: map[string][]metricSample{}, maxDevices: limits.MaxDevices}\n'
if old_store not in text:
    raise SystemExit('metric store marker not found')
metrics.write_text(text.replace(old_store, new_store, 1))

phase1 = Path('pkg/scheduler/plugins/gpustability/phase1_test.go')
test_text = phase1.read_text()
marker = 'TestParsePrometheusMetricsConvertsParserPanicToError'
if marker not in test_text:
    test_text += r'''

type panicMetricReader struct{}

func (panicMetricReader) Read([]byte) (int, error) {
	panic("malformed exporter stream")
}

func TestParsePrometheusMetricsConvertsParserPanicToError(t *testing.T) {
	_, err := parsePrometheusMetrics(panicMetricReader{})
	if err == nil {
		t.Fatal("parsePrometheusMetrics returned nil error after parser panic")
	}
	if !strings.Contains(err.Error(), "parser panic") {
		t.Fatalf("error = %q, want parser panic context", err)
	}
}
'''
    phase1.write_text(test_text)

status = Path('docs/space-compute/IMPLEMENTATION_STATUS.md')
status_text = status.read_text()
status_marker = 'Untrusted Prometheus parser panic containment'
if status_marker not in status_text:
    status_text += '''\n\n### Untrusted Prometheus parser panic containment\n\nThe Phase 4 full qualification fuzz gate exposed an expfmt.TextParser panic on malformed exporter text. The production parser boundary now recovers third-party parser panics, returns a fail-closed parse error and keeps collector/scheduler processes alive. A direct panic-boundary regression is included; the existing fuzz gate remains enabled and unchanged.\n'''
    status.write_text(status_text)
