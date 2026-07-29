from pathlib import Path


def replace_once(path, old, new):
    p = Path(path)
    text = p.read_text()
    if text.count(old) != 1:
        raise SystemExit(f'{path}: expected one marker {old!r}, got {text.count(old)}')
    p.write_text(text.replace(old, new, 1))

replace_once('cmd/space-compute-domain-agent/config.go', 'package main\n', '// Package main implements the Space Compute domain agent command.\npackage main\n')
replace_once('pkg/scheduler/plugins/gpustability/allocation_identity.go', 'package gpustability\n', '// Package gpustability implements the isolated Space Compute scheduler plugin.\npackage gpustability\n')
replace_once('contrib/space-compute/pkg/apis/v1alpha1/phase9_test.go', 'f.Fuzz(func(t *testing.T, stableID string, total, free int64, confidence int64) {', 'f.Fuzz(func(_ *testing.T, stableID string, total, free int64, confidence int64) {')
replace_once('contrib/space-compute/pkg/policy/local_test.go', 'f.Fuzz(func(t *testing.T, mission, placement string) {', 'f.Fuzz(func(_ *testing.T, mission, placement string) {')
replace_once('contrib/space-compute/pkg/workload/controller.go', 'func BuildAttemptPod(_ *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent, template corev1.PodTemplateSpec) (*corev1.Pod, error) {', 'func BuildAttemptPod(_ *spacev1.SpaceMission, _ *spacev1.SpacePlacementIntent, template corev1.PodTemplateSpec) (*corev1.Pod, error) {')
replace_once('pkg/scheduler/plugins/gpustability/gpu_stability.go', 'func max(values []float64) float64 {', 'func maxValue(values []float64) float64 {')
replace_once('pkg/scheduler/plugins/gpustability/phase1_test.go', 'copy := node.DeepCopy()\n\t\t\tcopy.Annotations[tt.key] = tt.value\n\t\t\tif _, err := plugin.collector.targetForNode(copy); err == nil {', 'testNode := node.DeepCopy()\n\t\t\ttestNode.Annotations[tt.key] = tt.value\n\t\t\tif _, err := plugin.collector.targetForNode(testNode); err == nil {')
replace_once('pkg/scheduler/plugins/gpustability/phase1_test.go', 'f.Fuzz(func(t *testing.T, raw string) {\n\t\t_, _ = parseMetrics(strings.NewReader(raw))', 'f.Fuzz(func(_ *testing.T, raw string) {\n\t\t_, _ = parseMetrics(strings.NewReader(raw))')
replace_once('pkg/scheduler/plugins/gpustability/phase2_test.go', 'copy := node.DeepCopy()\n\t\t\t\tif (worker+iteration)%2 == 0 {\n\t\t\t\t\tcopy.Annotations[AnnotationExporterProfile] = "iluvatar"\n\t\t\t\t}\n\t\t\t\tcollector.observeNode(copy)\n\t\t\t\t_ = collector.snapshotForNode(copy)', 'nodeCopy := node.DeepCopy()\n\t\t\t\tif (worker+iteration)%2 == 0 {\n\t\t\t\t\tnodeCopy.Annotations[AnnotationExporterProfile] = "iluvatar"\n\t\t\t\t}\n\t\t\t\tcollector.observeNode(nodeCopy)\n\t\t\t\t_ = collector.snapshotForNode(nodeCopy)')
replace_once('pkg/scheduler/plugins/gpustability/phase3_test.go', 'copy := metrics\n\t\t\t\tcopy.FetchedAt, copy.ValidUntil = now, now.Add(time.Hour)\n\t\t\t\tif !collector.store.publish(target, copy, now, copy.ValidUntil) {', 'metricsCopy := metrics\n\t\t\t\tmetricsCopy.FetchedAt, metricsCopy.ValidUntil = now, now.Add(time.Hour)\n\t\t\t\tif !collector.store.publish(target, metricsCopy, now, metricsCopy.ValidUntil) {')

exec(compile(Path('.github/stage11-fix5.py').read_text(), '.github/stage11-fix5.py', 'exec'))
