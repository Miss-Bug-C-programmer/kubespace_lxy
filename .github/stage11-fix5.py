from pathlib import Path

policy = Path('docs/space-compute/manifests/image-signature-policy.yaml')
text = policy.read_text()
if text.count('  mode: enforce\n') != 2:
    raise SystemExit(f'expected two obsolete mode fields, got {text.count("  mode: enforce\\n")}')
text = text.replace('  mode: enforce\n', '')
policy.write_text(text)

test = Path('cmd/space-compute-mission-planner/main_test.go')
text = test.read_text()
old = '{"kind: ClusterImagePolicy", "mode: enforce", "https://token.actions.githubusercontent.com", "space-compute-supply-chain.yml@refs/heads/main", "predicateType: https://spdx.dev/Document"}'
new = '{"kind: ClusterImagePolicy", "https://token.actions.githubusercontent.com", "space-compute-supply-chain.yml@refs/heads/main", "predicateType: https://spdx.dev/Document"}'
if text.count(old) != 1:
    raise SystemExit('image policy test marker mismatch')
test.write_text(text.replace(old, new, 1))

profiles = Path('pkg/scheduler/plugins/gpustability/metrics_profiles.go')
text = profiles.read_text()
for old, new in [('m.TemperatureC = max(temperatures)', 'm.TemperatureC = maxValue(temperatures)'), ('return max(values)', 'return maxValue(values)')]:
    if text.count(old) != 1:
        raise SystemExit(f'metrics profile max helper marker mismatch: {old!r}')
    text = text.replace(old, new, 1)
profiles.write_text(text)

workload = Path('contrib/space-compute/pkg/workload/controller.go')
text = workload.read_text()
old = 'func BuildAttemptPod(_ *spacev1.SpaceMission, _ *spacev1.SpacePlacementIntent, template corev1.PodTemplateSpec) (*corev1.Pod, error) {'
new = 'func BuildAttemptPod(_ *spacev1.SpaceMission, _ *spacev1.SpacePlacementIntent, _ corev1.PodTemplateSpec) (*corev1.Pod, error) {'
if text.count(old) != 1:
    raise SystemExit('BuildAttemptPod legacy fail-closed signature marker mismatch')
workload.write_text(text.replace(old, new, 1))

for path, old, new in [
    ('pkg/scheduler/plugins/gpustability/phase1_test.go', 'f.Fuzz(func(t *testing.T, raw string) {\n\t\t_, _ = configFromArgs(&runtime.Unknown{Raw: []byte(raw)})', 'f.Fuzz(func(_ *testing.T, raw string) {\n\t\t_, _ = configFromArgs(&runtime.Unknown{Raw: []byte(raw)})'),
    ('pkg/scheduler/plugins/gpustability/phase3_test.go', 'f.Fuzz(func(t *testing.T, raw string) {\n\t\tpod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{AnnotationWorkloadIntent: raw}}}', 'f.Fuzz(func(_ *testing.T, raw string) {\n\t\tpod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{AnnotationWorkloadIntent: raw}}}'),
]:
    p = Path(path)
    text = p.read_text()
    if text.count(old) != 1:
        raise SystemExit(f'{path}: remaining revive marker mismatch')
    p.write_text(text.replace(old, new, 1))
