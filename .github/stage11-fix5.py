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

Path('.gitleaksignore').write_text('''# Exact historical fingerprints verified as non-secret upstream K3s fixtures.\n# Do not add path-wide or rule-wide suppressions here.\n0724c7a4e169ff997deb6189f98ec0152aa6c53c:contrib/util/DIAGNOSTICS.md:generic-api-key:41\n0724c7a4e169ff997deb6189f98ec0152aa6c53c:pkg/clientaccess/token_test.go:generic-api-key:24\n0724c7a4e169ff997deb6189f98ec0152aa6c53c:tests/docker/token/token_test.go:generic-api-key:51\n0724c7a4e169ff997deb6189f98ec0152aa6c53c:tests/docker/token/token_test.go:generic-api-key:88\n0724c7a4e169ff997deb6189f98ec0152aa6c53c:tests/perf/tests/load/secret.yaml:kubernetes-secret-yaml:2\n0724c7a4e169ff997deb6189f98ec0152aa6c53c:tests/perf/tests/load/secret.yaml:generic-api-key:7\n''')

Path('scripts/space-compute-secret-scan').write_text(r'''#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
command -v gitleaks >/dev/null 2>&1 || { echo "gitleaks is required" >&2; exit 2; }

# Full repository history scan. .gitleaksignore contains only reviewed, exact
# fingerprints for immutable upstream test/document fixtures.
gitleaks git --redact --no-banner --verbose .

# The candidate may still be uncommitted when this gate runs, so scan every
# Phase 11 production/configuration surface directly as well.
sources=(
  .github/workflows
  cmd/space-compute-mission-planner
  cmd/space-compute-domain-agent
  cmd/space-compute-scheduler
  contrib/space-compute
  pkg/scheduler/plugins/gpustability
  docs/space-compute
  docs/gpu-scheduler
  scripts/space-compute
  scripts/space-compute-phase11
  scripts/space-compute-secret-scan
  go.mod
  go.sum
  Dockerfile.dapper
  Dockerfile.local
)
for dockerfile in Dockerfile.space-compute-*; do sources+=("$dockerfile"); done
for source in "${sources[@]}"; do
  [[ -e "$source" ]] || { echo "secret-scan source missing: $source" >&2; exit 1; }
  gitleaks dir --redact --no-banner --verbose "$source"
done
''')

supply = Path('.github/workflows/space-compute-supply-chain.yml')
text = supply.read_text()
old = '      - name: Secret and credential history scan\n        run: gitleaks git --redact --no-banner --verbose .\n'
new = '      - name: Secret private-key token and kubeconfig scan\n        run: bash scripts/space-compute-secret-scan\n'
if text.count(old) != 1:
    raise SystemExit('supply-chain gitleaks marker mismatch')
supply.write_text(text.replace(old, new, 1))
