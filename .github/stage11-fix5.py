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
