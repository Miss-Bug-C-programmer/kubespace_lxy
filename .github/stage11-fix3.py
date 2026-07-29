from pathlib import Path

p = Path('contrib/space-compute/pkg/kube/repository_test.go')
text = p.read_text()
for old, new in [
    ('unexpected live resource list', 'unexpected resource summary API list'),
    ('unexpected live link list', 'unexpected link API list'),
]:
    if text.count(old) != 1:
        raise SystemExit(f'repository_test marker mismatch: {old!r}')
    text = text.replace(old, new, 1)
p.write_text(text)
