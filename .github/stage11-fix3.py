from pathlib import Path

p = Path('contrib/space-compute/pkg/kube/repository_test.go')
text = p.read_text()
for old, new in [
    ('func(action k8stesting.Action) (bool, runtime.Object, error) {\n\t\treturn true, nil, fmt.Errorf("unexpected live resource list")',
     'func(_ k8stesting.Action) (bool, runtime.Object, error) {\n\t\treturn true, nil, fmt.Errorf("unexpected live resource list")'),
    ('func(action k8stesting.Action) (bool, runtime.Object, error) {\n\t\treturn true, nil, fmt.Errorf("unexpected live link list")',
     'func(_ k8stesting.Action) (bool, runtime.Object, error) {\n\t\treturn true, nil, fmt.Errorf("unexpected live link list")'),
]:
    if text.count(old) != 1:
        raise SystemExit(f'repository_test marker mismatch: {old!r}')
    text = text.replace(old, new, 1)
p.write_text(text)
