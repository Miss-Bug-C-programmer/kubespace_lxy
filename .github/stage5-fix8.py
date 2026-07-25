#!/usr/bin/env python3
from pathlib import Path

def remove_lines(path, predicates):
    p=Path(path); lines=p.read_text().splitlines(True); out=[]
    for line in lines:
        if any(pred(line) for pred in predicates):
            continue
        out.append(line)
    p.write_text(''.join(out))

remove_lines('contrib/space-compute/pkg/transport/envelope.go', [lambda line: '"encoding/json"' in line])
remove_lines('cmd/space-compute-domain-agent/store.go', [
    lambda line: 'spaceplanner "github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"' in line,
    lambda line: 'var _ = spaceplanner.ErrNotFound' in line,
])
print('stage5 format-independent import cleanup applied')
