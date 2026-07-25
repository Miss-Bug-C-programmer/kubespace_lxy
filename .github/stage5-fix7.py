#!/usr/bin/env python3
from pathlib import Path
for path, needle in [
    ('contrib/space-compute/pkg/transport/envelope.go', '\t"encoding/json"\n'),
    ('cmd/space-compute-domain-agent/store.go', '\tspaceplanner "github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"\n'),
]:
    p=Path(path); text=p.read_text(); text=text.replace(needle,''); p.write_text(text)
p=Path('cmd/space-compute-domain-agent/store.go');text=p.read_text();text=text.replace('\nvar _ = spaceplanner.ErrNotFound\n','\n');p.write_text(text)
print('stage5 command imports cleaned')
