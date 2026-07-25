#!/usr/bin/env python3
from pathlib import Path
p=Path('contrib/space-compute/pkg/execution/fence.go')
text=p.read_text()
for old,new in [
    ('observation.Spec.ObservedAt.After(&f.ExpiresAt)','observation.Spec.ObservedAt.After(f.ExpiresAt.Time)'),
    ('receipt.Spec.CompletedAt.After(&f.ExpiresAt)','receipt.Spec.CompletedAt.After(f.ExpiresAt.Time)'),
]:
    if text.count(old)!=1: raise SystemExit(f'marker mismatch: {old}')
    text=text.replace(old,new,1)
p.write_text(text)
print('stage5 fence time fix applied')
