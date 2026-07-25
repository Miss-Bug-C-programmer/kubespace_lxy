#!/usr/bin/env python3
from pathlib import Path
p=Path('contrib/space-compute/pkg/workload/controller.go')
text=p.read_text()
old='\t"strconv"\n\t"strings"\n\t"time"'
new='\t"strconv"\n\t"time"'
if text.count(old)!=1: raise SystemExit(f'workload import marker count {text.count(old)}')
p.write_text(text.replace(old,new,1))
print('stage5 workload import fix applied')
