#!/usr/bin/env python3
from pathlib import Path
p=Path('contrib/space-compute/pkg/transport/agent.go')
text=p.read_text()
text=text.replace('a.enqueueLeaseGrant(ctx, "", next, token)','a.enqueueLeaseGrant("", next, token)')
text=text.replace('a.enqueueLeaseGrant(ctx, r.Namespace, lease, token)','a.enqueueLeaseGrant(r.Namespace, lease, token)')
if '"k8s.io/apimachinery/pkg/runtime"' not in text:
    text=text.replace('metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"','metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"\n\t"k8s.io/apimachinery/pkg/runtime"')
old='func (a *Agent) signReporter(object any, p *spacev1.Provenance) error {'
new='func (a *Agent) signReporter(object runtime.Object, p *spacev1.Provenance) error {'
if text.count(old)!=1: raise SystemExit('signReporter marker mismatch')
text=text.replace(old,new,1)
p.write_text(text)
print('stage5 agent runtime interface fix applied')
