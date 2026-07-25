#!/usr/bin/env python3
from pathlib import Path
p=Path('contrib/space-compute/pkg/transport/agent.go')
text=p.read_text()
old='func (a *Agent) enqueueLeaseGrant(ctx, namespace string, lease *spacev1.SpaceExecutionLease, token string) error {'
new='func (a *Agent) enqueueLeaseGrant(namespace string, lease *spacev1.SpaceExecutionLease, token string) error {'
if text.count(old)!=1: raise SystemExit(f'enqueueLeaseGrant signature marker count {text.count(old)}')
text=text.replace(old,new,1)
old='''\t\tif next.Spec.Destination != a.Local {
\t\t\ttoken, err := a.Store.GetFenceToken(ctx, "", next.Spec.Fence)
\t\t\tif err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t\tif err := a.enqueueLeaseGrant("", next, token); err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t}'''
new='''\t\tif next.Spec.Destination != a.Local {
\t\t\t// The plaintext fencing token is sent only in the initial lease grant.
\t\t\t// Same-epoch heartbeats update the signed lease object; the origin keeps
\t\t\t// the already-persisted token Secret and stale tokens remain unchanged.
\t\t\tif err := a.enqueueReporterObject(next.Spec.Destination, "spaceexecutionleases", next, next.Spec.Fence.MissionUID, next.Spec.Fence.PlanID, next.Spec.Fence.Attempt, next.Spec.Provenance.Sequence, next.Spec.Fence.ExpiresAt.Time); err != nil {
\t\t\t\treturn err
\t\t\t}
\t\t}'''
if text.count(old)!=1: raise SystemExit(f'heartbeat grant marker count {text.count(old)}')
text=text.replace(old,new,1)
p.write_text(text)
print('stage5 lease grant/heartbeat fix applied')
