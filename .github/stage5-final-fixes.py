#!/usr/bin/env python3
from pathlib import Path

path = Path("contrib/space-compute/pkg/planner/execution.go")
text = path.read_text()
old = '''\t\tspacev1.PlacementTransferPending: {spacev1.PlacementDispatched: true, spacev1.PlacementRunning: true, spacev1.PlacementFailed: true},\n\t\tspacev1.PlacementReady:           {spacev1.PlacementDispatched: true, spacev1.PlacementRunning: true, spacev1.PlacementFailed: true},\n'''
new = '''\t\tspacev1.PlacementTransferPending:       {spacev1.PlacementDispatched: true, spacev1.PlacementRunning: true, spacev1.PlacementFailed: true},\n\t\tspacev1.PlacementExecutionLeasePending: {spacev1.PlacementDispatched: true, spacev1.PlacementRunning: true, spacev1.PlacementFailed: true},\n\t\tspacev1.PlacementReady:                 {spacev1.PlacementDispatched: true, spacev1.PlacementRunning: true, spacev1.PlacementFailed: true},\n'''
if text.count(old) != 1:
    raise SystemExit(f"planner transition marker count {text.count(old)}")
path.write_text(text.replace(old, new, 1))
print("ExecutionLeasePending transition semantics staged")
