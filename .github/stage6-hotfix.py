from pathlib import Path

p = Path('contrib/space-compute/pkg/workload/controller_test.go')
text = p.read_text()
old = '''\tif len(evidence.intents) != 1 {\n\t\tt.Fatalf("transfer intents=%d, want 1", len(evidence.intents))\n\t}\n\tintent := evidence.intents[0]\n'''
new = '''\tif len(evidence.intents) != 0 {\n\t\tt.Fatalf("dispatcher created %d transfer intents; transport-agent must own intent writes", len(evidence.intents))\n\t}\n\tintents, err := BuildInputTransferIntents(mission, placement, coordinator)\n\tif err != nil || len(intents) != 1 {\n\t\tt.Fatalf("transport intent build count=%d err=%v", len(intents), err)\n\t}\n\tintent := intents[0]\n\tevidence.intents = append(evidence.intents, intent.DeepCopy())\n'''
if text.count(old) != 1:
    raise SystemExit(f'controller transfer assertion marker count={text.count(old)}')
p.write_text(text.replace(old, new, 1))
