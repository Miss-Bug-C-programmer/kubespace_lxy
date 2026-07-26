from pathlib import Path

path = Path("pkg/scheduler/plugins/gpustability/phase4_test.go")
text = path.read_text()
old_input = 'Inputs: []spacev1.DataObject{{ID: "local-frame", SizeBytes: 1000, Locations: []string{"leo-a"}}}'
new_input = 'Inputs: []spacev1.DataObject{{ID: "local-frame", SizeBytes: 1000, Locations: []spacev1.DataLocation{{Domain: spacev1.DomainReference{Name: "leo-a", ClusterID: "leo-a-cluster", OrbitClass: spacev1.OrbitLEO}}}}}'
old_result = 'ResultDestinations: []string{"ground-a"}'
new_result = 'ResultDestinations: []spacev1.DataLocation{{Domain: spacev1.DomainReference{Name: "ground-a", ClusterID: "ground-a-cluster", OrbitClass: spacev1.OrbitGround}}}'
for old, new in ((old_input, new_input), (old_result, new_result)):
    if text.count(old) != 1:
        raise SystemExit(f"expected exactly one fixture marker, found {text.count(old)}: {old}")
    text = text.replace(old, new, 1)
path.write_text(text)
