# Recorded Phase 4 decision fixtures

These files are immutable inputs/expectations for deterministic CPU-only replay.
`leo-node.json` is a real Kubernetes Node object. `pod-mission-intent.json` is
the production Pod projection schema. `expected-decision.json` records the
selected domain, guarded epochs, fixed-unit scores and explanation codes.

The recorded exporter input for this scenario is the first-class Iluvatar
fixture at
`pkg/scheduler/plugins/gpustability/testdata/fixtures/iluvatar.prom`.
`recorded-links.json` stores the four production `SpaceLinkSnapshot` inputs.
Domain resources are constructed as typed production CRDs in
`planner_test.go`; their exact fixed timestamp, identity, provenance, units and
windows are part of that replay and are validated before planning.
