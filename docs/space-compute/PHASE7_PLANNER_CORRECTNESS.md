# Phase 7 planner correctness hardening

Phase 7 closes planner correctness defects that could overcommit accelerator capacity, choose an alternative capability set but estimate compute from a different set, overflow fixed-width arithmetic, or confuse same-named domains from different clusters.

## Deterministic capability allocation

Planner capability admission is now a single allocation problem per candidate domain.

1. Requirements are normalized by `class`, `architecture`, `model`, sorted `precision`, and sorted software constraints.
2. Exactly equivalent constraints are aggregated with checked integer addition.
3. `RequiredCapabilities` and each candidate `AlternativeCapabilities[*].allOf` are combined before allocation. A bucket is therefore never independently counted once for required capabilities and again for an alternative.
4. Capacity buckets are normalized and sorted. A bounded deterministic max-flow allocates integer device counts from buckets to compatible requirements. The resource API allows multiple buckets in one class when their model/architecture/precision/compute/fragmentation identity differs.
5. Alternative sets are evaluated in deterministic name/content order. The selected set, normalized selected requirements, and exact bucket allocations are retained in the candidate and emitted as `capability_set_selected` / `capability_allocation` placement explanations.

The matching graph is bounded by the v1alpha1 capability and device-bucket limits, so this does not introduce an unbounded combinatorial search.

## Compute estimate

`predictedComputeSeconds` consumes the actual allocation selected above. Alternative-only missions therefore use the compute rating of the selected alternative instead of falling back merely because `RequiredCapabilities` is empty. All intermediate compute arithmetic uses checked multiply/add/divide.

## Checked arithmetic and API bounds

Planner arithmetic now fails closed instead of wrapping for:

- input-byte totals and locality accounting;
- bytes-to-bits conversion and transfer duration division;
- seconds-to-`time.Duration` conversion;
- queue/safety/skew duration composition;
- timestamp addition;
- selected compute capacity and duration scaling;
- weighted score multiplication, accumulation, and division.

v1alpha1 validation and CRD schemas now bound device count, compute milli-rating, queue delay, maximum snapshot age, software maps/values, device topology enumerations, transfer epochs, and transfer byte counts. Planner topology input is additionally bounded to 20,000 resource-summary plus directed-link objects. The existing 5,000-domain qualification dataset remains within this limit.

## Structured data location identity

Mission data locations are no longer plain strings:

```go
type DataLocation struct {
    Domain DomainReference `json:"domain"`
    URI    string          `json:"uri,omitempty"`
}
```

Both `DataObject.Locations` and `SpaceMissionSpec.ResultDestinations` use `[]DataLocation`. The domain portion is always validated as the complete `{clusterID,name,orbitClass}` identity; a URI is optional but, when present, must be an absolute URI and is bounded to 2 KiB.

Planner link indexes use the complete directed identity:

`source.clusterID/source.name/source.orbitClass -> destination.clusterID/destination.name/destination.orbitClass`

`TransferEpoch` carries optional `sourceURI` / `destinationURI` so the selected endpoint is not discarded after planning. Same-named domains in different clusters are not interchangeable.

This is an intentional v1alpha1 schema tightening. Existing persisted Mission objects whose location fields are legacy strings must be migrated to structured `DataLocation` objects before they are updated under the new CRD schema. The implementation does not silently reinterpret a name-only legacy value because doing so would recreate the ambiguity this phase removes.

## Isolation

No default K3s scheduler source, registration, profile, manifest, or embedded scheduler path is changed by Phase 7. The planner remains outside scheduler framework callbacks and the standalone `space-compute-scheduler` remains an explicit opt-in scheduler profile.
