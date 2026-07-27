#!/usr/bin/env bash
set -euo pipefail
set -o pipefail
go test ./cmd/space-compute-conversion-webhook ./cmd/space-compute-storage-migrator ./cmd/space-compute-mission-planner ./cmd/space-compute-reporter-webhook ./cmd/space-compute-domain-agent ./contrib/space-compute/pkg/apis/v1alpha1 ./contrib/space-compute/pkg/apis/v1beta1 ./contrib/space-compute/pkg/conversion ./contrib/space-compute/pkg/migration ./contrib/space-compute/pkg/admission ./contrib/space-compute/pkg/kube ./contrib/space-compute/pkg/planner ./contrib/space-compute/pkg/transport ./contrib/space-compute/pkg/workload -count=1 2>&1 | tee /tmp/stage9-focused.log
go test -race ./contrib/space-compute/pkg/conversion ./contrib/space-compute/pkg/migration ./contrib/space-compute/pkg/admission ./contrib/space-compute/pkg/kube ./contrib/space-compute/pkg/planner ./contrib/space-compute/pkg/workload -count=1 2>&1 | tee /tmp/stage9-race.log
scripts/space-compute all 2>&1 | tee /tmp/space-compute-all.log
go mod verify
go mod tidy -diff
go test ./pkg/executor/embed -count=1
