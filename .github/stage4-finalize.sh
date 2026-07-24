#!/usr/bin/env bash
set -euo pipefail

LOG=/tmp/stage4-finalize.log
: > "$LOG"

on_error() {
  status=$?
  set +e
  git fetch origin main
  if [ "$(git rev-parse origin/main)" = "$(git rev-parse HEAD)" ]; then
    {
      echo "run_id=${GITHUB_RUN_ID:-unknown}"
      echo "base_sha=$(git rev-parse HEAD)"
      echo '--- last gate output ---'
      tail -n 600 "$LOG" 2>/dev/null || true
      echo '--- validator.go ---'
      sed -n '1,520p' contrib/space-compute/pkg/admission/validator.go 2>/dev/null || true
      echo '--- validator_test.go ---'
      sed -n '1,520p' contrib/space-compute/pkg/admission/validator_test.go 2>/dev/null || true
      echo '--- canonical.go ---'
      sed -n '1,360p' contrib/space-compute/pkg/apis/v1alpha1/canonical.go 2>/dev/null || true
    } > .github/stage4-failure.txt
    git add .github/stage4-failure.txt
    git config user.name space-compute-stage4-bot
    git config user.email space-compute-stage4-bot@users.noreply.github.com
    git commit -m 'chore: capture stage4 validation failure' && git push origin HEAD:main
  fi
  exit "$status"
}
trap on_error ERR

{
  echo '=== apply ==='
  python3 -m py_compile .github/stage4-complete.py
  python3 .github/stage4-complete.py
  gofmt -w \
    contrib/space-compute/pkg/apis/v1alpha1/*.go \
    contrib/space-compute/pkg/admission/*.go \
    contrib/space-compute/pkg/planner/*.go \
    cmd/space-compute-mission-planner/*.go \
    cmd/space-compute-reporter-webhook/*.go
  git diff --check

  echo '=== focused ==='
  go test ./contrib/space-compute/pkg/apis/v1alpha1 \
    -run '^(TestCanonicalReporterDigestIsStableAndCoversSchedulingFields|TestCanonicalResourceSummarySortsMapAndSetLikeFields|TestDerivedReporterObjectNamesBindFullDomainIdentity|TestReporterBindingValidationUsesPrincipalDerivedNameAndImmutableDomain|TestLinkValidationRejectsOverlapSkewStaleAndFastUnchangedUpdate)$' \
    -count=1 -timeout=90s
  go test ./contrib/space-compute/pkg/admission \
    -run '^(TestValidatorAcceptsTrustedSignedLinkAndRejectsForgery|TestValidatorEnforcesExactDigestChainIdentityAndStabilityUpdate|TestValidatorCoversSummaryAndReceiptKinds|TestAdmissionHTTPHandlerFailsClosed|TestKubernetesTrustSourceUsesDerivedBindingAndSingleSecret)$' \
    -count=1 -timeout=90s
  go test ./contrib/space-compute/pkg/planner \
    -run '^TestStabilityOnlyChangeChangesDigestMaterialInputAndTriggersReplan$' \
    -count=1 -timeout=90s
  go test ./cmd/space-compute-mission-planner \
    -run '^TestPhase4ManifestsHaveCRDsAdmissionIsolationAndLeastPrivilege$' \
    -count=1 -timeout=90s
  go test ./cmd/space-compute-reporter-webhook -count=1 -timeout=90s
  go build -buildvcs=false ./cmd/space-compute-reporter-webhook

  echo '=== race ==='
  go test -race \
    ./contrib/space-compute/pkg/apis/v1alpha1 \
    ./contrib/space-compute/pkg/admission \
    ./contrib/space-compute/pkg/planner \
    -run 'Test(Canonical|ReporterBinding|Validator|AdmissionHTTP|KubernetesTrustSource|StabilityOnly)' \
    -count=1 -timeout=180s

  echo '=== full ==='
  env GOCACHE="$RUNNER_TEMP/space-compute-stage4-gocache" scripts/space-compute all
  go test ./pkg/executor/embed -count=1 -timeout=120s

  echo '=== audit ==='
  test -z "$(gofmt -s -l contrib/space-compute/pkg/apis/v1alpha1/*.go contrib/space-compute/pkg/admission/*.go contrib/space-compute/pkg/planner/*.go cmd/space-compute-reporter-webhook/*.go)"
  go vet \
    ./contrib/space-compute/pkg/apis/v1alpha1 \
    ./contrib/space-compute/pkg/admission \
    ./contrib/space-compute/pkg/planner \
    ./cmd/space-compute-reporter-webhook
  grep -q 'StabilityMilli, w.ConfidenceMilli, w.Predicted' contrib/space-compute/pkg/apis/v1alpha1/validation.go
  grep -q 'ed25519.Verify' contrib/space-compute/pkg/admission/validator.go
  grep -q 'resourceNames: \[space-compute-reporter-public-keys\]' docs/space-compute/manifests/reporter-admission-webhook.yaml
  ! grep -q 'verbs: \[create, get, update, patch\]' docs/space-compute/manifests/mission-planner.yaml
  ! grep -R 'contrib/space-compute/pkg/admission' pkg/executor pkg/scheduler cmd/k3s 2>/dev/null

  echo '=== evidence ==='
  for file in docs/space-compute/IMPLEMENTATION_STATUS.md docs/space-compute/PHASE5_TEST_REPORT.md; do
    sed -i "s/__RUN_ID__/${GITHUB_RUN_ID}/g; s/__BASE_SHA__/$(git rev-parse HEAD)/g" "$file"
  done
  git diff --check

  echo '=== final push ==='
  git fetch origin main
  test "$(git rev-parse origin/main)" = "$(git rev-parse HEAD)"
  rm -rf .github/stage4-payload
  rm -f \
    .github/stage4-trigger \
    .github/stage4-trigger-v2 \
    .github/stage4-apply.py.gz.b64 \
    .github/stage4-apply.py.gz.b64.part* \
    .github/stage4-recovered.py \
    .github/stage4-complete.py \
    .github/stage4-finalize.sh \
    .github/stage4-failure.txt \
    .github/workflows/stage4-reporter-authenticity.yml \
    .github/workflows/stage4-finalize.yml \
    .github/workflows/stage4-simple-finalize.yml
  git add -A
  git diff --cached --check
  git config user.name space-compute-stage4-bot
  git config user.email space-compute-stage4-bot@users.noreply.github.com
  git commit -m 'fix: enforce reporter-domain data authenticity'
  git push origin HEAD:main
} 2>&1 | tee -a "$LOG"
