#!/usr/bin/env bash
set -euo pipefail
ruby -e 'require "yaml"; ARGV.each { |p| YAML.load_stream(File.read(p)) }' docs/space-compute/manifests/*.yaml docs/gpu-scheduler/manifests/space-compute-scheduler.yaml
ruby -ryaml -e 'd=YAML.load_stream(File.read(ARGV[0])); c=d.select{|x|x&&x["kind"]=="CustomResourceDefinition"}; abort("CRD count") unless c.size==11; c.each{|x|v=x.dig("spec","versions").to_h{|z|[z["name"],z]};a=v["v1alpha1"];b=v["v1beta1"];abort("version contract") unless a&&b&&a["served"]&&a["storage"]==false&&b["served"]&&b["storage"]&&a["schema"]==b["schema"]&&x.dig("spec","conversion","strategy")=="Webhook"&&x.dig("spec","conversion","webhook","clientConfig","service","path")=="/convert"}' docs/space-compute/manifests/phase9-canonical-crds.yaml
changed="$(git diff --name-only "$GITHUB_SHA" -- pkg/executor/embed pkg/scheduler/plugins/gpustability cmd/space-compute-scheduler docs/gpu-scheduler/manifests/space-compute-scheduler.yaml || true)"
test -z "$changed"
grep -q 'kind: PhysicalDeviceInventory' docs/space-compute/manifests/phase9-canonical-crds.yaml
for f in stableDeviceID kubernetesResourceName allocationID peerInterconnects memoryBandwidthBitsPerSecond interconnectBandwidthBitsPerSecond firmware driver runtime libraries temperatureMilliCelsius powerMilliwatts; do grep -q "$f" docs/space-compute/manifests/phase9-canonical-crds.yaml; done
for f in cpu: systemMemoryBytes ephemeralStorageBytes persistentStorage numaTopology trust: autonomyDurationSeconds energy: physicalDeviceInventoryRef cpuMilliCapacity cpuMilliAvailable memoryCapacityBytes memoryAvailableBytes capacityMilliWattHours availableMilliWattHours; do grep -q "$f" docs/space-compute/manifests/phase9-canonical-crds.yaml; done
for f in workingMemoryBytes workingStorageBytes minimumBandwidthBitsPerSecond maximumRTTMicroseconds maximumLossPartsPerMillion selectedCapabilitySetName selectedPhysicalDeviceConstraints transferState transferReceiptReferences executionLeaseReference fencingTokenHash checkpointReceipt resultReceipt remoteAcknowledgementSequence; do grep -q "$f" docs/space-compute/manifests/phase9-canonical-crds.yaml; done
grep -q 'CanonicalVersion.*v1beta1' contrib/space-compute/pkg/apis/v1alpha1/register.go
grep -q 'suspend: true' docs/space-compute/manifests/storage-version-migrator.yaml
grep -q -- '--target-version=v1beta1' docs/space-compute/manifests/storage-version-migrator.yaml
grep -q 'apiVersions: \[v1alpha1, v1beta1\]' docs/space-compute/manifests/mission-admission-webhook.yaml
grep -q 'apiVersions: \[v1alpha1, v1beta1\]' docs/space-compute/manifests/reporter-admission-webhook.yaml
