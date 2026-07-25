#!/usr/bin/env python3
from pathlib import Path

# Envelope payload is opaque bytes; JSON encoding should base64 it rather than
# interpreting untrusted binary chunks as embedded JSON.
p=Path('contrib/space-compute/pkg/transport/envelope.go')
text=p.read_text()
text=text.replace('\t"encoding/json"\n','')
old='\tPayload       json.RawMessage         `json:"payload"`'
new='\tPayload       []byte                  `json:"payload"`'
if text.count(old)!=1: raise SystemExit(f'envelope payload marker count {text.count(old)}')
p.write_text(text.replace(old,new,1))

# Execution observations may be local self-domain evidence. Reuse the same
# identity/provenance bounds without the transfer-only source!=destination rule.
p=Path('contrib/space-compute/pkg/apis/v1alpha1/phase5_validation.go')
text=p.read_text()
old='''\tvalidateReceiptIdentity("spec.observationID", observation.Spec.ObservationID, &errs)
\tvalidateReceiptCommon(observation.Spec.MissionUID, observation.Spec.PlanID, observation.Spec.Attempt, observation.Spec.Source, observation.Spec.Destination, 0, strings.Repeat("0", 64), observation.Spec.Provenance, &errs)'''
new='''\tvalidateReceiptIdentity("spec.observationID", observation.Spec.ObservationID, &errs)
\tif observation.Spec.MissionUID == "" || len(observation.Spec.MissionUID) > 128 || strings.ContainsAny(observation.Spec.MissionUID, "\\r\\n\\x00") { errs.add("spec.missionUID", "must be non-empty and bounded") }
\tif problems := utilvalidation.IsDNS1123Label(observation.Spec.PlanID); len(problems) > 0 { errs.add("spec.planID", strings.Join(problems, ", ")) }
\tif observation.Spec.Attempt < 1 || observation.Spec.Attempt > 100 { errs.add("spec.attempt", "must be between 1 and 100") }
\tvalidateDomain("spec.source", observation.Spec.Source, &errs)
\tvalidateDomain("spec.destination", observation.Spec.Destination, &errs)
\tvalidateProvenance("spec.provenance", observation.Spec.Provenance, &errs)
\tif observation.Spec.Provenance.PreviousDigest != "" { validateLowerSHA256("spec.provenance.previousDigest", observation.Spec.Provenance.PreviousDigest, &errs) }
'''
if text.count(old)!=1: raise SystemExit(f'observation common marker count {text.count(old)}')
p.write_text(text.replace(old,new,1))
print('stage5 binary envelope/local observation fix applied')
