#!/usr/bin/env python3
from pathlib import Path

def replace_once(path, old, new):
    p=Path(path); text=p.read_text()
    if text.count(old)!=1: raise SystemExit(f'{path}: direct marker count {text.count(old)} for {old[:80]!r}')
    p.write_text(text.replace(old,new,1))

replace_once('contrib/space-compute/pkg/apis/v1alpha1/types.go',
'''type DataObject struct {
\tID        string   `json:"id"`
\tSizeBytes int64    `json:"sizeBytes"`
\tLocations []string `json:"locations"`
}''',
'''type DataObject struct {
\tID            string   `json:"id"`
\tSizeBytes     int64    `json:"sizeBytes"`
\tLocations     []string `json:"locations"`
\t// PayloadDigest is required before a non-local input can be transferred.
\t// Local-only legacy inputs remain valid without it.
\tPayloadDigest string   `json:"payloadDigest,omitempty"`
}''')
replace_once('contrib/space-compute/pkg/apis/v1alpha1/validation.go',
'''\t\tif input.SizeBytes > 0 && len(input.Locations) == 0 {
\t\t\terrs.add(path+".locations", "is required for non-empty input")
\t\t}
\t\tvalidateLocations(path+".locations", input.Locations, &errs)''',
'''\t\tif input.SizeBytes > 0 && len(input.Locations) == 0 {
\t\t\terrs.add(path+".locations", "is required for non-empty input")
\t\t}
\t\tif input.PayloadDigest != "" { validateLowerSHA256(path+".payloadDigest", input.PayloadDigest, &errs) }
\t\tvalidateLocations(path+".locations", input.Locations, &errs)''')
replace_once('contrib/space-compute/pkg/workload/controller.go',
'''        intent:=&spacev1.SpaceTransferIntent{TypeMeta:metav1.TypeMeta{APIVersion:spacev1.SchemeGroupVersion.String(),Kind:"SpaceTransferIntent"},ObjectMeta:metav1.ObjectMeta{Name:spacev1.TransferIntentName(epoch.Source,epoch.Destination,string(mission.UID),placement.Spec.PlanID,transferID)},Spec:spacev1.SpaceTransferIntentSpec{TransferID:transferID,MissionUID:string(mission.UID),PlanID:placement.Spec.PlanID,Attempt:placement.Spec.Attempt,Source:epoch.Source,Destination:epoch.Destination,DataID:epoch.DataID,Bytes:epoch.Bytes,PayloadDigest:lookupInputDigest(mission,epoch.DataID),Window:epoch,ExpiresAt:placement.Spec.ExpiresAt}}
        if intent.Spec.PayloadDigest=="" { intent.Spec.PayloadDigest=strings.Repeat("0",64) }''',
'''        payloadDigest:=lookupInputDigest(mission,epoch.DataID)
        if payloadDigest=="" { return 0,fmt.Errorf("cross-domain input %q requires a trusted payloadDigest",epoch.DataID) }
        intent:=&spacev1.SpaceTransferIntent{TypeMeta:metav1.TypeMeta{APIVersion:spacev1.SchemeGroupVersion.String(),Kind:"SpaceTransferIntent"},ObjectMeta:metav1.ObjectMeta{Name:spacev1.TransferIntentName(epoch.Source,epoch.Destination,string(mission.UID),placement.Spec.PlanID,transferID)},Spec:spacev1.SpaceTransferIntentSpec{TransferID:transferID,MissionUID:string(mission.UID),PlanID:placement.Spec.PlanID,Attempt:placement.Spec.Attempt,Source:epoch.Source,Destination:epoch.Destination,DataID:epoch.DataID,Bytes:epoch.Bytes,PayloadDigest:payloadDigest,Window:epoch,ExpiresAt:placement.Spec.ExpiresAt}}''')
replace_once('contrib/space-compute/pkg/workload/controller.go',
'''func lookupInputDigest(m *spacev1.SpaceMission,id string)string{return ""}''',
'''func lookupInputDigest(m *spacev1.SpaceMission,id string)string{for _,input:=range m.Spec.Inputs{if input.ID==id{return input.PayloadDigest}};return ""}''')

# Reuse the already-reviewed strengthened test body without executing its fragile patch prelude.
src=Path('.github/stage5-tests.py').read_text()
marker="write('contrib/space-compute/pkg/workload/controller_test.go', r'''"
start=src.find(marker)
if start<0: raise SystemExit('controller test body marker missing')
start+=len(marker)
end=src.find("''')\nprint('stage5 workload evidence tests applied')",start)
if end<0: raise SystemExit('controller test body terminator missing')
Path('contrib/space-compute/pkg/workload/controller_test.go').write_text(src[start:end])
print('stage5 direct workload evidence tests applied')
