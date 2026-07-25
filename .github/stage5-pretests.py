#!/usr/bin/env python3
from pathlib import Path
p=Path('.github/stage5-tests.py')
text=p.read_text()
old='''\t\tintent := &spacev1.SpaceTransferIntent{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceTransferIntent"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.TransferIntentName(epoch.Source, epoch.Destination, string(mission.UID), placement.Spec.PlanID, transferID)}, Spec: spacev1.SpaceTransferIntentSpec{TransferID: transferID, MissionUID: string(mission.UID), PlanID: placement.Spec.PlanID, Attempt: placement.Spec.Attempt, Source: epoch.Source, Destination: epoch.Destination, DataID: epoch.DataID, Bytes: epoch.Bytes, PayloadDigest: lookupInputDigest(mission, epoch.DataID), Window: epoch, ExpiresAt: placement.Spec.ExpiresAt}}
\t\tif intent.Spec.PayloadDigest == "" {
\t\t\tintent.Spec.PayloadDigest = strings.Repeat("0", 64)
\t\t}'''
new='''        intent:=&spacev1.SpaceTransferIntent{TypeMeta:metav1.TypeMeta{APIVersion:spacev1.SchemeGroupVersion.String(),Kind:"SpaceTransferIntent"},ObjectMeta:metav1.ObjectMeta{Name:spacev1.TransferIntentName(epoch.Source,epoch.Destination,string(mission.UID),placement.Spec.PlanID,transferID)},Spec:spacev1.SpaceTransferIntentSpec{TransferID:transferID,MissionUID:string(mission.UID),PlanID:placement.Spec.PlanID,Attempt:placement.Spec.Attempt,Source:epoch.Source,Destination:epoch.Destination,DataID:epoch.DataID,Bytes:epoch.Bytes,PayloadDigest:lookupInputDigest(mission,epoch.DataID),Window:epoch,ExpiresAt:placement.Spec.ExpiresAt}}
        if intent.Spec.PayloadDigest=="" { intent.Spec.PayloadDigest=strings.Repeat("0",64) }'''
if text.count(old)!=1: raise SystemExit('stage5-tests intent marker source mismatch')
text=text.replace(old,new,1)
old2='''func lookupInputDigest(m *spacev1.SpaceMission, id string) string { return "" }'''
new2='''func lookupInputDigest(m *spacev1.SpaceMission,id string)string{return ""}'''
if text.count(old2)!=1: raise SystemExit('stage5-tests lookup marker source mismatch')
text=text.replace(old2,new2,1)
p.write_text(text)
print('stage5 workload patch markers stabilized')
