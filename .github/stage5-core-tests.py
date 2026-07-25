#!/usr/bin/env python3
from pathlib import Path

def write(path,text):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True);p.write_text(text)

write('contrib/space-compute/pkg/execution/fence_test.go', r'''package execution

import (
    "strings"
    "testing"
    "time"

    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/types"

    spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

func TestLeaseAdvanceRejectsOldEpochAndTokenReuse(t *testing.T){now:=time.Date(2026,7,25,0,0,0,0,time.UTC);m,p,l:=fenceFixture(now);same:=l.DeepCopy();same.Spec.HeartbeatAt=metav1.NewTime(now.Add(time.Minute));same.Spec.Fence.ExpiresAt=metav1.NewTime(now.Add(6*time.Minute));same.Spec.Fence.TokenHash=strings.Repeat("c",64);if err:=ValidateLeaseAdvance(l,same,now.Add(time.Minute));err==nil{t.Fatal("same epoch token change accepted")};next:=l.DeepCopy();next.Spec.Fence.Attempt=2;next.Spec.Fence.LeaseEpoch=2;next.Spec.Fence.ExpiresAt=metav1.NewTime(now.Add(10*time.Minute));next.Spec.HeartbeatAt=metav1.NewTime(now.Add(time.Minute));if err:=ValidateLeaseAdvance(l,next,now.Add(time.Minute));err==nil{t.Fatal("higher epoch reused old token")};_,_=m,p}

func TestNonCheckpointablePartitionNeverDuplicatesOnExpiryAlone(t *testing.T){now:=time.Date(2026,7,25,0,0,0,0,time.UTC);m,_,l:=fenceFixture(now);m.Spec.Checkpoint.Checkpointable=false;l.Spec.Fence.ExpiresAt=metav1.NewTime(now.Add(-time.Minute));if err:=CanStartAttempt(m,l,nil,now);err==nil{t.Fatal("expired non-checkpointable attempt duplicated without trusted stop")};stop:=validObs(l,spacev1.ExecutionObservationStopped,"",now.Add(-2*time.Minute));if err:=CanStartAttempt(m,l,[]*spacev1.SpaceExecutionObservation{stop},now);err!=nil{t.Fatalf("trusted stop rejected: %v",err)}}

func TestCheckpointableMigrationRequiresSignedCheckpointThenStopOrExpiry(t *testing.T){now:=time.Date(2026,7,25,0,0,0,0,time.UTC);m,_,l:=fenceFixture(now);if err:=CanStartAttempt(m,l,nil,now);err==nil{t.Fatal("migration without checkpoint accepted")};checkpoint:=validObs(l,spacev1.ExecutionObservationCheckpointed,"cp-1",now);if err:=CanStartAttempt(m,l,[]*spacev1.SpaceExecutionObservation{checkpoint},now);err==nil{t.Fatal("live checkpointed lease migrated without stop/expiry")};l.Spec.Fence.ExpiresAt=metav1.NewTime(now.Add(-time.Minute));checkpoint.Spec.ObservedAt=metav1.NewTime(now.Add(-2*time.Minute));if err:=CanStartAttempt(m,l,[]*spacev1.SpaceExecutionObservation{checkpoint},now);err!=nil{t.Fatalf("expired checkpointed lease should migrate: %v",err)}}

func TestOldTokenReportRejected(t *testing.T){now:=time.Date(2026,7,25,0,0,0,0,time.UTC);_,_,l:=fenceFixture(now);token,hash,err:=NewFenceToken();if err!=nil{t.Fatal(err)};l.Spec.Fence.TokenHash=hash;if err:=ValidateReport(Report{MissionUID:l.Spec.Fence.MissionUID,PlanID:l.Spec.Fence.PlanID,Attempt:1,LeaseEpoch:1,Token:token,Phase:spacev1.ExecutionObservationHeartbeat},l,now);err!=nil{t.Fatalf("current token rejected: %v",err)};oldToken,_,_:=NewFenceToken();if err:=ValidateReport(Report{MissionUID:l.Spec.Fence.MissionUID,PlanID:l.Spec.Fence.PlanID,Attempt:1,LeaseEpoch:1,Token:oldToken,Phase:spacev1.ExecutionObservationHeartbeat},l,now);err==nil{t.Fatal("old token heartbeat accepted")}}

func TestCanDispatchRequiresTransferReceiptComputeTimeAndLease(t *testing.T){now:=time.Date(2026,7,25,0,0,0,0,time.UTC);m,p,l:=fenceFixture(now);digest:=strings.Repeat("d",64);src:=spacev1.DomainReference{Name:"ground",ClusterID:"ground",OrbitClass:spacev1.OrbitGround};m.Spec.Inputs=[]spacev1.DataObject{{ID:"sensor",SizeBytes:10,Locations:[]string{"ground"},PayloadDigest:digest}};p.Spec.InputTransfers=[]spacev1.TransferEpoch{{DataID:"sensor",Source:src,Destination:p.Spec.Target,Start:metav1.NewTime(now.Add(-time.Minute)),End:metav1.NewTime(now),Bytes:10}};p.Spec.ComputeStart=metav1.NewTime(now.Add(time.Minute));p.Spec.NotBefore=p.Spec.ComputeStart;if err:=CanDispatch(m,p,l,nil,now);err==nil{t.Fatal("dispatch before compute start accepted")};p.Spec.ComputeStart=metav1.NewTime(now);p.Spec.NotBefore=p.Spec.ComputeStart;if err:=CanDispatch(m,p,l,nil,now);err==nil{t.Fatal("dispatch without transfer receipt accepted")};r:=&spacev1.SpaceTransferReceipt{Spec:spacev1.SpaceTransferReceiptSpec{TransferID:spacev1.InputTransferID(0,"sensor"),MissionUID:string(m.UID),PlanID:p.Spec.PlanID,Attempt:p.Spec.Attempt,Source:src,Destination:p.Spec.Target,DataID:"sensor",Bytes:10,PayloadDigest:digest,StartedAt:metav1.NewTime(now.Add(-time.Minute)),CompletedAt:metav1.NewTime(now),Provenance:prov()}};if err:=CanDispatch(m,p,l,[]*spacev1.SpaceTransferReceipt{r},now);err!=nil{t.Fatalf("complete dispatch evidence rejected: %v",err)}}

func fenceFixture(now time.Time)(*spacev1.SpaceMission,*spacev1.SpacePlacementIntent,*spacev1.SpaceExecutionLease){m:=&spacev1.SpaceMission{ObjectMeta:metav1.ObjectMeta{Name:"m",Namespace:"missions",UID:types.UID("mission-uid")},Spec:spacev1.SpaceMissionSpec{MissionClass:"science",Priority:1,StatePolicy:spacev1.PolicyStrict,RequiredCapabilities:[]spacev1.CapabilityRequirement{{Class:"gpu",Quantity:1}},Deadline:metav1.NewTime(now.Add(time.Hour)),ExpectedDurationSeconds:30,MaximumDurationSeconds:60,DurationUncertaintySecs:10,SafetyMarginSeconds:5,MaximumClockSkewSeconds:1,Retry:spacev1.RetryPolicy{MaxAttempts:3,AllowMigration:true,MaxConcurrentExecutions:1},Checkpoint:spacev1.CheckpointPolicy{Checkpointable:true}}};target:=spacev1.DomainReference{Name:"leo",ClusterID:"leo",OrbitClass:spacev1.OrbitLEO};p:=&spacev1.SpacePlacementIntent{Spec:spacev1.SpacePlacementIntentSpec{MissionRef:coreRef(m),PlanID:"plan-one",Attempt:1,Target:target,NotBefore:metav1.NewTime(now),ComputeStart:metav1.NewTime(now),ComputeEnd:metav1.NewTime(now.Add(time.Minute)),ExpiresAt:metav1.NewTime(now.Add(20*time.Minute)),MaterialInputDigest:"x",SnapshotSequences:map[string]int64{}}};f:=spacev1.ExecutionFence{MissionUID:string(m.UID),PlanID:p.Spec.PlanID,Attempt:1,LeaseEpoch:1,TokenHash:strings.Repeat("b",64),ExpiresAt:metav1.NewTime(now.Add(5*time.Minute))};l:=&spacev1.SpaceExecutionLease{ObjectMeta:metav1.ObjectMeta{Name:spacev1.ExecutionLeaseName(f.MissionUID,f.PlanID,1,1)},Spec:spacev1.SpaceExecutionLeaseSpec{Source:target,Destination:target,Fence:f,HeartbeatAt:metav1.NewTime(now),MaximumClockSkewSeconds:1,Provenance:prov()}};return m,p,l}
func coreRef(m *spacev1.SpaceMission) corev1.ObjectReference{return corev1.ObjectReference{Namespace:m.Namespace,Name:m.Name,UID:m.UID}}
func prov()spacev1.Provenance{return spacev1.Provenance{ReporterID:"reporter",Source:"agent",Digest:strings.Repeat("a",64),Sequence:1}}
func validObs(l *spacev1.SpaceExecutionLease,phase spacev1.ExecutionObservationPhase,checkpoint string,at time.Time)*spacev1.SpaceExecutionObservation{f:=l.Spec.Fence;o:=&spacev1.SpaceExecutionObservation{Spec:spacev1.SpaceExecutionObservationSpec{ObservationID:"obs",MissionUID:f.MissionUID,PlanID:f.PlanID,Attempt:f.Attempt,LeaseEpoch:f.LeaseEpoch,TokenHash:f.TokenHash,Source:l.Spec.Source,Destination:l.Spec.Destination,Phase:phase,CheckpointID:checkpoint,ObservedAt:metav1.NewTime(at),Provenance:prov()}};o.Name=spacev1.ExecutionObservationName(o.Spec.Source,o.Spec.Destination,o.Spec.MissionUID,o.Spec.PlanID,o.Spec.ObservationID);return o}
''')
# execution test needs corev1 import.
p=Path('contrib/space-compute/pkg/execution/fence_test.go');text=p.read_text().replace('    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"','    corev1 "k8s.io/api/core/v1"\n    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"');p.write_text(text)

write('contrib/space-compute/pkg/transport/transport_test.go', r'''package transport

import (
    "crypto/ed25519"
    "crypto/rand"
    "crypto/sha256"
    "encoding/hex"
    "os"
    "path/filepath"
    "strings"
    "testing"
    "time"

    spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

func TestEnvelopeSignatureDigestExpiry(t *testing.T){pub,priv,_:=ed25519.GenerateKey(rand.Reader);src,dst:=domains();now:=time.Date(2026,7,25,0,0,0,0,time.UTC);e:=NewEnvelope("env-1","test",src,dst,"m","plan",1,1,now,now.Add(time.Minute),[]byte("payload"));if err:=e.Sign(priv);err!=nil{t.Fatal(err)};if err:=e.Verify(pub,now,DefaultLimits());err!=nil{t.Fatalf("valid envelope: %v",err)};e.Payload=[]byte("forged");if err:=e.Verify(pub,now,DefaultLimits());err==nil{t.Fatal("payload forgery accepted")};e.Payload=[]byte("payload");if err:=e.Verify(pub,now.Add(5*time.Minute),DefaultLimits());err==nil{t.Fatal("expired envelope accepted")}}

func TestDiskQueuePersistsBoundsAndIdempotency(t *testing.T){limits:=DefaultLimits();limits.MaxQueueItems=1;limits.MaxQueueBytes=2<<20;dir:=t.TempDir();q,err:=OpenDiskQueue(dir,limits);if err!=nil{t.Fatal(err)};src,dst:=domains();now:=time.Now().UTC();e:=NewEnvelope("same","test",src,dst,"m","plan",1,1,now,now.Add(time.Hour),[]byte("x"));if err:=q.Enqueue(e);err!=nil{t.Fatal(err)};if err:=q.Enqueue(e);err!=nil{t.Fatalf("idempotent enqueue: %v",err)};e2:=NewEnvelope("second","test",src,dst,"m","plan",1,1,now,now.Add(time.Hour),[]byte("y"));if err:=q.Enqueue(e2);err==nil{t.Fatal("queue item bound not enforced")};q2,err:=OpenDiskQueue(dir,limits);if err!=nil{t.Fatal(err)};due,err:=q2.Due(now.Add(time.Second),1);if err!=nil||len(due)!=1||due[0].Envelope.ID!="same"{t.Fatalf("restart recovery due=%v err=%v",due,err)}}

func TestDedupePersistsAcrossRestartAndFailedHandlerCanRetry(t *testing.T){path:=filepath.Join(t.TempDir(),"dedupe.json");d,err:=OpenDedupeStore(path,time.Hour);if err!=nil{t.Fatal(err)};now:=time.Now().UTC();seen,_:=d.Seen("e",1,now);if seen{t.Fatal("new envelope already seen")};if err:=d.Record("e",1,now);err!=nil{t.Fatal(err)};d2,_:=OpenDedupeStore(path,time.Hour);seen,_=d2.Seen("e",1,now);if !seen{t.Fatal("dedupe did not persist")};seen,_=d2.Seen("retry",1,now);if seen{t.Fatal("failed/unrecorded handler would be suppressed")}}

func TestFileAssemblerPersistsAndVerifiesDigest(t *testing.T){root:=t.TempDir();payload:=[]byte(strings.Repeat("0123456789",1000));sum:=sha256.Sum256(payload);digest:=hex.EncodeToString(sum[:]);src,dst:=domains();intent:=&spacev1.SpaceTransferIntent{ObjectMeta:metav1.ObjectMeta{Name:"intent"},Spec:spacev1.SpaceTransferIntentSpec{TransferID:"transfer-one",MissionUID:"m",PlanID:"plan",Attempt:1,Purpose:spacev1.TransferPurposeInput,Source:src,Destination:dst,DataID:"sensor",Bytes:int64(len(payload)),PayloadDigest:digest,ExpiresAt:metav1.NewTime(time.Now().Add(time.Hour))}};source:=t.TempDir();if err:=os.WriteFile(filepath.Join(source,"sensor"),payload,0600);err!=nil{t.Fatal(err)};chunks,err:=ReadChunks(intent,source,2048);if err!=nil{t.Fatal(err)};assembler:=&FileAssembler{Root:root,MaxBytes:1<<20};for i,c:=range chunks{complete,ack,err:=assembler.Accept(c);if err!=nil{t.Fatal(err)};if i<len(chunks)-1&&complete{t.Fatal("completed before all chunks")};if i==len(chunks)-1{if !complete||ack==nil||ack.PayloadDigest!=digest{t.Fatalf("final complete=%v ack=%v",complete,ack)}}};stored,err:=os.ReadFile(filepath.Join(root,"sensor"));if err!=nil||string(stored)!=string(payload){t.Fatalf("assembled payload mismatch err=%v",err)}}

func domains()(spacev1.DomainReference,spacev1.DomainReference){return spacev1.DomainReference{Name:"ground",ClusterID:"g",OrbitClass:spacev1.OrbitGround},spacev1.DomainReference{Name:"leo",ClusterID:"l",OrbitClass:spacev1.OrbitLEO}}
''')
# Add metav1 import.
p=Path('contrib/space-compute/pkg/transport/transport_test.go');text=p.read_text().replace('    "time"\n\n    spacev1','    "time"\n\n    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"\n\n    spacev1');p.write_text(text)
print('stage5 core transport/execution tests applied')
