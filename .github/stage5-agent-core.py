#!/usr/bin/env python3
from pathlib import Path

def read(path): return Path(path).read_text()
def write(path,text):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True);p.write_text(text)
def replace_once(path,old,new):
    text=read(path)
    if text.count(old)!=1: raise SystemExit(f'{path}: marker count {text.count(old)} for {old[:80]!r}')
    write(path,text.replace(old,new,1))

# Extend desired transfer API so input and result transfers share one durable
# transport primitive without overloading annotations.
replace_once('contrib/space-compute/pkg/apis/v1alpha1/phase5_transport.go',
'''const (
\tTransferIntentPending   = "Pending"
\tTransferIntentSending   = "Sending"
\tTransferIntentCompleted = "Completed"
\tTransferIntentFailed    = "Failed"
)''',
'''const (
\tTransferIntentPending   = "Pending"
\tTransferIntentSending   = "Sending"
\tTransferIntentCompleted = "Completed"
\tTransferIntentFailed    = "Failed"
\tTransferPurposeInput    = "Input"
\tTransferPurposeResult   = "Result"
)''')
replace_once('contrib/space-compute/pkg/apis/v1alpha1/phase5_transport.go',
'''type SpaceTransferIntentSpec struct {
\tTransferID    string          `json:"transferID"`
\tMissionUID    string          `json:"missionUID"`
\tPlanID        string          `json:"planID"`
\tAttempt       int32           `json:"attempt"`
\tSource        DomainReference `json:"source"`
\tDestination   DomainReference `json:"destination"`
\tDataID        string          `json:"dataID"`
\tBytes         int64           `json:"bytes"`
\tPayloadDigest string          `json:"payloadDigest"`
\tWindow        TransferEpoch   `json:"window"`
\tExpiresAt     metav1.Time     `json:"expiresAt"`
}''',
'''type SpaceTransferIntentSpec struct {
\tTransferID    string          `json:"transferID"`
\tMissionUID    string          `json:"missionUID"`
\tPlanID        string          `json:"planID"`
\tAttempt       int32           `json:"attempt"`
\tPurpose       string          `json:"purpose"`
\tSource        DomainReference `json:"source"`
\tDestination   DomainReference `json:"destination"`
\tDataID        string          `json:"dataID"`
\tBytes         int64           `json:"bytes"`
\tPayloadDigest string          `json:"payloadDigest"`
\tLeaseEpoch    int64           `json:"leaseEpoch,omitempty"`
\tTokenHash     string          `json:"tokenHash,omitempty"`
\tWindow        TransferEpoch   `json:"window"`
\tExpiresAt     metav1.Time     `json:"expiresAt"`
}''')
# Insert exported deterministic IDs used identically by planner/workload/agent.
text=read('contrib/space-compute/pkg/apis/v1alpha1/phase5_transport.go')
marker='''func TransferIntentName(source, destination DomainReference, missionUID, planID, transferID string) string {'''
extra='''func InputTransferID(index int, dataID string) string {\n\tvalue := strings.ToLower(strings.TrimSpace(dataID))\n\tvar b strings.Builder\n\tfor _, r := range value { if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' { b.WriteRune(r) } else { b.WriteByte('-') } }\n\tvalue = strings.Trim(b.String(), "-")\n\tif value == "" { value = "data" }; if len(value) > 40 { value = value[:40] }\n\treturn fmt.Sprintf("input-%d-%s", index+1, value)\n}\n\nfunc ResultTransferID(attempt int32) string { return fmt.Sprintf("result-attempt-%d", attempt) }\n\n'''
if text.count(marker)!=1: raise SystemExit('transfer name marker mismatch')
write('contrib/space-compute/pkg/apis/v1alpha1/phase5_transport.go',text.replace(marker,extra+marker,1))

# Transfer intent validation now understands purpose/fence.
replace_once('contrib/space-compute/pkg/apis/v1alpha1/phase5_validation.go',
'''\tvalidateReceiptIdentity("spec.transferID", intent.Spec.TransferID, &errs)
\tvalidateReceiptCommon(intent.Spec.MissionUID, intent.Spec.PlanID, intent.Spec.Attempt, intent.Spec.Source, intent.Spec.Destination, intent.Spec.Bytes, intent.Spec.PayloadDigest, Provenance{ReporterID: "local", Source: "intent", Digest: strings.Repeat("0", 64), Sequence: 1}, &errs)''',
'''\tvalidateReceiptIdentity("spec.transferID", intent.Spec.TransferID, &errs)
\tvalidateReceiptCommon(intent.Spec.MissionUID, intent.Spec.PlanID, intent.Spec.Attempt, intent.Spec.Source, intent.Spec.Destination, intent.Spec.Bytes, intent.Spec.PayloadDigest, Provenance{ReporterID: "local", Source: "intent", Digest: strings.Repeat("0", 64), Sequence: 1}, &errs)
\tswitch intent.Spec.Purpose { case TransferPurposeInput: if intent.Spec.LeaseEpoch != 0 || intent.Spec.TokenHash != "" { errs.add("spec", "input transfer cannot carry execution fence") }; case TransferPurposeResult: if intent.Spec.LeaseEpoch < 1 { errs.add("spec.leaseEpoch", "result transfer requires a positive lease epoch") }; validateLowerSHA256("spec.tokenHash", intent.Spec.TokenHash, &errs); default: errs.add("spec.purpose", "must be Input or Result") }''')
# Allow self-domain lease/observation for local execution. Admission below avoids requiring self in peer list.
replace_once('contrib/space-compute/pkg/apis/v1alpha1/phase5_validation.go',
'''\tif lease.Spec.Source == lease.Spec.Destination {
\t\terrs.add("spec.destination", "must differ from source")
\t}''','''\t// A local-domain execution still requires a fence. source==destination is
\t// therefore valid for locally issued leases and never bypasses signature or
\t// epoch/token validation.''')

# Admission treats self-domain lease/observation as no cross-domain peer hop.
replace_once('contrib/space-compute/pkg/admission/validator.go',
'''\t\tdestination := value.Spec.Destination
\t\treturn &reporterEnvelope{kind: "SpaceExecutionLease", name: value.Name, provenance: &value.Spec.Provenance, source: value.Spec.Source, destination: &destination, digestObject: value, observedAtNano: value.Spec.HeartbeatAt.UnixNano(), identity: normalizedEnvelopeIdentity(value.Spec.Source, &value.Spec.Destination, fmt.Sprintf("%s|%s|%d|%d", value.Spec.Fence.MissionUID, value.Spec.Fence.PlanID, value.Spec.Fence.Attempt, value.Spec.Fence.LeaseEpoch))}, nil''',
'''\t\tdestination := value.Spec.Destination
\t\tvar peer *spacev1.DomainReference
\t\tif destination != value.Spec.Source { peer = &destination }
\t\treturn &reporterEnvelope{kind: "SpaceExecutionLease", name: value.Name, provenance: &value.Spec.Provenance, source: value.Spec.Source, destination: peer, digestObject: value, observedAtNano: value.Spec.HeartbeatAt.UnixNano(), identity: normalizedEnvelopeIdentity(value.Spec.Source, &value.Spec.Destination, fmt.Sprintf("%s|%s|%d|%d", value.Spec.Fence.MissionUID, value.Spec.Fence.PlanID, value.Spec.Fence.Attempt, value.Spec.Fence.LeaseEpoch))}, nil''')
replace_once('contrib/space-compute/pkg/admission/validator.go',
'''\t\tdestination := value.Spec.Destination
\t\treturn &reporterEnvelope{kind: "SpaceExecutionObservation", name: value.Name, provenance: &value.Spec.Provenance, source: value.Spec.Source, destination: &destination, digestObject: value, observedAtNano: value.Spec.ObservedAt.UnixNano(), identity: normalizedEnvelopeIdentity(value.Spec.Source, &value.Spec.Destination, fmt.Sprintf("%s|%s|%d|%d|%s", value.Spec.MissionUID, value.Spec.PlanID, value.Spec.Attempt, value.Spec.LeaseEpoch, value.Spec.ObservationID))}, nil''',
'''\t\tdestination := value.Spec.Destination
\t\tvar peer *spacev1.DomainReference
\t\tif destination != value.Spec.Source { peer = &destination }
\t\treturn &reporterEnvelope{kind: "SpaceExecutionObservation", name: value.Name, provenance: &value.Spec.Provenance, source: value.Spec.Source, destination: peer, digestObject: value, observedAtNano: value.Spec.ObservedAt.UnixNano(), identity: normalizedEnvelopeIdentity(value.Spec.Source, &value.Spec.Destination, fmt.Sprintf("%s|%s|%d|%d|%s", value.Spec.MissionUID, value.Spec.PlanID, value.Spec.Attempt, value.Spec.LeaseEpoch, value.Spec.ObservationID))}, nil''')

# Queue enqueue is idempotent and collision-safe across controller retries.
path=Path('contrib/space-compute/pkg/transport/spool.go');text=path.read_text()
old='''func (q *DiskQueue) Enqueue(e *Envelope) error {
\tq.mu.Lock()
\tdefer q.mu.Unlock()
\traw, err := json.Marshal(e)
\tif err != nil {
\t\treturn err
\t}
\tif int64(len(raw)) > q.limits.MaxMessageBytes {
\t\treturn fmt.Errorf("serialized envelope exceeds message bound")
\t}
\tfiles, bytes, err := q.statsLocked()'''
new='''func (q *DiskQueue) Enqueue(e *Envelope) error {
\tq.mu.Lock()
\tdefer q.mu.Unlock()
\traw, err := json.Marshal(e)
\tif err != nil { return err }
\tif int64(len(raw)) > q.limits.MaxMessageBytes { return fmt.Errorf("serialized envelope exceeds message bound") }
\ttarget := q.path(e.ID, e.Sequence)
\tif _, statErr := os.Stat(target); statErr == nil {
\t\tvar existing queuedEnvelope
\t\tif err := readJSON(target, &existing); err != nil { return err }
\t\toldRaw, _ := json.Marshal(&existing.Envelope)
\t\tif bytes.Equal(oldRaw, raw) { return nil }
\t\treturn fmt.Errorf("envelope identity collision for %s sequence %d", e.ID, e.Sequence)
\t} else if !os.IsNotExist(statErr) { return statErr }
\tfiles, bytesUsed, err := q.statsLocked()'''
if text.count(old)!=1: raise SystemExit('queue enqueue marker mismatch')
text=text.replace(old,new,1).replace('''\tif files >= q.limits.MaxQueueItems || bytes+int64(len(raw)) > q.limits.MaxQueueBytes {''','''\tif files >= q.limits.MaxQueueItems || bytesUsed+int64(len(raw)) > q.limits.MaxQueueBytes {''',1).replace('''\treturn writeAtomic(q.path(e.ID, e.Sequence), item)''','''\treturn writeAtomic(target, item)''',1)
text=text.replace('import (\n\t"crypto/sha256"','import (\n\t"bytes"\n\t"crypto/sha256"',1)
path.write_text(text)

write('contrib/space-compute/pkg/transport/chunks.go', r'''package transport

import (
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "io"
    "os"
    "path/filepath"
    "sort"
    "strings"
    "sync"
    "time"

    spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

const TransferChunkKind = "transfer-chunk"
const TransferAckKind = "transfer-ack"
const ReporterObjectKind = "reporter-object"
const LeaseRequestKind = "lease-request"
const LeaseGrantKind = "lease-grant"

type TransferChunk struct {
    IntentName string `json:"intentName"`
    TransferID string `json:"transferID"`
    MissionUID string `json:"missionUID"`
    PlanID string `json:"planID"`
    Attempt int32 `json:"attempt"`
    Purpose string `json:"purpose"`
    Source spacev1.DomainReference `json:"source"`
    Destination spacev1.DomainReference `json:"destination"`
    DataID string `json:"dataID"`
    TotalBytes int64 `json:"totalBytes"`
    PayloadDigest string `json:"payloadDigest"`
    LeaseEpoch int64 `json:"leaseEpoch,omitempty"`
    TokenHash string `json:"tokenHash,omitempty"`
    ChunkIndex int32 `json:"chunkIndex"`
    ChunkCount int32 `json:"chunkCount"`
    Offset int64 `json:"offset"`
    StartedAt time.Time `json:"startedAt"`
    Data []byte `json:"data"`
}

type TransferAck struct {
    IntentName string `json:"intentName"`
    TransferID string `json:"transferID"`
    MissionUID string `json:"missionUID"`
    PlanID string `json:"planID"`
    Attempt int32 `json:"attempt"`
    Purpose string `json:"purpose"`
    Source spacev1.DomainReference `json:"source"`
    Destination spacev1.DomainReference `json:"destination"`
    DataID string `json:"dataID"`
    Bytes int64 `json:"bytes"`
    PayloadDigest string `json:"payloadDigest"`
    LeaseEpoch int64 `json:"leaseEpoch,omitempty"`
    TokenHash string `json:"tokenHash,omitempty"`
    StartedAt time.Time `json:"startedAt"`
    CompletedAt time.Time `json:"completedAt"`
}

type assemblyState struct {
    TransferID string `json:"transferID"`; DataID string `json:"dataID"`; TotalBytes int64 `json:"totalBytes"`; PayloadDigest string `json:"payloadDigest"`; ChunkCount int32 `json:"chunkCount"`; Seen []int32 `json:"seen"`; StartedAt time.Time `json:"startedAt"`
}

type FileAssembler struct { mu sync.Mutex; Root string; MaxBytes int64 }
func DataPath(root,id string)(string,error){id=strings.TrimSpace(id);if id==""||strings.Contains(id,"/")||strings.Contains(id,"\\")||id=="."||id==".."{return "",fmt.Errorf("unsafe data ID")};root=filepath.Clean(root);path:=filepath.Join(root,id);if filepath.Dir(path)!=root{return "",fmt.Errorf("data path escapes root")};return path,nil}
func (a *FileAssembler) Accept(chunk TransferChunk)(bool,*TransferAck,error){a.mu.Lock();defer a.mu.Unlock();if a.MaxBytes<=0{a.MaxBytes=1<<40};if chunk.TotalBytes<0||chunk.TotalBytes>a.MaxBytes||chunk.ChunkCount<1||chunk.ChunkCount>1_000_000||chunk.ChunkIndex<0||chunk.ChunkIndex>=chunk.ChunkCount||chunk.Offset<0||chunk.Offset+int64(len(chunk.Data))>chunk.TotalBytes{return false,nil,fmt.Errorf("invalid transfer chunk bounds")};dest,err:=DataPath(a.Root,chunk.DataID);if err!=nil{return false,nil,err};incoming:=filepath.Join(a.Root,".incoming");if err:=os.MkdirAll(incoming,0700);err!=nil{return false,nil,err};sum:=sha256.Sum256([]byte(chunk.TransferID));key:=hex.EncodeToString(sum[:12]);part:=filepath.Join(incoming,key+".part");statePath:=filepath.Join(incoming,key+".json");state:=assemblyState{TransferID:chunk.TransferID,DataID:chunk.DataID,TotalBytes:chunk.TotalBytes,PayloadDigest:chunk.PayloadDigest,ChunkCount:chunk.ChunkCount,StartedAt:chunk.StartedAt.UTC()};if raw,err:=os.ReadFile(statePath);err==nil{if err:=json.Unmarshal(raw,&state);err!=nil{return false,nil,err};if state.TransferID!=chunk.TransferID||state.DataID!=chunk.DataID||state.TotalBytes!=chunk.TotalBytes||state.PayloadDigest!=chunk.PayloadDigest||state.ChunkCount!=chunk.ChunkCount{return false,nil,fmt.Errorf("transfer assembly identity changed")}}
    seen:=map[int32]struct{}{};for _,i:=range state.Seen{seen[i]=struct{}{}};if _,ok:=seen[chunk.ChunkIndex];!ok{f,err:=os.OpenFile(part,os.O_CREATE|os.O_RDWR,0600);if err!=nil{return false,nil,err};if err:=f.Truncate(chunk.TotalBytes);err==nil{_,err=f.WriteAt(chunk.Data,chunk.Offset)};if err==nil{err=f.Sync()};_ = f.Close();if err!=nil{return false,nil,err};seen[chunk.ChunkIndex]=struct{}{};state.Seen=state.Seen[:0];for i:=range seen{state.Seen=append(state.Seen,i)};sort.Slice(state.Seen,func(i,j int)bool{return state.Seen[i]<state.Seen[j]});if err:=writeAtomic(statePath,state);err!=nil{return false,nil,err}}
    if len(seen)!=int(chunk.ChunkCount){return false,nil,nil};f,err:=os.Open(part);if err!=nil{return false,nil,err};h:=sha256.New();n,err:=io.Copy(h,f);_ = f.Close();if err!=nil{return false,nil,err};if n!=chunk.TotalBytes||hex.EncodeToString(h.Sum(nil))!=chunk.PayloadDigest{return false,nil,fmt.Errorf("assembled payload digest/size mismatch")};if err:=os.MkdirAll(filepath.Dir(dest),0700);err!=nil{return false,nil,err};if err:=os.Rename(part,dest);err!=nil{return false,nil,err};_ = os.Remove(statePath);return true,&TransferAck{IntentName:chunk.IntentName,TransferID:chunk.TransferID,MissionUID:chunk.MissionUID,PlanID:chunk.PlanID,Attempt:chunk.Attempt,Purpose:chunk.Purpose,Source:chunk.Destination,Destination:chunk.Source,DataID:chunk.DataID,Bytes:chunk.TotalBytes,PayloadDigest:chunk.PayloadDigest,LeaseEpoch:chunk.LeaseEpoch,TokenHash:chunk.TokenHash,StartedAt:chunk.StartedAt.UTC(),CompletedAt:time.Now().UTC()},nil}

func ReadChunks(intent *spacev1.SpaceTransferIntent,root string,maxChunk int)([]TransferChunk,error){if intent==nil{return nil,fmt.Errorf("transfer intent required")};if maxChunk<1024{return nil,fmt.Errorf("chunk size too small")};path,err:=DataPath(root,intent.Spec.DataID);if err!=nil{return nil,err};raw,err:=os.ReadFile(path);if err!=nil{return nil,err};if int64(len(raw))!=intent.Spec.Bytes{return nil,fmt.Errorf("source data size does not match transfer intent")};sum:=sha256.Sum256(raw);if hex.EncodeToString(sum[:])!=intent.Spec.PayloadDigest{return nil,fmt.Errorf("source data digest does not match transfer intent")};count:=(len(raw)+maxChunk-1)/maxChunk;if count==0{count=1};started:=time.Now().UTC();out:=make([]TransferChunk,0,count);for i:=0;i<count;i++{start:=i*maxChunk;end:=start+maxChunk;if end>len(raw){end=len(raw)};data:=append([]byte(nil),raw[start:end]...);out=append(out,TransferChunk{IntentName:intent.Name,TransferID:intent.Spec.TransferID,MissionUID:intent.Spec.MissionUID,PlanID:intent.Spec.PlanID,Attempt:intent.Spec.Attempt,Purpose:intent.Spec.Purpose,Source:intent.Spec.Source,Destination:intent.Spec.Destination,DataID:intent.Spec.DataID,TotalBytes:intent.Spec.Bytes,PayloadDigest:intent.Spec.PayloadDigest,LeaseEpoch:intent.Spec.LeaseEpoch,TokenHash:intent.Spec.TokenHash,ChunkIndex:int32(i),ChunkCount:int32(count),Offset:int64(start),StartedAt:started,Data:data})};return out,nil}
''')

write('contrib/space-compute/pkg/execution/dispatch.go', r'''package execution

import (
    "fmt"
    "time"

    spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

func CanDispatch(mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent, lease *spacev1.SpaceExecutionLease, receipts []*spacev1.SpaceTransferReceipt, now time.Time) error {
    if mission==nil||placement==nil||lease==nil{return fmt.Errorf("mission, placement and execution lease are required")};now=now.UTC();if !placement.Spec.ExpiresAt.After(now){return fmt.Errorf("placement expired")};dispatchAt:=placement.Spec.ComputeStart.Time.UTC();if placement.Spec.NotBefore.Time.After(dispatchAt){dispatchAt=placement.Spec.NotBefore.Time.UTC()};if now.Before(dispatchAt){return fmt.Errorf("compute start has not arrived")};if err:=ValidateLease(lease,now);err!=nil{return err};f:=lease.Spec.Fence;if f.MissionUID!=string(mission.UID)||f.PlanID!=placement.Spec.PlanID||f.Attempt!=placement.Spec.Attempt||lease.Spec.Source!=placement.Spec.Target{return fmt.Errorf("execution lease does not fence placement")}
    for i,epoch:=range placement.Spec.InputTransfers{digest:="";for _,input:=range mission.Spec.Inputs{if input.ID==epoch.DataID{digest=input.PayloadDigest;break}};if digest==""{return fmt.Errorf("input %q has no trusted payload digest",epoch.DataID)};transferID:=spacev1.InputTransferID(i,epoch.DataID);matched:=false;for _,receipt:=range receipts{if receipt==nil{continue};s:=receipt.Spec;if s.TransferID==transferID&&s.MissionUID==string(mission.UID)&&s.PlanID==placement.Spec.PlanID&&s.Attempt==placement.Spec.Attempt&&s.Source==epoch.Source&&s.Destination==epoch.Destination&&s.DataID==epoch.DataID&&s.Bytes==epoch.Bytes&&s.PayloadDigest==digest{matched=true;break}};if !matched{return fmt.Errorf("input %q has no matching trusted transfer receipt",epoch.DataID)}};return nil
}

type Report struct { MissionUID string `json:"missionUID"`; PlanID string `json:"planID"`; Attempt int32 `json:"attempt"`; LeaseEpoch int64 `json:"leaseEpoch"`; Token string `json:"token"`; Phase spacev1.ExecutionObservationPhase `json:"phase"`; CheckpointID string `json:"checkpointID,omitempty"`; ResultDataID string `json:"resultDataID,omitempty"` }
func ValidateReport(report Report,lease *spacev1.SpaceExecutionLease,now time.Time)error{if err:=ValidateLease(lease,now);err!=nil{return err};hash,err:=TokenHash(report.Token);if err!=nil{return err};f:=lease.Spec.Fence;if report.MissionUID!=f.MissionUID||report.PlanID!=f.PlanID||report.Attempt!=f.Attempt||report.LeaseEpoch!=f.LeaseEpoch||hash!=f.TokenHash{return fmt.Errorf("execution report uses stale or foreign fence token")};switch report.Phase{case spacev1.ExecutionObservationHeartbeat,spacev1.ExecutionObservationStopped,spacev1.ExecutionObservationFailed:if report.CheckpointID!=""||report.ResultDataID!=""{return fmt.Errorf("phase does not accept checkpoint/result payload")};case spacev1.ExecutionObservationCheckpointed:if report.CheckpointID==""{return fmt.Errorf("checkpointed report requires checkpointID")};case spacev1.ExecutionObservationCompleted:default:return fmt.Errorf("unsupported report phase")};return nil}
''')

# Workload controller uses shared deterministic transfer ID and distinguishes
# remote placement: it never creates a local Pod when LocalDomain is configured
# and target differs.
replace_once('contrib/space-compute/pkg/workload/controller.go','''type Controller struct {
\tStore    Store
\tEvidence EvidenceStore
\tClock    spacev1.Clock
}''','''type Controller struct {
\tStore       Store
\tEvidence    EvidenceStore
\tClock       spacev1.Clock
\tLocalDomain *spacev1.DomainReference
}''')
replace_once('contrib/space-compute/pkg/workload/controller.go','''\tfor i, epoch := range placement.Spec.InputTransfers {
\t\ttransferID := fmt.Sprintf("input-%d-%s", i+1, safeID(epoch.DataID))''','''\tfor i, epoch := range placement.Spec.InputTransfers {
\t\ttransferID := spacev1.InputTransferID(i, epoch.DataID)''')
replace_once('contrib/space-compute/pkg/workload/controller.go','''\tif now.Before(dispatchAt) {
\t\tplacement.Status.Phase = spacev1.PlacementExecutionLeasePending
\t\tif err := c.Store.UpdatePlacementStatus(ctx, placement); err != nil {''','''\tif now.Before(dispatchAt) {
\t\tplacement.Status.Phase = spacev1.PlacementExecutionLeasePending
\t\tif err := c.Store.UpdatePlacementStatus(ctx, placement); err != nil {''')
# Insert remote branch immediately before deterministic local Pod name.
text=read('contrib/space-compute/pkg/workload/controller.go');marker='''\tname := AttemptPodName(mission.Name, placement.Spec.Attempt)''';remote='''\tif c.LocalDomain != nil && placement.Spec.Target != *c.LocalDomain {
\t\t// Remote target execution is owned by the target domain-agent. The planner
\t\t// domain never creates a local shadow Pod for a remote placement.
\t\tplacement.Status.Phase = spacev1.PlacementDispatched
\t\tif err := c.Store.UpdatePlacementStatus(ctx, placement); err != nil { return 0, err }
\t\tc.Store.Event(ctx, mission.Namespace, mission.Name, "Normal", "RemoteMissionDispatched", fmt.Sprintf("remote attempt %d fenced by lease epoch %d; waiting for signed remote observation", placement.Spec.Attempt, lease.Spec.Fence.LeaseEpoch))
\t\treturn 0, nil
\t}

''';
if text.count(marker)!=1: raise SystemExit('local pod marker mismatch')
write('contrib/space-compute/pkg/workload/controller.go',text.replace(marker,remote+marker,1))
# safeID no longer used.
text=read('contrib/space-compute/pkg/workload/controller.go')
start=text.find('func safeID(')
if start>=0:
    end=text.find('\nfunc lookupInputDigest',start)
    if end<0: raise SystemExit('safeID terminator missing')
    text=text[:start]+text[end+1:]
write('contrib/space-compute/pkg/workload/controller.go',text)

print('stage5 domain agent core patch applied')
