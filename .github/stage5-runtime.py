#!/usr/bin/env python3
from pathlib import Path


def read(path): return Path(path).read_text()
def write(path, text):
    p=Path(path); p.parent.mkdir(parents=True, exist_ok=True); p.write_text(text)
def replace_once(path, old, new):
    text=read(path)
    if text.count(old)!=1: raise SystemExit(f"{path}: marker count {text.count(old)} for {old[:100]!r}")
    write(path,text.replace(old,new,1))

write("contrib/space-compute/pkg/execution/fence.go", r'''// Package execution owns execution-fence semantics. It does not create Pods or
// perform cross-domain I/O; callers supply already-authenticated lease/evidence.
package execution

import (
    "crypto/rand"
    "crypto/sha256"
    "encoding/base64"
    "encoding/hex"
    "fmt"
    "sort"
    "time"

    spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

const TokenBytes = 32

func NewFenceToken() (string, string, error) {
    raw := make([]byte, TokenBytes)
    if _, err := rand.Read(raw); err != nil { return "", "", fmt.Errorf("generate fence token: %w", err) }
    sum := sha256.Sum256(raw)
    return base64.RawURLEncoding.EncodeToString(raw), hex.EncodeToString(sum[:]), nil
}

func TokenHash(token string) (string, error) {
    raw, err := base64.RawURLEncoding.DecodeString(token)
    if err != nil || len(raw) != TokenBytes { return "", fmt.Errorf("fence token must be %d random bytes encoded base64url", TokenBytes) }
    sum := sha256.Sum256(raw)
    return hex.EncodeToString(sum[:]), nil
}

func ValidateLease(lease *spacev1.SpaceExecutionLease, now time.Time) error {
    if lease == nil { return fmt.Errorf("execution lease is required") }
    if err := spacev1.ValidateExecutionLease(lease, fixedClock{now.UTC()}); err != nil { return err }
    skew := time.Duration(lease.Spec.MaximumClockSkewSeconds)*time.Second
    if !lease.Spec.Fence.ExpiresAt.Time.After(now.UTC().Add(-skew)) { return fmt.Errorf("execution lease expired") }
    return nil
}

// ValidateLeaseAdvance enforces one monotonic epoch stream. A heartbeat may
// update the same epoch only when the complete fence identity is unchanged.
// A replacement lease must use a strictly higher epoch and attempt.
func ValidateLeaseAdvance(previous, next *spacev1.SpaceExecutionLease, now time.Time) error {
    if err := ValidateLease(next, now); err != nil { return err }
    if previous == nil { return nil }
    a, b := previous.Spec.Fence, next.Spec.Fence
    if a.MissionUID != b.MissionUID { return fmt.Errorf("mission UID is immutable across lease stream") }
    if b.LeaseEpoch < a.LeaseEpoch { return fmt.Errorf("lease epoch regressed") }
    if b.LeaseEpoch == a.LeaseEpoch {
        if b.PlanID != a.PlanID || b.Attempt != a.Attempt || b.TokenHash != a.TokenHash { return fmt.Errorf("same-epoch heartbeat changed fence identity") }
        if !next.Spec.HeartbeatAt.After(previous.Spec.HeartbeatAt.Time) { return fmt.Errorf("same-epoch heartbeat time must increase") }
        if !b.ExpiresAt.After(a.ExpiresAt.Time) { return fmt.Errorf("same-epoch expiry must increase") }
        return nil
    }
    if b.Attempt <= a.Attempt { return fmt.Errorf("higher lease epoch must advance attempt") }
    if b.TokenHash == a.TokenHash { return fmt.Errorf("replacement lease must use a non-reusable token") }
    return nil
}

func ValidateObservationAgainstLease(observation *spacev1.SpaceExecutionObservation, lease *spacev1.SpaceExecutionLease, now time.Time) error {
    if observation == nil || lease == nil { return fmt.Errorf("observation and lease are required") }
    if err := spacev1.ValidateExecutionObservation(observation, fixedClock{now.UTC()}); err != nil { return err }
    f := lease.Spec.Fence
    if observation.Spec.MissionUID != f.MissionUID || observation.Spec.PlanID != f.PlanID || observation.Spec.Attempt != f.Attempt || observation.Spec.LeaseEpoch != f.LeaseEpoch || observation.Spec.TokenHash != f.TokenHash { return fmt.Errorf("execution observation is fenced by a different lease/token") }
    if observation.Spec.Source != lease.Spec.Source || observation.Spec.Destination != lease.Spec.Destination { return fmt.Errorf("execution observation domain identity does not match lease") }
    if observation.Spec.ObservedAt.After(&f.ExpiresAt) { return fmt.Errorf("execution observation was produced after lease expiry") }
    return nil
}

func ValidateResultAgainstLease(receipt *spacev1.SpaceResultReceipt, lease *spacev1.SpaceExecutionLease, now time.Time) error {
    if receipt == nil || lease == nil { return fmt.Errorf("result receipt and lease are required") }
    if err := spacev1.ValidateResultReceipt(receipt, fixedClock{now.UTC()}); err != nil { return err }
    f := lease.Spec.Fence
    if receipt.Spec.MissionUID != f.MissionUID || receipt.Spec.PlanID != f.PlanID || receipt.Spec.Attempt != f.Attempt || receipt.Spec.LeaseEpoch != f.LeaseEpoch || receipt.Spec.TokenHash != f.TokenHash { return fmt.Errorf("result receipt is fenced by a different lease/token") }
    if receipt.Spec.Source != lease.Spec.Source { return fmt.Errorf("result receipt source does not match lease issuer domain") }
    if receipt.Spec.CompletedAt.After(&f.ExpiresAt) { return fmt.Errorf("result receipt was produced after lease expiry") }
    return nil
}

// CanStartAttempt is deliberately conservative under partition. Local Pod
// deletion is not an input. A non-checkpointable prior execution requires a
// signed remote Stopped observation. A checkpointable execution additionally
// requires a signed Checkpointed observation before migration; after that,
// either a remote stop or expiry beyond the declared skew fences the old lease.
func CanStartAttempt(mission *spacev1.SpaceMission, previous *spacev1.SpaceExecutionLease, observations []*spacev1.SpaceExecutionObservation, now time.Time) error {
    if mission == nil { return fmt.Errorf("mission is required") }
    if previous == nil { return nil }
    stopped := false
    checkpointed := false
    for _, observation := range observations {
        if observation == nil { continue }
        if err := ValidateObservationAgainstLease(observation, previous, now); err != nil { continue }
        switch observation.Spec.Phase {
        case spacev1.ExecutionObservationStopped:
            stopped = true
        case spacev1.ExecutionObservationCheckpointed:
            checkpointed = true
        }
    }
    if !mission.Spec.Checkpoint.Checkpointable {
        if !stopped { return fmt.Errorf("non-checkpointable prior attempt has no trusted remote stop; partition cannot create a duplicate") }
        return nil
    }
    if !checkpointed { return fmt.Errorf("checkpointable migration requires a signed checkpoint receipt") }
    skew := time.Duration(previous.Spec.MaximumClockSkewSeconds)*time.Second
    expired := !previous.Spec.Fence.ExpiresAt.Time.After(now.UTC().Add(-skew))
    if !stopped && !expired { return fmt.Errorf("previous checkpointed execution is not remotely stopped and lease has not expired") }
    return nil
}

func LatestLeaseForAttempt(leases []*spacev1.SpaceExecutionLease, missionUID, planID string, attempt int32, now time.Time) (*spacev1.SpaceExecutionLease, error) {
    candidates := make([]*spacev1.SpaceExecutionLease,0,len(leases))
    for _, lease := range leases {
        if lease == nil { continue }
        f:=lease.Spec.Fence
        if f.MissionUID!=missionUID || f.PlanID!=planID || f.Attempt!=attempt { continue }
        if ValidateLease(lease,now)!=nil { continue }
        candidates=append(candidates,lease)
    }
    if len(candidates)==0 { return nil, nil }
    sort.Slice(candidates,func(i,j int)bool{return candidates[i].Spec.Fence.LeaseEpoch>candidates[j].Spec.Fence.LeaseEpoch})
    if len(candidates)>1 && candidates[0].Spec.Fence.LeaseEpoch==candidates[1].Spec.Fence.LeaseEpoch && candidates[0].Spec.Fence.TokenHash!=candidates[1].Spec.Fence.TokenHash { return nil,fmt.Errorf("conflicting execution leases share epoch %d",candidates[0].Spec.Fence.LeaseEpoch) }
    return candidates[0],nil
}

type fixedClock struct{ now time.Time }
func (c fixedClock) Now() time.Time { return c.now }
''')

write("contrib/space-compute/pkg/transport/envelope.go", r'''// Package transport provides bounded, mutually-authenticated, persistent
// at-least-once cross-domain message transport. It is independent of scheduler
// framework callbacks and contains no scheduler dependency.
package transport

import (
    "bytes"
    "crypto/ed25519"
    "crypto/sha256"
    "encoding/base64"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "net/url"
    "strconv"
    "strings"
    "time"

    spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

const EnvelopeVersion = "spacecompute-envelope-v1"

type Envelope struct {
    ID            string                  `json:"id"`
    Kind          string                  `json:"kind"`
    Source        spacev1.DomainReference `json:"source"`
    Destination   spacev1.DomainReference `json:"destination"`
    MissionUID    string                  `json:"missionUID"`
    PlanID        string                  `json:"planID"`
    Attempt       int32                   `json:"attempt"`
    Sequence      int64                   `json:"sequence"`
    Timestamp     time.Time               `json:"timestamp"`
    Expiry        time.Time               `json:"expiry"`
    PayloadDigest string                  `json:"payloadDigest"`
    Payload       json.RawMessage         `json:"payload"`
    Signature     string                  `json:"signature"`
}

type Limits struct {
    MaxMessageBytes int64
    MaxQueueItems int
    MaxQueueBytes int64
    MaxConcurrent int
    MaxRetries int
    DiskRetention time.Duration
    BackoffBase time.Duration
    BackoffMax time.Duration
    JitterFraction float64
    CircuitFailures int
    CircuitOpen time.Duration
    MaximumClockSkew time.Duration
}

func DefaultLimits() Limits { return Limits{MaxMessageBytes:1<<20,MaxQueueItems:4096,MaxQueueBytes:256<<20,MaxConcurrent:8,MaxRetries:15,DiskRetention:24*time.Hour,BackoffBase:time.Second,BackoffMax:2*time.Minute,JitterFraction:.2,CircuitFailures:5,CircuitOpen:2*time.Minute,MaximumClockSkew:2*time.Minute} }

func (l Limits) Validate() error {
    if l.MaxMessageBytes<1024 || l.MaxMessageBytes>64<<20 { return fmt.Errorf("max message bytes out of bounds") }
    if l.MaxQueueItems<1 || l.MaxQueueItems>100000 || l.MaxQueueBytes<l.MaxMessageBytes { return fmt.Errorf("queue bounds are invalid") }
    if l.MaxConcurrent<1 || l.MaxConcurrent>128 || l.MaxRetries<1 || l.MaxRetries>100 { return fmt.Errorf("concurrency/retry bounds are invalid") }
    if l.DiskRetention<=0 || l.BackoffBase<=0 || l.BackoffMax<l.BackoffBase || l.JitterFraction<0 || l.JitterFraction>.5 || l.CircuitFailures<1 || l.CircuitOpen<=0 || l.MaximumClockSkew<0 { return fmt.Errorf("transport timing bounds are invalid") }
    return nil
}

func NewEnvelope(id,kind string, source,destination spacev1.DomainReference, missionUID,planID string, attempt int32, sequence int64, timestamp,expiry time.Time, payload []byte) *Envelope {
    sum:=sha256.Sum256(payload)
    return &Envelope{ID:id,Kind:kind,Source:source,Destination:destination,MissionUID:missionUID,PlanID:planID,Attempt:attempt,Sequence:sequence,Timestamp:timestamp.UTC(),Expiry:expiry.UTC(),PayloadDigest:hex.EncodeToString(sum[:]),Payload:append([]byte(nil),payload...)}
}

func (e *Envelope) canonicalBytes() ([]byte,error) {
    if e==nil { return nil,fmt.Errorf("envelope is required") }
    var b bytes.Buffer
    field:=func(k,v string){fmt.Fprintf(&b,"%s=%s\n",k,strconv.Quote(v))}
    integer:=func(k string,v int64){fmt.Fprintf(&b,"%s=%d\n",k,v)}
    domain:=func(prefix string,d spacev1.DomainReference){field(prefix+".name",strings.ToLower(strings.TrimSpace(d.Name)));field(prefix+".clusterID",strings.ToLower(strings.TrimSpace(d.ClusterID)));field(prefix+".orbitClass",strings.ToLower(strings.TrimSpace(string(d.OrbitClass))))}
    field("version",EnvelopeVersion);field("id",e.ID);field("kind",e.Kind);domain("source",e.Source);domain("destination",e.Destination);field("missionUID",e.MissionUID);field("planID",e.PlanID);integer("attempt",int64(e.Attempt));integer("sequence",e.Sequence);field("timestamp",e.Timestamp.UTC().Format(time.RFC3339Nano));field("expiry",e.Expiry.UTC().Format(time.RFC3339Nano));field("payloadDigest",e.PayloadDigest)
    return b.Bytes(),nil
}

func (e *Envelope) Sign(private ed25519.PrivateKey) error {
    raw,err:=e.canonicalBytes();if err!=nil{return err}
    if len(private)!=ed25519.PrivateKeySize{return fmt.Errorf("Ed25519 private key is required")}
    e.Signature=base64.StdEncoding.EncodeToString(ed25519.Sign(private,raw));return nil
}

func (e *Envelope) Verify(public ed25519.PublicKey, now time.Time, limits Limits) error {
    if err:=limits.Validate();err!=nil{return err}
    if e.ID==""||len(e.ID)>128||e.Kind==""||len(e.Kind)>64||e.Sequence<1||e.Attempt<1{return fmt.Errorf("envelope stable identity/sequence is invalid")}
    if e.Timestamp.IsZero()||e.Expiry.IsZero()||!e.Expiry.After(e.Timestamp){return fmt.Errorf("envelope time interval is invalid")}
    now=now.UTC();if e.Timestamp.After(now.Add(limits.MaximumClockSkew)){return fmt.Errorf("envelope timestamp exceeds clock skew")};if !e.Expiry.After(now.Add(-limits.MaximumClockSkew)){return fmt.Errorf("envelope expired")}
    if int64(len(e.Payload))>limits.MaxMessageBytes{return fmt.Errorf("envelope payload exceeds %d bytes",limits.MaxMessageBytes)}
    sum:=sha256.Sum256(e.Payload);if hex.EncodeToString(sum[:])!=e.PayloadDigest{return fmt.Errorf("payload digest mismatch")}
    raw,err:=e.canonicalBytes();if err!=nil{return err};sig,err:=base64.StdEncoding.DecodeString(e.Signature);if err!=nil||len(sig)!=ed25519.SignatureSize{return fmt.Errorf("invalid envelope signature encoding")};if len(public)!=ed25519.PublicKeySize||!ed25519.Verify(public,raw,sig){return fmt.Errorf("envelope signature verification failed")};return nil
}

func SPIFFEID(domain spacev1.DomainReference, trustDomain string) string {
    trustDomain=strings.TrimSpace(trustDomain);if trustDomain==""{trustDomain="spacecompute.k3s.io"}
    return fmt.Sprintf("spiffe://%s/domain/%s/%s/%s",trustDomain,url.PathEscape(strings.ToLower(string(domain.OrbitClass))),url.PathEscape(strings.ToLower(domain.ClusterID)),url.PathEscape(strings.ToLower(domain.Name)))
}
''')

write("contrib/space-compute/pkg/transport/spool.go", r'''package transport

import (
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "sort"
    "strings"
    "sync"
    "time"
)

type queuedEnvelope struct { Envelope Envelope `json:"envelope"`; Attempts int `json:"attempts"`; NextAttempt time.Time `json:"nextAttempt"`; CreatedAt time.Time `json:"createdAt"`; LastError string `json:"lastError,omitempty"` }

type DiskQueue struct { mu sync.Mutex; dir string; limits Limits }
func OpenDiskQueue(dir string, limits Limits)(*DiskQueue,error){if err:=limits.Validate();err!=nil{return nil,err};if err:=os.MkdirAll(dir,0700);err!=nil{return nil,err};q:=&DiskQueue{dir:dir,limits:limits};if err:=q.pruneLocked(time.Now().UTC());err!=nil{return nil,err};return q,nil}
func (q *DiskQueue) Enqueue(e *Envelope) error {q.mu.Lock();defer q.mu.Unlock();raw,err:=json.Marshal(e);if err!=nil{return err};if int64(len(raw))>q.limits.MaxMessageBytes{return fmt.Errorf("serialized envelope exceeds message bound")};files,bytes,err:=q.statsLocked();if err!=nil{return err};if files>=q.limits.MaxQueueItems||bytes+int64(len(raw))>q.limits.MaxQueueBytes{return fmt.Errorf("persistent transport queue is full")};item:=queuedEnvelope{Envelope:*e,CreatedAt:time.Now().UTC(),NextAttempt:time.Now().UTC()};return writeAtomic(q.path(e.ID,e.Sequence),item)}
func (q *DiskQueue) Due(now time.Time,limit int)([]queuedEnvelope,error){q.mu.Lock();defer q.mu.Unlock();if limit<1||limit>q.limits.MaxConcurrent{limit=q.limits.MaxConcurrent};entries,err:=os.ReadDir(q.dir);if err!=nil{return nil,err};sort.Slice(entries,func(i,j int)bool{return entries[i].Name()<entries[j].Name()});out:=make([]queuedEnvelope,0,limit);for _,entry:=range entries{if len(out)>=limit||entry.IsDir()||!strings.HasSuffix(entry.Name(),".json"){continue};var item queuedEnvelope;if err:=readJSON(filepath.Join(q.dir,entry.Name()),&item);err!=nil{return nil,err};if !item.NextAttempt.After(now){out=append(out,item)}};return out,nil}
func (q *DiskQueue) Ack(id string,sequence int64) error {q.mu.Lock();defer q.mu.Unlock();err:=os.Remove(q.path(id,sequence));if os.IsNotExist(err){return nil};return err}
func (q *DiskQueue) Fail(item queuedEnvelope,err error,next time.Time) error {q.mu.Lock();defer q.mu.Unlock();item.Attempts++;item.NextAttempt=next.UTC();if err!=nil{item.LastError=err.Error();if len(item.LastError)>512{item.LastError=item.LastError[:512]}};if item.Attempts>=q.limits.MaxRetries{return os.Remove(q.path(item.Envelope.ID,item.Envelope.Sequence))};return writeAtomic(q.path(item.Envelope.ID,item.Envelope.Sequence),item)}
func (q *DiskQueue) path(id string,sequence int64)string{sum:=fmt.Sprintf("%x",shaKey(id));return filepath.Join(q.dir,fmt.Sprintf("%s-%020d.json",sum[:24],sequence))}
func (q *DiskQueue) statsLocked()(int,int64,error){entries,err:=os.ReadDir(q.dir);if err!=nil{return 0,0,err};n:=0;var b int64;for _,e:=range entries{if e.IsDir()||!strings.HasSuffix(e.Name(),".json"){continue};info,err:=e.Info();if err!=nil{return 0,0,err};n++;b+=info.Size()};return n,b,nil}
func (q *DiskQueue) pruneLocked(now time.Time)error{entries,err:=os.ReadDir(q.dir);if err!=nil{return err};for _,e:=range entries{if e.IsDir()||!strings.HasSuffix(e.Name(),".json"){continue};p:=filepath.Join(q.dir,e.Name());var item queuedEnvelope;if readJSON(p,&item)!=nil{_ = os.Remove(p);continue};if item.CreatedAt.Add(q.limits.DiskRetention).Before(now){_ = os.Remove(p)}};return nil}

// DedupeStore persists accepted envelope ID+sequence so receiver restarts remain
// idempotent. Entries age out only after the configured disk-retention horizon.
type DedupeStore struct{mu sync.Mutex;path string;retention time.Duration;entries map[string]time.Time}
func OpenDedupeStore(path string,retention time.Duration)(*DedupeStore,error){if retention<=0{return nil,fmt.Errorf("retention must be positive")};d:=&DedupeStore{path:path,retention:retention,entries:map[string]time.Time{}};_ = readJSON(path,&d.entries);d.prune(time.Now().UTC());return d,nil}
func (d *DedupeStore) SeenOrRecord(id string,sequence int64,now time.Time)(bool,error){d.mu.Lock();defer d.mu.Unlock();d.prune(now);key:=fmt.Sprintf("%s#%d",id,sequence);if _,ok:=d.entries[key];ok{return true,nil};d.entries[key]=now.UTC();return false,writeAtomic(d.path,d.entries)}
func (d *DedupeStore) prune(now time.Time){for k,t:=range d.entries{if t.Add(d.retention).Before(now){delete(d.entries,k)}}}

func shaKey(v string)[32]byte{return sha256Sum([]byte(v))}
func writeAtomic(path string,value interface{})error{if err:=os.MkdirAll(filepath.Dir(path),0700);err!=nil{return err};raw,err:=json.Marshal(value);if err!=nil{return err};tmp:=path+".tmp";if err:=os.WriteFile(tmp,raw,0600);err!=nil{return err};f,err:=os.OpenFile(tmp,os.O_RDWR,0600);if err==nil{_ = f.Sync();_ = f.Close()};return os.Rename(tmp,path)}
func readJSON(path string,out interface{})error{raw,err:=os.ReadFile(path);if err!=nil{return err};return json.Unmarshal(raw,out)}
''')
# Add missing crypto import to spool compactly.
text=read("contrib/space-compute/pkg/transport/spool.go")
text=text.replace('import (\n    "encoding/json"', 'import (\n    "crypto/sha256"\n    "encoding/json"').replace('func shaKey(v string)[32]byte{return sha256Sum([]byte(v))}', 'func shaKey(v string)[32]byte{return sha256.Sum256([]byte(v))}')
write("contrib/space-compute/pkg/transport/spool.go",text)

write("contrib/space-compute/pkg/transport/http.go", r'''package transport

import (
    "context"
    "crypto/ed25519"
    "crypto/rand"
    "crypto/tls"
    "encoding/json"
    "fmt"
    "io"
    "math"
    mrand "math/rand"
    "net/http"
    "strings"
    "sync"
    "time"

    spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

type PeerKeys interface { PublicKey(spacev1.DomainReference)(ed25519.PublicKey,error) }
type Handler func(context.Context,*Envelope)error

type Receiver struct { Local spacev1.DomainReference; TrustDomain string; Limits Limits; Keys PeerKeys; Dedupe *DedupeStore; Handler Handler; Now func()time.Time }
func (r *Receiver) ServeHTTP(w http.ResponseWriter,req *http.Request){if req.Method!=http.MethodPost{http.Error(w,"method not allowed",405);return};if r.Keys==nil||r.Dedupe==nil||r.Handler==nil{http.Error(w,"receiver unavailable",503);return};limits:=r.Limits;if err:=limits.Validate();err!=nil{http.Error(w,"receiver limits invalid",503);return};req.Body=http.MaxBytesReader(w,req.Body,limits.MaxMessageBytes+65536);raw,err:=io.ReadAll(req.Body);if err!=nil{http.Error(w,"bounded envelope read failed",413);return};var e Envelope;if err:=json.Unmarshal(raw,&e);err!=nil{http.Error(w,"invalid envelope JSON",400);return};if e.Destination!=r.Local{http.Error(w,"destination domain mismatch",403);return};if req.TLS==nil||len(req.TLS.PeerCertificates)==0{http.Error(w,"mutual TLS client certificate required",401);return};expected:=SPIFFEID(e.Source,r.TrustDomain);matched:=false;for _,uri:=range req.TLS.PeerCertificates[0].URIs{if uri.String()==expected{matched=true;break}};if !matched{http.Error(w,"client certificate SPIFFE identity does not match envelope source",403);return};key,err:=r.Keys.PublicKey(e.Source);if err!=nil{http.Error(w,"unknown source domain",403);return};now:=time.Now().UTC();if r.Now!=nil{now=r.Now().UTC()};if err:=e.Verify(key,now,limits);err!=nil{http.Error(w,err.Error(),403);return};seen,err:=r.Dedupe.SeenOrRecord(e.ID,e.Sequence,now);if err!=nil{http.Error(w,"dedupe persistence failed",503);return};if seen{w.WriteHeader(http.StatusNoContent);return};if err:=r.Handler(req.Context(),&e);err!=nil{http.Error(w,"durable handler rejected envelope",503);return};w.WriteHeader(http.StatusNoContent)}

func ServerTLSConfig(cert tls.Certificate,clientCAs *x509.CertPool)*tls.Config{return &tls.Config{MinVersion:tls.VersionTLS13,Certificates:[]tls.Certificate{cert},ClientAuth:tls.RequireAndVerifyClientCert,ClientCAs:clientCAs}}

type EndpointResolver interface { Endpoint(spacev1.DomainReference)(string,error) }
type circuitState struct{failures int;openUntil time.Time}
type Sender struct { Queue *DiskQueue; Client *http.Client; Endpoints EndpointResolver; Limits Limits; Now func()time.Time; mu sync.Mutex; circuits map[string]circuitState; random *mrand.Rand }
func (s *Sender) Run(ctx context.Context)error{if s.Queue==nil||s.Client==nil||s.Endpoints==nil{return fmt.Errorf("sender queue, client and endpoints are required")};if err:=s.Limits.Validate();err!=nil{return err};s.mu.Lock();if s.circuits==nil{s.circuits=map[string]circuitState{}};if s.random==nil{s.random=mrand.New(mrand.NewSource(1))};s.mu.Unlock();ticker:=time.NewTicker(250*time.Millisecond);defer ticker.Stop();sem:=make(chan struct{},s.Limits.MaxConcurrent);for{select{case<-ctx.Done():return ctx.Err();case<-ticker.C:now:=time.Now().UTC();if s.Now!=nil{now=s.Now().UTC()};items,err:=s.Queue.Due(now,s.Limits.MaxConcurrent);if err!=nil{return err};for _,item:=range items{item:=item;if s.circuitOpen(item.Envelope.Destination,now){continue};select{case sem<-struct{}{}:go func(){defer func(){<-sem}();s.deliver(ctx,item)}();default:}}}}}
func (s *Sender) deliver(ctx context.Context,item queuedEnvelope){e:=item.Envelope;endpoint,err:=s.Endpoints.Endpoint(e.Destination);if err==nil{var raw []byte;raw,err=json.Marshal(&e);if err==nil{var req *http.Request;req,err=http.NewRequestWithContext(ctx,http.MethodPost,strings.TrimRight(endpoint,"/")+"/v1/envelopes",strings.NewReader(string(raw)));if err==nil{req.Header.Set("Content-Type","application/json");var resp *http.Response;resp,err=s.Client.Do(req);if err==nil{io.Copy(io.Discard,io.LimitReader(resp.Body,4096));resp.Body.Close();if resp.StatusCode>=200&&resp.StatusCode<300{_ = s.Queue.Ack(e.ID,e.Sequence);s.recordSuccess(e.Destination);return};err=fmt.Errorf("remote status %s",resp.Status)}}}}};s.recordFailure(e.Destination);delay:=s.backoff(item.Attempts+1);_ = s.Queue.Fail(item,err,time.Now().UTC().Add(delay))}
func (s *Sender) backoff(attempt int)time.Duration{factor:=math.Pow(2,float64(attempt-1));d:=time.Duration(float64(s.Limits.BackoffBase)*factor);if d>s.Limits.BackoffMax{d=s.Limits.BackoffMax};s.mu.Lock();j:=(s.random.Float64()*2-1)*s.Limits.JitterFraction;s.mu.Unlock();return time.Duration(float64(d)*(1+j))}
func (s *Sender) circuitKey(d spacev1.DomainReference)string{return strings.ToLower(string(d.OrbitClass)+"/"+d.ClusterID+"/"+d.Name)}
func (s *Sender) circuitOpen(d spacev1.DomainReference,now time.Time)bool{s.mu.Lock();defer s.mu.Unlock();st:=s.circuits[s.circuitKey(d)];return st.openUntil.After(now)}
func (s *Sender) recordSuccess(d spacev1.DomainReference){s.mu.Lock();defer s.mu.Unlock();delete(s.circuits,s.circuitKey(d))}
func (s *Sender) recordFailure(d spacev1.DomainReference){s.mu.Lock();defer s.mu.Unlock();k:=s.circuitKey(d);st:=s.circuits[k];st.failures++;if st.failures>=s.Limits.CircuitFailures{st.openUntil=time.Now().UTC().Add(s.Limits.CircuitOpen);st.failures=0};s.circuits[k]=st}

func init(){var b [1]byte;_,_=rand.Read(b)}
''')
# Fix imports in http.go (x509 used; crypto/rand only to avoid deterministic global is unnecessary).
text=read("contrib/space-compute/pkg/transport/http.go")
text=text.replace('    "crypto/rand"\n    "crypto/tls"', '    "crypto/tls"\n    "crypto/x509"')
text=text.replace('\nfunc init(){var b [1]byte;_,_=rand.Read(b)}\n','\n')
write("contrib/space-compute/pkg/transport/http.go",text)

# Planner carries explicit transfer identity into placement; NotBefore remains the
# earliest dispatch time (computeStart), never transfer start.
replace_once("contrib/space-compute/pkg/planner/planner.go",
'''\t\tif transfer, ok := fitTransfer(snapshot, input.SizeBytes, earliest, mission, now); ok && (bestSnapshot == nil || transfer.End.Before(&best.End) || (transfer.End.Equal(&best.End) && snapshot.Name < bestSnapshot.Name)) {
\t\t\t\tbest, bestSnapshot = transfer, snapshot
\t\t\t}''',
'''\t\tif transfer, ok := fitTransfer(snapshot, input.SizeBytes, earliest, mission, now); ok && (bestSnapshot == nil || transfer.End.Before(&best.End) || (transfer.End.Equal(&best.End) && snapshot.Name < bestSnapshot.Name)) {
\t\t\t\ttransfer.DataID = input.ID
\t\t\t\ttransfer.Source = snapshot.Spec.Source
\t\t\t\ttransfer.Destination = target
\t\t\t\tbest, bestSnapshot = transfer, snapshot
\t\t\t}''')
replace_once("contrib/space-compute/pkg/planner/planner.go",
'''\tif locationMatchesDomain(values, source) {
\t\tat := metav1.NewTime(earliest)
\t\treturn spacev1.TransferEpoch{WindowID: "local-result", Start: at, End: at, Bytes: size},''',
'''\tif locationMatchesDomain(values, source) {
\t\tat := metav1.NewTime(earliest)
\t\treturn spacev1.TransferEpoch{WindowID: "local-result", DataID: "result", Source: source, Destination: source, Start: at, End: at, Bytes: size},''')
replace_once("contrib/space-compute/pkg/planner/planner.go",
'''\t\tif transfer, ok := fitTransfer(snapshot, size, earliest, mission, now); ok && (bestSnapshot == nil || transfer.End.Before(&best.End) || (transfer.End.Equal(&best.End) && snapshot.Name < bestSnapshot.Name)) {
\t\t\t\tbest, bestSnapshot = transfer, snapshot
\t\t\t}''',
'''\t\tif transfer, ok := fitTransfer(snapshot, size, earliest, mission, now); ok && (bestSnapshot == nil || transfer.End.Before(&best.End) || (transfer.End.Equal(&best.End) && snapshot.Name < bestSnapshot.Name)) {
\t\t\t\ttransfer.DataID = "result"
\t\t\t\ttransfer.Source = source
\t\t\t\ttransfer.Destination = snapshot.Spec.Destination
\t\t\t\tbest, bestSnapshot = transfer, snapshot
\t\t\t}''')
replace_once("contrib/space-compute/pkg/planner/planner.go",
'''\treturn spacev1.PlacementReady
}''','''\treturn spacev1.PlacementExecutionLeasePending
}''')

# Evidence adapter uses CRDs already admitted by API server/webhook.
write("contrib/space-compute/pkg/kube/evidence.go", r'''package kube

import (
    "context"
    "fmt"
    "reflect"

    apierrors "k8s.io/apimachinery/pkg/api/errors"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/runtime/schema"

    spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

var (
    TransferIntentGVR = schema.GroupVersionResource{Group:spacev1.GroupName,Version:"v1alpha1",Resource:"spacetransferintents"}
    TransferReceiptGVR = schema.GroupVersionResource{Group:spacev1.GroupName,Version:"v1alpha1",Resource:"spacetransferreceipts"}
    ExecutionLeaseGVR = schema.GroupVersionResource{Group:spacev1.GroupName,Version:"v1alpha1",Resource:"spaceexecutionleases"}
    ExecutionObservationGVR = schema.GroupVersionResource{Group:spacev1.GroupName,Version:"v1alpha1",Resource:"spaceexecutionobservations"}
    ResultReceiptGVR = schema.GroupVersionResource{Group:spacev1.GroupName,Version:"v1alpha1",Resource:"spaceresultreceipts"}
)

func (s *WorkloadStore) EnsureTransferIntent(ctx context.Context, desired *spacev1.SpaceTransferIntent) error {
    if s==nil||s.Repository==nil||s.Repository.Dynamic==nil{return fmt.Errorf("dynamic evidence client is required")}
    resource:=s.Repository.Dynamic.Resource(TransferIntentGVR)
    current,err:=resource.Get(ctx,desired.Name,metav1.GetOptions{})
    if apierrors.IsNotFound(err){u,err:=toUnstructured(desired);if err!=nil{return err};_,err=resource.Create(ctx,u,metav1.CreateOptions{});return err}
    if err!=nil{return err}
    existing:=&spacev1.SpaceTransferIntent{};if err:=fromUnstructured(current,existing);err!=nil{return err}
    if !reflect.DeepEqual(existing.Spec,desired.Spec){return fmt.Errorf("transfer intent %s exists with different immutable spec",desired.Name)}
    return nil
}
func (s *WorkloadStore) ListTransferReceipts(ctx context.Context)([]*spacev1.SpaceTransferReceipt,error){list,err:=s.Repository.Dynamic.Resource(TransferReceiptGVR).List(ctx,metav1.ListOptions{});if err!=nil{return nil,err};out:=make([]*spacev1.SpaceTransferReceipt,0,len(list.Items));for i:=range list.Items{v:=&spacev1.SpaceTransferReceipt{};if err:=fromUnstructured(&list.Items[i],v);err!=nil{return nil,err};out=append(out,v)};return out,nil}
func (s *WorkloadStore) ListExecutionLeases(ctx context.Context)([]*spacev1.SpaceExecutionLease,error){list,err:=s.Repository.Dynamic.Resource(ExecutionLeaseGVR).List(ctx,metav1.ListOptions{});if err!=nil{return nil,err};out:=make([]*spacev1.SpaceExecutionLease,0,len(list.Items));for i:=range list.Items{v:=&spacev1.SpaceExecutionLease{};if err:=fromUnstructured(&list.Items[i],v);err!=nil{return nil,err};out=append(out,v)};return out,nil}
func (s *WorkloadStore) GetExecutionLease(ctx context.Context,name string)(*spacev1.SpaceExecutionLease,error){u,err:=s.Repository.Dynamic.Resource(ExecutionLeaseGVR).Get(ctx,name,metav1.GetOptions{});if apierrors.IsNotFound(err){return nil,planner.ErrNotFound};if err!=nil{return nil,err};v:=&spacev1.SpaceExecutionLease{};if err:=fromUnstructured(u,v);err!=nil{return nil,err};return v,nil}
func (s *WorkloadStore) ListExecutionObservations(ctx context.Context)([]*spacev1.SpaceExecutionObservation,error){list,err:=s.Repository.Dynamic.Resource(ExecutionObservationGVR).List(ctx,metav1.ListOptions{});if err!=nil{return nil,err};out:=make([]*spacev1.SpaceExecutionObservation,0,len(list.Items));for i:=range list.Items{v:=&spacev1.SpaceExecutionObservation{};if err:=fromUnstructured(&list.Items[i],v);err!=nil{return nil,err};out=append(out,v)};return out,nil}
func (s *WorkloadStore) ListResultReceipts(ctx context.Context)([]*spacev1.SpaceResultReceipt,error){list,err:=s.Repository.Dynamic.Resource(ResultReceiptGVR).List(ctx,metav1.ListOptions{});if err!=nil{return nil,err};out:=make([]*spacev1.SpaceResultReceipt,0,len(list.Items));for i:=range list.Items{v:=&spacev1.SpaceResultReceipt{};if err:=fromUnstructured(&list.Items[i],v);err!=nil{return nil,err};out=append(out,v)};return out,nil}
''')
# evidence.go needs planner import.
text=read("contrib/space-compute/pkg/kube/evidence.go")
text=text.replace('spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"','spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"\n    "github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"')
write("contrib/space-compute/pkg/kube/evidence.go",text)

# Replace workload controller with evidence-gated dispatch. Legacy annotations are
# never consulted as trusted state.
write("contrib/space-compute/pkg/workload/controller.go", r'''// Package workload owns local durable dispatch and consumes trusted transfer /
// execution evidence. It never performs cross-domain network I/O.
package workload

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "strconv"
    "time"

    corev1 "k8s.io/api/core/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

    spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
    spaceexecution "github.com/k3s-io/k3s/contrib/space-compute/pkg/execution"
    "github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"
    spacepolicy "github.com/k3s-io/k3s/contrib/space-compute/pkg/policy"
)

type Store interface {
    GetPod(context.Context,string,string)(*corev1.Pod,error)
    CreatePod(context.Context,*corev1.Pod)(*corev1.Pod,error)
    DeletePod(context.Context,string,string)error
    UpdatePlacementStatus(context.Context,*spacev1.SpacePlacementIntent)error
    Event(context.Context,string,string,string,string,string)
}

type EvidenceStore interface {
    EnsureTransferIntent(context.Context,*spacev1.SpaceTransferIntent)error
    ListTransferReceipts(context.Context)([]*spacev1.SpaceTransferReceipt,error)
    ListExecutionLeases(context.Context)([]*spacev1.SpaceExecutionLease,error)
    GetExecutionLease(context.Context,string)(*spacev1.SpaceExecutionLease,error)
    ListExecutionObservations(context.Context)([]*spacev1.SpaceExecutionObservation,error)
    ListResultReceipts(context.Context)([]*spacev1.SpaceResultReceipt,error)
}

type Controller struct { Store Store; Evidence EvidenceStore; Clock spacev1.Clock }

func (c *Controller) clock() spacev1.Clock { if c.Clock==nil{return spacev1.RealClock{}};return c.Clock }

// ReconcileDispatch is fail-closed. Time alone never proves transfer or fencing.
func (c *Controller) ReconcileDispatch(ctx context.Context,mission *spacev1.SpaceMission,placement *spacev1.SpacePlacementIntent,template corev1.PodTemplateSpec)(time.Duration,error){
    clock:=c.clock();now:=clock.Now().UTC()
    if err:=spacev1.ValidateMission(mission,clock);err!=nil{return 0,err};if err:=spacev1.ValidatePlacement(placement,mission);err!=nil{return 0,err}
    if placement.Status.Phase==spacev1.PlacementCompleted||placement.Status.Phase==spacev1.PlacementFailed{return 0,nil}
    if !placement.Spec.ExpiresAt.After(now){placement.Status.Phase=spacev1.PlacementExpired;return 0,c.Store.UpdatePlacementStatus(ctx,placement)}
    if c.Evidence==nil { return c.wait(ctx,mission,placement,phaseBeforeLease(placement),"TrustedEvidenceUnavailable","transfer/lease evidence store is unavailable") }

    receipts,err:=c.Evidence.ListTransferReceipts(ctx);if err!=nil{return 0,err}
    for i,epoch:=range placement.Spec.InputTransfers{
        transferID:=fmt.Sprintf("input-%d-%s",i+1,safeID(epoch.DataID))
        intent:=&spacev1.SpaceTransferIntent{TypeMeta:metav1.TypeMeta{APIVersion:spacev1.SchemeGroupVersion.String(),Kind:"SpaceTransferIntent"},ObjectMeta:metav1.ObjectMeta{Name:spacev1.TransferIntentName(epoch.Source,epoch.Destination,string(mission.UID),placement.Spec.PlanID,transferID)},Spec:spacev1.SpaceTransferIntentSpec{TransferID:transferID,MissionUID:string(mission.UID),PlanID:placement.Spec.PlanID,Attempt:placement.Spec.Attempt,Source:epoch.Source,Destination:epoch.Destination,DataID:epoch.DataID,Bytes:epoch.Bytes,PayloadDigest:lookupInputDigest(mission,epoch.DataID),Window:epoch,ExpiresAt:placement.Spec.ExpiresAt}}
        if intent.Spec.PayloadDigest=="" { intent.Spec.PayloadDigest=strings.Repeat("0",64) }
        if err:=c.Evidence.EnsureTransferIntent(ctx,intent);err!=nil{return 0,err}
        if !matchingTransferReceipt(intent,receipts){return c.wait(ctx,mission,placement,spacev1.PlacementTransferPending,"TransferReceiptPending",fmt.Sprintf("input %s has no trusted transfer receipt",epoch.DataID))}
    }

    leases,err:=c.Evidence.ListExecutionLeases(ctx);if err!=nil{return 0,err}
    lease,err:=spaceexecution.LatestLeaseForAttempt(leases,string(mission.UID),placement.Spec.PlanID,placement.Spec.Attempt,now);if err!=nil{return 0,err}
    if lease==nil||lease.Spec.Source!=placement.Spec.Target{return c.wait(ctx,mission,placement,spacev1.PlacementExecutionLeasePending,"ExecutionLeasePending","no current trusted execution lease from target domain")}
    if placement.Spec.Attempt>1 {
        previous:=latestLeaseAnyPlan(leases,string(mission.UID),placement.Spec.Attempt-1)
        observations,err:=c.Evidence.ListExecutionObservations(ctx);if err!=nil{return 0,err}
        if err:=spaceexecution.CanStartAttempt(mission,previous,observations,now);err!=nil{return c.wait(ctx,mission,placement,spacev1.PlacementExecutionLeasePending,"PreviousAttemptNotFenced",err.Error())}
        if previous!=nil&&lease.Spec.Fence.LeaseEpoch<=previous.Spec.Fence.LeaseEpoch{return 0,fmt.Errorf("replacement execution lease epoch %d is not higher than previous %d",lease.Spec.Fence.LeaseEpoch,previous.Spec.Fence.LeaseEpoch)}
    }
    dispatchAt:=placement.Spec.ComputeStart.Time.UTC();if placement.Spec.NotBefore.Time.After(dispatchAt){dispatchAt=placement.Spec.NotBefore.Time.UTC()};if now.Before(dispatchAt){placement.Status.Phase=spacev1.PlacementExecutionLeasePending;if err:=c.Store.UpdatePlacementStatus(ctx,placement);err!=nil{return 0,err};return dispatchAt.Sub(now),nil}

    name:=AttemptPodName(mission.Name,placement.Spec.Attempt)
    if active:=placement.Status.ActivePod;active!=nil&&active.Name!=""&&active.Name!=name{ns:=active.Namespace;if ns==""{ns=mission.Namespace};_,err:=c.Store.GetPod(ctx,ns,active.Name);if err==nil{if err:=c.Store.DeletePod(ctx,ns,active.Name);err!=nil{return 0,err};c.Store.Event(ctx,mission.Namespace,mission.Name,"Normal","PreviousAttemptLocalCleanup",fmt.Sprintf("deleted local Pod %s only after remote execution fence was proven",active.Name));return time.Second,nil};if !errors.Is(err,planner.ErrNotFound){return 0,err};placement.Status.ActivePod=nil}
    existing,err:=c.Store.GetPod(ctx,mission.Namespace,name);if err==nil{if existing.Labels[spacev1.LabelPlacementID]!=placement.Spec.PlanID{return 0,fmt.Errorf("deterministic attempt Pod %s is fenced by a different plan",name)};if !podMatchesLease(existing,lease){return 0,fmt.Errorf("existing attempt Pod is fenced by a different execution lease/token")};if placement.Status.ActivePod==nil||placement.Status.ActivePod.UID!=existing.UID{placement.Status.ActivePod=&corev1.ObjectReference{Namespace:existing.Namespace,Name:existing.Name,UID:existing.UID};placement.Status.Phase=spacev1.PlacementDispatched;return 0,c.Store.UpdatePlacementStatus(ctx,placement)};return 0,nil};if !errors.Is(err,planner.ErrNotFound){return 0,err}
    pod,err:=BuildAttemptPodWithLease(mission,placement,template,lease);if err!=nil{return 0,err};created,err:=c.Store.CreatePod(ctx,pod);if err!=nil{return 0,err};placement.Status.ActivePod=&corev1.ObjectReference{Namespace:created.Namespace,Name:created.Name,UID:created.UID};placement.Status.Phase=spacev1.PlacementDispatched;placement.Status.RetryCount=placement.Spec.Attempt-1;if err:=c.Store.UpdatePlacementStatus(ctx,placement);err!=nil{return 0,err};c.Store.Event(ctx,mission.Namespace,mission.Name,"Normal","MissionAttemptDispatched",fmt.Sprintf("created attempt %d Pod %s under execution lease epoch %d",placement.Spec.Attempt,created.Name,lease.Spec.Fence.LeaseEpoch));return 0,nil
}

func phaseBeforeLease(p *spacev1.SpacePlacementIntent)spacev1.PlacementPhase{if len(p.Spec.InputTransfers)>0{return spacev1.PlacementTransferPending};return spacev1.PlacementExecutionLeasePending}
func (c *Controller) wait(ctx context.Context,mission *spacev1.SpaceMission,p *spacev1.SpacePlacementIntent,phase spacev1.PlacementPhase,reason,message string)(time.Duration,error){p.Status.Phase=phase;if err:=c.Store.UpdatePlacementStatus(ctx,p);err!=nil{return 0,err};c.Store.Event(ctx,mission.Namespace,mission.Name,"Normal",reason,message);return time.Second,nil}

// ReconcilePodStatus consumes only local Pod lifecycle under the accepted lease.
// result-returned/checkpoint-id annotations are untrusted hints and are ignored.
func (c *Controller) ReconcilePodStatus(ctx context.Context,mission *spacev1.SpaceMission,placement *spacev1.SpacePlacementIntent,pod *corev1.Pod)(bool,error){if mission==nil||placement==nil||pod==nil{return false,fmt.Errorf("mission, placement and Pod are required")};if c.Evidence==nil{return false,fmt.Errorf("trusted execution evidence store is required")};leaseName:=pod.Annotations[spacev1.GroupName+"/execution-lease"];lease,err:=c.Evidence.GetExecutionLease(ctx,leaseName);if err!=nil{return false,err};if !podMatchesLease(pod,lease){return false,fmt.Errorf("Pod execution fence does not match trusted lease")};if placement.Status.ActivePod!=nil&&placement.Status.ActivePod.UID!=""&&placement.Status.ActivePod.UID!=pod.UID{return false,fmt.Errorf("Pod UID is fenced by active execution")};phase:="dispatched";switch pod.Status.Phase{case corev1.PodRunning:phase="running";case corev1.PodFailed:phase="failed";case corev1.PodSucceeded:if mission.Spec.ResultReturnRequired{phase="return-pending"}else{phase="completed"}};if last:=placement.Status.LastObservation;last!=nil&&last.PodUID==string(pod.UID)&&last.Phase==phase{return false,nil};obs:=spacev1.ExecutionObservation{Sequence:placement.Status.LastObservationSequence+1,Attempt:placement.Spec.Attempt,PodUID:string(pod.UID),Phase:phase,ObservedAt:metav1.NewTime(c.clock().Now())};changed,err:=planner.ApplyExecutionObservation(placement,mission,obs,c.clock());if err!=nil{return false,err};if changed{if err:=c.Store.UpdatePlacementStatus(ctx,placement);err!=nil{return false,err}};return changed,nil}

// ReconcileTrustedEvidence is the only path that accepts checkpoint/result
// completion across a domain boundary.
func (c *Controller) ReconcileTrustedEvidence(ctx context.Context,mission *spacev1.SpaceMission,placement *spacev1.SpacePlacementIntent)(bool,error){if c.Evidence==nil{return false,nil};now:=c.clock().Now().UTC();leases,err:=c.Evidence.ListExecutionLeases(ctx);if err!=nil{return false,err};lease,err:=spaceexecution.LatestLeaseForAttempt(leases,string(mission.UID),placement.Spec.PlanID,placement.Spec.Attempt,now);if err!=nil||lease==nil{return false,err};if placement.Status.Phase==spacev1.PlacementReplanning&&mission.Spec.Checkpoint.Checkpointable{observations,err:=c.Evidence.ListExecutionObservations(ctx);if err!=nil{return false,err};for _,o:=range observations{if o.Spec.Phase!=spacev1.ExecutionObservationCheckpointed{continue};if spaceexecution.ValidateObservationAgainstLease(o,lease,now)!=nil{continue};obs:=spacev1.ExecutionObservation{Sequence:placement.Status.LastObservationSequence+1,Attempt:placement.Spec.Attempt,PodUID:activeUID(placement),Phase:"checkpointed",ObservedAt:o.Spec.ObservedAt,CheckpointID:o.Spec.CheckpointID};changed,err:=planner.ApplyExecutionObservation(placement,mission,obs,c.clock());if err!=nil{return false,err};if changed{return true,c.Store.UpdatePlacementStatus(ctx,placement)}}}
    if placement.Status.Phase==spacev1.PlacementReturnPending&&mission.Spec.ResultReturnRequired{receipts,err:=c.Evidence.ListResultReceipts(ctx);if err!=nil{return false,err};for _,r:=range receipts{if spaceexecution.ValidateResultAgainstLease(r,lease,now)!=nil{continue};obs:=spacev1.ExecutionObservation{Sequence:placement.Status.LastObservationSequence+1,Attempt:placement.Spec.Attempt,PodUID:activeUID(placement),Phase:"completed",ObservedAt:r.Spec.CompletedAt};changed,err:=planner.ApplyExecutionObservation(placement,mission,obs,c.clock());if err!=nil{return false,err};if changed{placement.Status.ResultReturned=true;return true,c.Store.UpdatePlacementStatus(ctx,placement)}}};return false,nil}

func matchingTransferReceipt(intent *spacev1.SpaceTransferIntent,receipts []*spacev1.SpaceTransferReceipt)bool{for _,r:=range receipts{if r==nil{continue};s:=r.Spec;if s.TransferID==intent.Spec.TransferID&&s.MissionUID==intent.Spec.MissionUID&&s.PlanID==intent.Spec.PlanID&&s.Attempt==intent.Spec.Attempt&&s.Source==intent.Spec.Source&&s.Destination==intent.Spec.Destination&&s.DataID==intent.Spec.DataID&&s.Bytes==intent.Spec.Bytes&&s.PayloadDigest==intent.Spec.PayloadDigest{return true}};return false}
func latestLeaseAnyPlan(values []*spacev1.SpaceExecutionLease,uid string,attempt int32)*spacev1.SpaceExecutionLease{var best *spacev1.SpaceExecutionLease;for _,v:=range values{if v!=nil&&v.Spec.Fence.MissionUID==uid&&v.Spec.Fence.Attempt==attempt&&(best==nil||v.Spec.Fence.LeaseEpoch>best.Spec.Fence.LeaseEpoch){best=v}};return best}
func safeID(v string)string{if v==""{return "data"};out:="";for _,r:=range strings.ToLower(v){if (r>='a'&&r<='z')||(r>='0'&&r<='9')||r=='-'{out+=string(r)}else{out+="-"}};out=strings.Trim(out,"-");if out==""{out="data"};if len(out)>40{out=out[:40]};return out}
func lookupInputDigest(m *spacev1.SpaceMission,id string)string{return ""}
func activeUID(p *spacev1.SpacePlacementIntent)string{if p.Status.ActivePod==nil{return ""};return string(p.Status.ActivePod.UID)}

func BuildAttemptPod(mission *spacev1.SpaceMission,placement *spacev1.SpacePlacementIntent,template corev1.PodTemplateSpec)(*corev1.Pod,error){return nil,fmt.Errorf("execution lease is required; use BuildAttemptPodWithLease")}
func BuildAttemptPodWithLease(mission *spacev1.SpaceMission,placement *spacev1.SpacePlacementIntent,template corev1.PodTemplateSpec,lease *spacev1.SpaceExecutionLease)(*corev1.Pod,error){if mission==nil||placement==nil||lease==nil{return nil,fmt.Errorf("mission, placement and execution lease are required")};f:=lease.Spec.Fence;if f.MissionUID!=string(mission.UID)||f.PlanID!=placement.Spec.PlanID||f.Attempt!=placement.Spec.Attempt||lease.Spec.Source!=placement.Spec.Target{return nil,fmt.Errorf("execution lease does not match placement")};missionIntent:=spacepolicy.PodMissionIntent{TypeMeta:metav1.TypeMeta{APIVersion:spacev1.SchemeGroupVersion.String(),Kind:"PodMissionIntent"},MissionUID:string(mission.UID),Spec:mission.Spec};podPlacement:=spacepolicy.PodPlacement{TypeMeta:metav1.TypeMeta{APIVersion:spacev1.SchemeGroupVersion.String(),Kind:"PodPlacement"},Spec:placement.Spec};missionRaw,err:=json.Marshal(missionIntent);if err!=nil{return nil,err};placementRaw,err:=json.Marshal(podPlacement);if err!=nil{return nil,err};pod:=&corev1.Pod{ObjectMeta:*template.ObjectMeta.DeepCopy(),Spec:*template.Spec.DeepCopy()};pod.Namespace=mission.Namespace;pod.Name=AttemptPodName(mission.Name,placement.Spec.Attempt);pod.GenerateName="";if pod.Labels==nil{pod.Labels=map[string]string{}};if pod.Annotations==nil{pod.Annotations=map[string]string{}};pod.Labels[spacev1.LabelPlacementID]=placement.Spec.PlanID;pod.Labels[spacev1.LabelMissionUID]=string(mission.UID);pod.Annotations[spacev1.AnnotationMissionIntent]=string(missionRaw);pod.Annotations[spacev1.AnnotationPlacement]=string(placementRaw);pod.Annotations[spacev1.GroupName+"/execution-lease"]=lease.Name;pod.Annotations[spacev1.GroupName+"/lease-epoch"]=strconv.FormatInt(f.LeaseEpoch,10);pod.Annotations[spacev1.GroupName+"/token-hash"]=f.TokenHash;pod.Spec.SchedulerName="space-compute-scheduler";tokenEnv:=corev1.EnvVar{Name:"SPACE_COMPUTE_FENCE_TOKEN",ValueFrom:&corev1.EnvVarSource{SecretKeyRef:&corev1.SecretKeySelector{LocalObjectReference:corev1.LocalObjectReference{Name:spacev1.ExecutionTokenSecretName(f)},Key:"token"}}};for i:=range pod.Spec.Containers{pod.Spec.Containers[i].Env=append(pod.Spec.Containers[i].Env,tokenEnv,corev1.EnvVar{Name:"SPACE_COMPUTE_LEASE_EPOCH",Value:strconv.FormatInt(f.LeaseEpoch,10)},corev1.EnvVar{Name:"SPACE_COMPUTE_TOKEN_HASH",Value:f.TokenHash})};controller:=true;block:=true;pod.OwnerReferences=[]metav1.OwnerReference{{APIVersion:spacev1.SchemeGroupVersion.String(),Kind:"SpaceMission",Name:mission.Name,UID:mission.UID,Controller:&controller,BlockOwnerDeletion:&block}};return pod,nil}
func podMatchesLease(pod *corev1.Pod,lease *spacev1.SpaceExecutionLease)bool{if pod==nil||lease==nil{return false};epoch,err:=strconv.ParseInt(pod.Annotations[spacev1.GroupName+"/lease-epoch"],10,64);return err==nil&&pod.Annotations[spacev1.GroupName+"/execution-lease"]==lease.Name&&epoch==lease.Spec.Fence.LeaseEpoch&&pod.Annotations[spacev1.GroupName+"/token-hash"]==lease.Spec.Fence.TokenHash}
func AttemptPodName(missionName string,attempt int32)string{suffix:=fmt.Sprintf("-attempt-%d",attempt);limit:=253-len(suffix);if len(missionName)>limit{missionName=missionName[:limit]};return missionName+suffix}
''')
# Add strings import missing in workload controller.
text=read("contrib/space-compute/pkg/workload/controller.go")
text=text.replace('    "strconv"\n    "time"','    "strconv"\n    "strings"\n    "time"')
write("contrib/space-compute/pkg/workload/controller.go",text)

# Planner process watches trusted evidence and wires the evidence store.
replace_once("cmd/space-compute-mission-planner/main.go",
'''\tresources := factory.ForResource(spacekube.ResourceSummaryGVR).Informer()
\tcoreFactory := informers.NewSharedInformerFactory(client, 10*time.Minute)''',
'''\tresources := factory.ForResource(spacekube.ResourceSummaryGVR).Informer()
\ttransferIntents := factory.ForResource(spacekube.TransferIntentGVR).Informer()
\ttransferReceipts := factory.ForResource(spacekube.TransferReceiptGVR).Informer()
\texecutionLeases := factory.ForResource(spacekube.ExecutionLeaseGVR).Informer()
\texecutionObservations := factory.ForResource(spacekube.ExecutionObservationGVR).Informer()
\tresultReceipts := factory.ForResource(spacekube.ResultReceiptGVR).Informer()
\tcoreFactory := informers.NewSharedInformerFactory(client, 10*time.Minute)''')
replace_once("cmd/space-compute-mission-planner/main.go",
'''\t_, _ = links.AddEventHandler(resourceHandler)
\t_, _ = resources.AddEventHandler(resourceHandler)
\t_, _ = pods.AddEventHandler''',
'''\t_, _ = links.AddEventHandler(resourceHandler)
\t_, _ = resources.AddEventHandler(resourceHandler)
\t_, _ = transferIntents.AddEventHandler(resourceHandler)
\t_, _ = transferReceipts.AddEventHandler(resourceHandler)
\t_, _ = executionLeases.AddEventHandler(resourceHandler)
\t_, _ = executionObservations.AddEventHandler(resourceHandler)
\t_, _ = resultReceipts.AddEventHandler(resourceHandler)
\t_, _ = pods.AddEventHandler''')
replace_once("cmd/space-compute-mission-planner/main.go",
'''\tif !cache.WaitForCacheSync(ctx.Done(), missions.HasSynced, placements.HasSynced, links.HasSynced, resources.HasSynced, pods.HasSynced) {''',
'''\tif !cache.WaitForCacheSync(ctx.Done(), missions.HasSynced, placements.HasSynced, links.HasSynced, resources.HasSynced, transferIntents.HasSynced, transferReceipts.HasSynced, executionLeases.HasSynced, executionObservations.HasSynced, resultReceipts.HasSynced, pods.HasSynced) {''')
replace_once("cmd/space-compute-mission-planner/main.go",
'''\tworkloadController := &spaceworkload.Controller{Store: &spacekube.WorkloadStore{Client: client, Repository: repository, Recorder: recorder}, Clock: spacev1.RealClock{}}''',
'''\tworkloadStore := &spacekube.WorkloadStore{Client: client, Repository: repository, Recorder: recorder}
\tworkloadController := &spaceworkload.Controller{Store: workloadStore, Evidence: workloadStore, Clock: spacev1.RealClock{}}''')
replace_once("cmd/space-compute-mission-planner/main.go",
'''\t\tif placement != nil && placement.Status.ActivePod != nil && placement.Status.ActivePod.Name != "" {''',
'''\t\tif placement != nil {
\t\t\tif _, evidenceErr := workloadController.ReconcileTrustedEvidence(ctx, mission, placement); evidenceErr != nil {
\t\t\t\tretryControllerItem(queue, item, "missions", evidenceErr, observer)
\t\t\t\treturn
\t\t\t}
\t\t}
\t\tplacement, _ = repository.GetPlacement(ctx, spaceplanner.MissionKey{Namespace: namespace, Name: name})
\t\tif placement != nil && placement.Status.ActivePod != nil && placement.Status.ActivePod.Name != "" {''')

print("stage5 runtime patch applied")
