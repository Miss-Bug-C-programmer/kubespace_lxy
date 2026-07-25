#!/usr/bin/env python3
from pathlib import Path

def write(path,text):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True);p.write_text(text)
def replace_once(path,old,new):
    p=Path(path);text=p.read_text()
    if text.count(old)!=1: raise SystemExit(f'{path}: marker count {text.count(old)} for {old[:100]!r}')
    p.write_text(text.replace(old,new,1))

write('cmd/space-compute-domain-agent/config.go', r'''package main

import (
    "crypto/ed25519"
    "crypto/tls"
    "crypto/x509"
    "encoding/base64"
    "encoding/json"
    "encoding/pem"
    "fmt"
    "os"
    "strings"
    "time"

    spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
    spacetransport "github.com/k3s-io/k3s/contrib/space-compute/pkg/transport"
)

type peerConfig struct {
    Domain spacev1.DomainReference `json:"domain"`
    URL string `json:"url"`
    PublicKeyFile string `json:"publicKeyFile"`
}

type agentConfig struct {
    LocalDomain spacev1.DomainReference `json:"localDomain"`
    ReporterPrincipal string `json:"reporterPrincipal"`
    TrustDomain string `json:"trustDomain"`
    ListenAddress string `json:"listenAddress"`
    ReportAddress string `json:"reportAddress"`
    HealthAddress string `json:"healthAddress"`
    StateDir string `json:"stateDir"`
    DataRoot string `json:"dataRoot"`
    TLSCertFile string `json:"tlsCertFile"`
    TLSKeyFile string `json:"tlsKeyFile"`
    ClientCAFile string `json:"clientCAFile"`
    SigningKeyFile string `json:"signingKeyFile"`
    LeaseTTLSeconds int64 `json:"leaseTTLSeconds"`
    MaxChunkBytes int `json:"maxChunkBytes"`
    MaxMessageBytes int64 `json:"maxMessageBytes"`
    MaxQueueItems int `json:"maxQueueItems"`
    MaxQueueBytes int64 `json:"maxQueueBytes"`
    MaxConcurrent int `json:"maxConcurrent"`
    MaxRetries int `json:"maxRetries"`
    DiskRetentionSeconds int64 `json:"diskRetentionSeconds"`
    BackoffBaseMillis int64 `json:"backoffBaseMillis"`
    BackoffMaxMillis int64 `json:"backoffMaxMillis"`
    JitterFraction float64 `json:"jitterFraction"`
    CircuitFailures int `json:"circuitFailures"`
    CircuitOpenSeconds int64 `json:"circuitOpenSeconds"`
    MaximumClockSkewSeconds int64 `json:"maximumClockSkewSeconds"`
    Peers []peerConfig `json:"peers"`
}

func loadAgentConfig(path string)(agentConfig,error){var cfg agentConfig;raw,err:=os.ReadFile(path);if err!=nil{return cfg,err};dec:=json.NewDecoder(strings.NewReader(string(raw)));dec.DisallowUnknownFields();if err:=dec.Decode(&cfg);err!=nil{return cfg,err};if cfg.TrustDomain==""{cfg.TrustDomain="spacecompute.k3s.io"};if cfg.ListenAddress==""{cfg.ListenAddress=":10443"};if cfg.ReportAddress==""{cfg.ReportAddress=":10445"};if cfg.HealthAddress==""{cfg.HealthAddress=":10446"};if cfg.LeaseTTLSeconds==0{cfg.LeaseTTLSeconds=120};if cfg.MaxChunkBytes==0{cfg.MaxChunkBytes=256<<10};defaults:=spacetransport.DefaultLimits();if cfg.MaxMessageBytes==0{cfg.MaxMessageBytes=defaults.MaxMessageBytes};if cfg.MaxQueueItems==0{cfg.MaxQueueItems=defaults.MaxQueueItems};if cfg.MaxQueueBytes==0{cfg.MaxQueueBytes=defaults.MaxQueueBytes};if cfg.MaxConcurrent==0{cfg.MaxConcurrent=defaults.MaxConcurrent};if cfg.MaxRetries==0{cfg.MaxRetries=defaults.MaxRetries};if cfg.DiskRetentionSeconds==0{cfg.DiskRetentionSeconds=int64(defaults.DiskRetention/time.Second)};if cfg.BackoffBaseMillis==0{cfg.BackoffBaseMillis=int64(defaults.BackoffBase/time.Millisecond)};if cfg.BackoffMaxMillis==0{cfg.BackoffMaxMillis=int64(defaults.BackoffMax/time.Millisecond)};if cfg.JitterFraction==0{cfg.JitterFraction=defaults.JitterFraction};if cfg.CircuitFailures==0{cfg.CircuitFailures=defaults.CircuitFailures};if cfg.CircuitOpenSeconds==0{cfg.CircuitOpenSeconds=int64(defaults.CircuitOpen/time.Second)};if cfg.MaximumClockSkewSeconds==0{cfg.MaximumClockSkewSeconds=int64(defaults.MaximumClockSkew/time.Second)};if cfg.LocalDomain.Name==""||cfg.LocalDomain.ClusterID==""||cfg.ReporterPrincipal==""||cfg.StateDir==""||cfg.DataRoot==""||cfg.TLSCertFile==""||cfg.TLSKeyFile==""||cfg.ClientCAFile==""||cfg.SigningKeyFile==""{return cfg,fmt.Errorf("localDomain, reporterPrincipal, state/data directories and TLS/signing files are required")};if len(cfg.Peers)>64{return cfg,fmt.Errorf("peer count exceeds 64")};if err:=cfg.limits().Validate();err!=nil{return cfg,err};return cfg,nil}
func (c agentConfig) limits()spacetransport.Limits{return spacetransport.Limits{MaxMessageBytes:c.MaxMessageBytes,MaxQueueItems:c.MaxQueueItems,MaxQueueBytes:c.MaxQueueBytes,MaxConcurrent:c.MaxConcurrent,MaxRetries:c.MaxRetries,DiskRetention:time.Duration(c.DiskRetentionSeconds)*time.Second,BackoffBase:time.Duration(c.BackoffBaseMillis)*time.Millisecond,BackoffMax:time.Duration(c.BackoffMaxMillis)*time.Millisecond,JitterFraction:c.JitterFraction,CircuitFailures:c.CircuitFailures,CircuitOpen:time.Duration(c.CircuitOpenSeconds)*time.Second,MaximumClockSkew:time.Duration(c.MaximumClockSkewSeconds)*time.Second}}

func loadPrivateKey(path string)(ed25519.PrivateKey,error){raw,err:=os.ReadFile(path);if err!=nil{return nil,err};if block,_:=pem.Decode(raw);block!=nil{key,err:=x509.ParsePKCS8PrivateKey(block.Bytes);if err!=nil{return nil,err};value,ok:=key.(ed25519.PrivateKey);if !ok{return nil,fmt.Errorf("signing key is not Ed25519")};return value,nil};trimmed:=strings.TrimSpace(string(raw));if decoded,err:=base64.StdEncoding.DecodeString(trimmed);err==nil{raw=decoded};if len(raw)!=ed25519.PrivateKeySize{return nil,fmt.Errorf("Ed25519 private key must be raw/base64 64 bytes or PKCS8 PEM")};return ed25519.PrivateKey(append([]byte(nil),raw...)),nil}
func loadPublicKey(path string)(ed25519.PublicKey,error){raw,err:=os.ReadFile(path);if err!=nil{return nil,err};if block,_:=pem.Decode(raw);block!=nil{key,err:=x509.ParsePKIXPublicKey(block.Bytes);if err!=nil{return nil,err};value,ok:=key.(ed25519.PublicKey);if !ok{return nil,fmt.Errorf("peer key is not Ed25519")};return value,nil};trimmed:=strings.TrimSpace(string(raw));if decoded,err:=base64.StdEncoding.DecodeString(trimmed);err==nil{raw=decoded};if len(raw)!=ed25519.PublicKeySize{return nil,fmt.Errorf("Ed25519 public key must be raw/base64 32 bytes or PKIX PEM")};return ed25519.PublicKey(append([]byte(nil),raw...)),nil}
func loadTLS(cfg agentConfig)(tls.Certificate,*x509.CertPool,error){cert,err:=tls.LoadX509KeyPair(cfg.TLSCertFile,cfg.TLSKeyFile);if err!=nil{return cert,nil,err};ca,err:=os.ReadFile(cfg.ClientCAFile);if err!=nil{return cert,nil,err};pool:=x509.NewCertPool();if !pool.AppendCertsFromPEM(ca){return cert,nil,fmt.Errorf("client CA file contains no certificates")};return cert,pool,nil}
''')

write('cmd/space-compute-domain-agent/peers.go', r'''package main

import (
    "crypto/ed25519"
    "crypto/tls"
    "crypto/x509"
    "fmt"
    "net/http"
    "net/url"
    "strings"
    "time"

    spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
    spacetransport "github.com/k3s-io/k3s/contrib/space-compute/pkg/transport"
)

type peerRecord struct{domain spacev1.DomainReference;endpoint string;key ed25519.PublicKey;client *http.Client}
type peerRegistry struct{records map[string]peerRecord}
func peerKey(d spacev1.DomainReference)string{return strings.ToLower(string(d.OrbitClass)+"/"+d.ClusterID+"/"+d.Name)}
func newPeerRegistry(cfg agentConfig,cert tls.Certificate,roots *x509.CertPool)(*peerRegistry,error){r:=&peerRegistry{records:map[string]peerRecord{}};for _,p:=range cfg.Peers{if p.Domain.Name==""||p.Domain.ClusterID==""||p.URL==""||p.PublicKeyFile==""{return nil,fmt.Errorf("peer domain/url/publicKeyFile are required")};parsed,err:=url.Parse(p.URL);if err!=nil||parsed.Scheme!="https"||parsed.Host==""{return nil,fmt.Errorf("peer %s URL must be https",p.Domain.Name)};key,err:=loadPublicKey(p.PublicKeyFile);if err!=nil{return nil,fmt.Errorf("peer %s public key: %w",p.Domain.Name,err)};expected:=spacetransport.SPIFFEID(p.Domain,cfg.TrustDomain);tlsConfig:=&tls.Config{MinVersion:tls.VersionTLS13,Certificates:[]tls.Certificate{cert},RootCAs:roots,InsecureSkipVerify:true,VerifyConnection:func(cs tls.ConnectionState)error{if len(cs.PeerCertificates)==0{return fmt.Errorf("peer certificate missing")};intermediates:=x509.NewCertPool();for _,c:=range cs.PeerCertificates[1:]{intermediates.AddCert(c)};if _,err:=cs.PeerCertificates[0].Verify(x509.VerifyOptions{Roots:roots,Intermediates:intermediates,KeyUsages:[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},CurrentTime:time.Now()});err!=nil{return fmt.Errorf("peer certificate chain: %w",err)};for _,uri:=range cs.PeerCertificates[0].URIs{if uri.String()==expected{return nil}};return fmt.Errorf("peer certificate SPIFFE identity does not match %s",expected)}};client:=&http.Client{Transport:&http.Transport{TLSClientConfig:tlsConfig,MaxIdleConns:cfg.MaxConcurrent*2,MaxIdleConnsPerHost:cfg.MaxConcurrent,IdleConnTimeout:90*time.Second},Timeout:30*time.Second};record:=peerRecord{domain:p.Domain,endpoint:strings.TrimRight(p.URL,"/"),key:key,client:client};k:=peerKey(p.Domain);if _,exists:=r.records[k];exists{return nil,fmt.Errorf("duplicate peer domain %s",p.Domain.Name)};r.records[k]=record};return r,nil}
func (r *peerRegistry) Endpoint(d spacev1.DomainReference)(string,error){p,ok:=r.records[peerKey(d)];if !ok{return "",fmt.Errorf("peer endpoint not configured")};return p.endpoint,nil}
func (r *peerRegistry) PublicKey(d spacev1.DomainReference)(ed25519.PublicKey,error){p,ok:=r.records[peerKey(d)];if !ok{return nil,fmt.Errorf("peer public key not configured")};return p.key,nil}
func (r *peerRegistry) Client(d spacev1.DomainReference)(*http.Client,error){p,ok:=r.records[peerKey(d)];if !ok{return nil,fmt.Errorf("peer HTTP client not configured")};return p.client,nil}
''')

write('cmd/space-compute-domain-agent/store.go', r'''package main

import (
    "context"
    "encoding/json"
    "fmt"
    "reflect"
    "strconv"

    corev1 "k8s.io/api/core/v1"
    apierrors "k8s.io/apimachinery/pkg/api/errors"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
    "k8s.io/apimachinery/pkg/runtime"
    "k8s.io/apimachinery/pkg/runtime/schema"
    "k8s.io/client-go/dynamic"
    "k8s.io/client-go/kubernetes"

    spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
    spacekube "github.com/k3s-io/k3s/contrib/space-compute/pkg/kube"
    spaceplanner "github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"
    spacetransport "github.com/k3s-io/k3s/contrib/space-compute/pkg/transport"
    spaceworkload "github.com/k3s-io/k3s/contrib/space-compute/pkg/workload"
)

type kubeAgentStore struct{dynamic dynamic.Interface;client kubernetes.Interface;remote *spacetransport.FileAssignmentStore}
func (s *kubeAgentStore) ListAssignments(ctx context.Context)([]spacetransport.Assignment,error){list,err:=s.dynamic.Resource(spacekube.PlacementGVR).Namespace(metav1.NamespaceAll).List(ctx,metav1.ListOptions{});if err!=nil{return nil,err};out:=make([]spacetransport.Assignment,0,len(list.Items));for i:=range list.Items{p:=&spacev1.SpacePlacementIntent{};if err:=fromU(&list.Items[i],p);err!=nil{return nil,err};if p.Status.Phase==spacev1.PlacementCompleted||p.Status.Phase==spacev1.PlacementFailed||p.Status.Phase==spacev1.PlacementExpired{continue};ns:=p.Spec.MissionRef.Namespace;if ns==""{ns=p.Namespace};u,err:=s.dynamic.Resource(spacekube.MissionGVR).Namespace(ns).Get(ctx,p.Spec.MissionRef.Name,metav1.GetOptions{});if apierrors.IsNotFound(err){continue};if err!=nil{return nil,err};m:=&spacev1.SpaceMission{};if err:=fromU(u,m);err!=nil{return nil,err};if p.Spec.MissionRef.UID!=""&&m.UID!=p.Spec.MissionRef.UID{continue};out=append(out,spacetransport.Assignment{Mission:m,Placement:p})};return out,nil}
func (s *kubeAgentStore) SaveRemoteAssignment(_ context.Context,a spacetransport.Assignment)error{return s.remote.Save(a)}
func (s *kubeAgentStore) ListRemoteAssignments(context.Context)([]spacetransport.Assignment,error){return s.remote.List()}
func (s *kubeAgentStore) ListTransferIntents(ctx context.Context)([]*spacev1.SpaceTransferIntent,error){return listTyped[spacev1.SpaceTransferIntent](ctx,s.dynamic,spacekube.TransferIntentGVR,func(v *spacev1.SpaceTransferIntent)*spacev1.SpaceTransferIntent{return v})}
func (s *kubeAgentStore) GetTransferIntent(ctx context.Context,name string)(*spacev1.SpaceTransferIntent,error){u,err:=s.dynamic.Resource(spacekube.TransferIntentGVR).Get(ctx,name,metav1.GetOptions{});if err!=nil{return nil,err};v:=&spacev1.SpaceTransferIntent{};return v,fromU(u,v)}
func (s *kubeAgentStore) UpsertTransferIntent(ctx context.Context,v *spacev1.SpaceTransferIntent)error{return upsertTyped(ctx,s.dynamic,spacekube.TransferIntentGVR,v)}
func (s *kubeAgentStore) ListTransferReceipts(ctx context.Context)([]*spacev1.SpaceTransferReceipt,error){list,err:=s.dynamic.Resource(spacekube.TransferReceiptGVR).List(ctx,metav1.ListOptions{});if err!=nil{return nil,err};out:=make([]*spacev1.SpaceTransferReceipt,0,len(list.Items));for i:=range list.Items{v:=&spacev1.SpaceTransferReceipt{};if err:=fromU(&list.Items[i],v);err!=nil{return nil,err};out=append(out,v)};return out,nil}
func (s *kubeAgentStore) UpsertTransferReceipt(ctx context.Context,v *spacev1.SpaceTransferReceipt)error{return upsertTyped(ctx,s.dynamic,spacekube.TransferReceiptGVR,v)}
func (s *kubeAgentStore) ListExecutionLeases(ctx context.Context)([]*spacev1.SpaceExecutionLease,error){list,err:=s.dynamic.Resource(spacekube.ExecutionLeaseGVR).List(ctx,metav1.ListOptions{});if err!=nil{return nil,err};out:=make([]*spacev1.SpaceExecutionLease,0,len(list.Items));for i:=range list.Items{v:=&spacev1.SpaceExecutionLease{};if err:=fromU(&list.Items[i],v);err!=nil{return nil,err};out=append(out,v)};return out,nil}
func (s *kubeAgentStore) UpsertExecutionLease(ctx context.Context,v *spacev1.SpaceExecutionLease)error{return upsertTyped(ctx,s.dynamic,spacekube.ExecutionLeaseGVR,v)}
func (s *kubeAgentStore) ListExecutionObservations(ctx context.Context)([]*spacev1.SpaceExecutionObservation,error){list,err:=s.dynamic.Resource(spacekube.ExecutionObservationGVR).List(ctx,metav1.ListOptions{});if err!=nil{return nil,err};out:=make([]*spacev1.SpaceExecutionObservation,0,len(list.Items));for i:=range list.Items{v:=&spacev1.SpaceExecutionObservation{};if err:=fromU(&list.Items[i],v);err!=nil{return nil,err};out=append(out,v)};return out,nil}
func (s *kubeAgentStore) UpsertExecutionObservation(ctx context.Context,v *spacev1.SpaceExecutionObservation)error{return upsertTyped(ctx,s.dynamic,spacekube.ExecutionObservationGVR,v)}
func (s *kubeAgentStore) UpsertResultReceipt(ctx context.Context,v *spacev1.SpaceResultReceipt)error{return upsertTyped(ctx,s.dynamic,spacekube.ResultReceiptGVR,v)}
func (s *kubeAgentStore) UpsertRemoteReporterObject(ctx context.Context,resource string,raw []byte)error{gvr,kind,ok:=reporterGVR(resource);if !ok{return fmt.Errorf("remote reporter resource %q is not allowed",resource)};var object unstructured.Unstructured;if err:=json.Unmarshal(raw,&object.Object);err!=nil{return err};if object.GetAPIVersion()!=spacev1.SchemeGroupVersion.String()||object.GetKind()!=kind||object.GetName()==""{return fmt.Errorf("remote reporter object GVK/name mismatch")};return upsertU(ctx,s.dynamic,gvr,&object)}
func (s *kubeAgentStore) PutFenceToken(ctx context.Context,namespace string,f spacev1.ExecutionFence,token string)error{if namespace==""{return fmt.Errorf("mission namespace is required for fence token")};name:=spacev1.ExecutionTokenSecretName(f);secrets:=s.client.CoreV1().Secrets(namespace);current,err:=secrets.Get(ctx,name,metav1.GetOptions{});if err==nil{if string(current.Data["token"])!=token{return fmt.Errorf("fence token Secret collision")};return nil};if !apierrors.IsNotFound(err){return err};immutable:=true;_,err=secrets.Create(ctx,&corev1.Secret{ObjectMeta:metav1.ObjectMeta{Name:name,Namespace:namespace,Labels:map[string]string{spacev1.GroupName+"/mission-uid":f.MissionUID,spacev1.GroupName+"/plan-id":f.PlanID,spacev1.GroupName+"/attempt":strconv.Itoa(int(f.Attempt)),spacev1.GroupName+"/lease-epoch":strconv.FormatInt(f.LeaseEpoch,10)}},Immutable:&immutable,Type:corev1.SecretTypeOpaque,Data:map[string][]byte{"token":[]byte(token)}},metav1.CreateOptions{});return err}
func (s *kubeAgentStore) GetFenceToken(ctx context.Context,namespace string,f spacev1.ExecutionFence)(string,error){if namespace==""{return "",fmt.Errorf("mission namespace is required")};v,err:=s.client.CoreV1().Secrets(namespace).Get(ctx,spacev1.ExecutionTokenSecretName(f),metav1.GetOptions{});if err!=nil{return "",err};token:=string(v.Data["token"]);if token==""{return "",fmt.Errorf("fence token key is missing")};return token,nil}

func reporterGVR(resource string)(schema.GroupVersionResource,string,bool){switch resource{case "spacetransferreceipts":return spacekube.TransferReceiptGVR,"SpaceTransferReceipt",true;case "spaceexecutionleases":return spacekube.ExecutionLeaseGVR,"SpaceExecutionLease",true;case "spaceexecutionobservations":return spacekube.ExecutionObservationGVR,"SpaceExecutionObservation",true;case "spaceresultreceipts":return spacekube.ResultReceiptGVR,"SpaceResultReceipt",true;default:return schema.GroupVersionResource{},"",false}}
func fromU(u *unstructured.Unstructured,out any)error{return runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object,out)}
func toU(v any)(*unstructured.Unstructured,error){m,err:=runtime.DefaultUnstructuredConverter.ToUnstructured(v);if err!=nil{return nil,err};return &unstructured.Unstructured{Object:m},nil}
func upsertTyped(ctx context.Context,d dynamic.Interface,gvr schema.GroupVersionResource,v any)error{u,err:=toU(v);if err!=nil{return err};return upsertU(ctx,d,gvr,u)}
func upsertU(ctx context.Context,d dynamic.Interface,gvr schema.GroupVersionResource,u *unstructured.Unstructured)error{resource:=d.Resource(gvr);current,err:=resource.Get(ctx,u.GetName(),metav1.GetOptions{});if apierrors.IsNotFound(err){_,err=resource.Create(ctx,u,metav1.CreateOptions{});return err};if err!=nil{return err};same,err:=sameOrAdvance(current,u);if err!=nil{return err};if same{return nil};u.SetResourceVersion(current.GetResourceVersion());_,err=resource.Update(ctx,u,metav1.UpdateOptions{});return err}
func sameOrAdvance(old,new *unstructured.Unstructured)(bool,error){oldSeq,_,_:=unstructured.NestedInt64(old.Object,"spec","provenance","sequence");newSeq,_,_:=unstructured.NestedInt64(new.Object,"spec","provenance","sequence");oldDigest,_,_:=unstructured.NestedString(old.Object,"spec","provenance","digest");newDigest,_,_:=unstructured.NestedString(new.Object,"spec","provenance","digest");if oldSeq==0&&newSeq==0{return reflect.DeepEqual(old.Object["spec"],new.Object["spec"]),nil};if newSeq==oldSeq&&newDigest==oldDigest{return true,nil};if newSeq<=oldSeq{return false,fmt.Errorf("reporter sequence did not advance: old=%d new=%d",oldSeq,newSeq)};return false,nil}

// Generic helper is kept only for transfer intents whose concrete conversion is
// compile-time checked by the callers below.
func listTyped[T any](ctx context.Context,d dynamic.Interface,gvr schema.GroupVersionResource,identity func(*T)*T)([]*T,error){list,err:=d.Resource(gvr).List(ctx,metav1.ListOptions{});if err!=nil{return nil,err};out:=make([]*T,0,len(list.Items));for i:=range list.Items{v:=new(T);if err:=fromU(&list.Items[i],v);err!=nil{return nil,err};out=append(out,identity(v))};return out,nil}

type kubeExecutor struct{client kubernetes.Interface}
func (e *kubeExecutor) EnsureExecution(ctx context.Context,m *spacev1.SpaceMission,p *spacev1.SpacePlacementIntent,l *spacev1.SpaceExecutionLease)error{pod,err:=spaceworkload.BuildAttemptPodWithLease(m,p,m.Spec.WorkloadTemplate,l);if err!=nil{return err};pods:=e.client.CoreV1().Pods(m.Namespace);current,err:=pods.Get(ctx,pod.Name,metav1.GetOptions{});if err==nil{if current.Labels[spacev1.LabelPlacementID]!=p.Spec.PlanID||current.Annotations[spacev1.GroupName+"/execution-lease"]!=l.Name||current.Annotations[spacev1.GroupName+"/token-hash"]!=l.Spec.Fence.TokenHash{return fmt.Errorf("existing remote attempt Pod is fenced by different identity")};return nil};if !apierrors.IsNotFound(err){return err};_,err=pods.Create(ctx,pod,metav1.CreateOptions{});return err}

var _ spacetransport.AgentStore = (*kubeAgentStore)(nil)
var _ spacetransport.Executor = (*kubeExecutor)(nil)
var _ = spaceplanner.ErrNotFound
''')

write('cmd/space-compute-domain-agent/main.go', r'''package main

import (
    "context"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "net"
    "net/http"
    "os"
    "os/signal"
    "path/filepath"
    "syscall"
    "time"

    "k8s.io/client-go/dynamic"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/rest"
    clientcmd "k8s.io/client-go/tools/clientcmd"
    "k8s.io/klog/v2"

    spaceexecution "github.com/k3s-io/k3s/contrib/space-compute/pkg/execution"
    spacetransport "github.com/k3s-io/k3s/contrib/space-compute/pkg/transport"
)

const componentName="space-compute-domain-agent"
type reportRequest struct{Namespace string `json:"namespace"`;Report spaceexecution.Report `json:"report"`}

func main(){klog.InitFlags(nil);configPath:=flag.String("config","/etc/space-compute/domain-agent.json","Path to strict domain-agent JSON config");kubeconfig:=flag.String("kubeconfig","","Path to kubeconfig; empty uses in-cluster config");flag.Parse();if err:=run(*configPath,*kubeconfig);err!=nil{klog.Fatalf("%s: %v",componentName,err)}}
func run(configPath,kubeconfig string)error{cfg,err:=loadAgentConfig(configPath);if err!=nil{return err};privateKey,err:=loadPrivateKey(cfg.SigningKeyFile);if err!=nil{return err};cert,roots,err:=loadTLS(cfg);if err!=nil{return err};peers,err:=newPeerRegistry(cfg,cert,roots);if err!=nil{return err};restConfig,err:=kubeConfig(kubeconfig);if err!=nil{return err};dynamicClient,err:=dynamic.NewForConfig(restConfig);if err!=nil{return err};client,err:=kubernetes.NewForConfig(restConfig);if err!=nil{return err};limits:=cfg.limits();queue,err:=spacetransport.OpenDiskQueue(filepath.Join(cfg.StateDir,"outbox"),limits);if err!=nil{return err};dedupe,err:=spacetransport.OpenDedupeStore(filepath.Join(cfg.StateDir,"dedupe.json"),limits.DiskRetention);if err!=nil{return err};assignments,err:=spacetransport.OpenFileAssignmentStore(filepath.Join(cfg.StateDir,"assignments"),4096);if err!=nil{return err};store:=&kubeAgentStore{dynamic:dynamicClient,client:client,remote:assignments};agent:=&spacetransport.Agent{Local:cfg.LocalDomain,ReporterPrincipal:cfg.ReporterPrincipal,PrivateKey:privateKey,Queue:queue,Store:store,Executor:&kubeExecutor{client:client},Assembler:&spacetransport.FileAssembler{Root:cfg.DataRoot,MaxBytes:1<<40},DataRoot:cfg.DataRoot,LeaseTTL:time.Duration(cfg.LeaseTTLSeconds)*time.Second,MaxChunkBytes:cfg.MaxChunkBytes,Limits:limits};if err:=agent.Validate();err!=nil{return err};receiver:=&spacetransport.Receiver{Local:cfg.LocalDomain,TrustDomain:cfg.TrustDomain,Limits:limits,Keys:peers,Dedupe:dedupe,Handler:agent.HandleEnvelope};sender:=&spacetransport.Sender{Queue:queue,Clients:peers,Endpoints:peers,Limits:limits};ctx,cancel:=signal.NotifyContext(context.Background(),os.Interrupt,syscall.SIGTERM);defer cancel();errCh:=make(chan error,4);go func(){errCh<-serveEnvelope(ctx,cfg.ListenAddress,spacetransport.ServerTLSConfig(cert,roots),receiver)}();go func(){errCh<-serveReport(ctx,cfg.ReportAddress,agent)}();go func(){errCh<-serveHealth(ctx,cfg.HealthAddress)}();go func(){errCh<-sender.Run(ctx)}();ticker:=time.NewTicker(time.Second);defer ticker.Stop();for{select{case<-ctx.Done():return nil;case err:=<-errCh:if err!=nil&&ctx.Err()==nil{return err};case<-ticker.C:if err:=agent.ReconcileOnce(ctx);err!=nil{klog.Errorf("domain reconcile failed: %v",err)}}}}
func kubeConfig(path string)(*rest.Config,error){if path!=""{return clientcmd.BuildConfigFromFlags("",path)};return rest.InClusterConfig()}
func serveEnvelope(ctx context.Context,address string,tlsConfig *tls.Config,handler http.Handler)error{listener,err:=net.Listen("tcp",address);if err!=nil{return err};server:=&http.Server{Handler:handler,ReadHeaderTimeout:5*time.Second,ReadTimeout:30*time.Second,WriteTimeout:30*time.Second,IdleTimeout:90*time.Second};tlsListener:=tls.NewListener(listener,tlsConfig);go func(){<-ctx.Done();shutdownCtx,cancel:=context.WithTimeout(context.Background(),5*time.Second);defer cancel();_ = server.Shutdown(shutdownCtx)}();err=server.Serve(tlsListener);if err==http.ErrServerClosed{return nil};return err}
func serveReport(ctx context.Context,address string,agent *spacetransport.Agent)error{mux:=http.NewServeMux();mux.HandleFunc("/v1/report",func(w http.ResponseWriter,r *http.Request){if r.Method!=http.MethodPost{http.Error(w,"method not allowed",405);return};r.Body=http.MaxBytesReader(w,r.Body,64<<10);raw,err:=io.ReadAll(r.Body);if err!=nil{http.Error(w,"report too large",413);return};var request reportRequest;if err:=json.Unmarshal(raw,&request);err!=nil{http.Error(w,"invalid report",400);return};if request.Namespace==""{http.Error(w,"namespace required",400);return};if err:=agent.ReportExecution(r.Context(),request.Namespace,request.Report);err!=nil{http.Error(w,err.Error(),403);return};w.WriteHeader(http.StatusNoContent)});return servePlain(ctx,address,mux)}
func serveHealth(ctx context.Context,address string)error{mux:=http.NewServeMux();mux.HandleFunc("/livez",func(w http.ResponseWriter,_ *http.Request){w.WriteHeader(200)});mux.HandleFunc("/readyz",func(w http.ResponseWriter,_ *http.Request){w.WriteHeader(200)});return servePlain(ctx,address,mux)}
func servePlain(ctx context.Context,address string,handler http.Handler)error{server:=&http.Server{Addr:address,Handler:handler,ReadHeaderTimeout:5*time.Second,ReadTimeout:15*time.Second,WriteTimeout:15*time.Second,IdleTimeout:60*time.Second};go func(){<-ctx.Done();shutdownCtx,cancel:=context.WithTimeout(context.Background(),5*time.Second);defer cancel();_ = server.Shutdown(shutdownCtx)}();err:=server.ListenAndServe();if err==http.ErrServerClosed{return nil};return err}
var _ = fmt.Sprintf
''')
# Fix main imports TLS, remove fmt dummy by keeping fmt import only if not used. We don't need fmt.
p=Path('cmd/space-compute-domain-agent/main.go');text=p.read_text().replace('    "fmt"\n','').replace('    "net"\n','    "net"\n    "crypto/tls"\n').replace('\nvar _ = fmt.Sprintf\n','\n');p.write_text(text)

# Mission planner learns its local domain. When configured, remote placements are
# never materialized as local Pods.
replace_once('cmd/space-compute-mission-planner/main.go',
'''\tworkloadStore := &spacekube.WorkloadStore{Client: client, Repository: repository, Recorder: recorder}
\tworkloadController := &spaceworkload.Controller{Store: workloadStore, Evidence: workloadStore, Clock: spacev1.RealClock{}}''',
'''\tworkloadStore := &spacekube.WorkloadStore{Client: client, Repository: repository, Recorder: recorder}
\tworkloadController := &spaceworkload.Controller{Store: workloadStore, Evidence: workloadStore, Clock: spacev1.RealClock{}}
\tif raw := os.Getenv("SPACE_COMPUTE_LOCAL_DOMAIN_JSON"); raw != "" {
\t\tvar localDomain spacev1.DomainReference
\t\tif err := json.Unmarshal([]byte(raw), &localDomain); err != nil || localDomain.Name == "" || localDomain.ClusterID == "" {
\t\t\treturn fmt.Errorf("invalid SPACE_COMPUTE_LOCAL_DOMAIN_JSON")
\t\t}
\t\tworkloadController.LocalDomain = &localDomain
\t}''')
print('stage5 domain-agent command applied')
