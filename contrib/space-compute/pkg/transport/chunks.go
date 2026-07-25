package transport

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
	IntentName    string                  `json:"intentName"`
	TransferID    string                  `json:"transferID"`
	MissionUID    string                  `json:"missionUID"`
	PlanID        string                  `json:"planID"`
	Attempt       int32                   `json:"attempt"`
	Purpose       string                  `json:"purpose"`
	Source        spacev1.DomainReference `json:"source"`
	Destination   spacev1.DomainReference `json:"destination"`
	DataID        string                  `json:"dataID"`
	TotalBytes    int64                   `json:"totalBytes"`
	PayloadDigest string                  `json:"payloadDigest"`
	LeaseEpoch    int64                   `json:"leaseEpoch,omitempty"`
	TokenHash     string                  `json:"tokenHash,omitempty"`
	ChunkIndex    int32                   `json:"chunkIndex"`
	ChunkCount    int32                   `json:"chunkCount"`
	Offset        int64                   `json:"offset"`
	StartedAt     time.Time               `json:"startedAt"`
	Data          []byte                  `json:"data"`
}

type TransferAck struct {
	IntentName    string                  `json:"intentName"`
	TransferID    string                  `json:"transferID"`
	MissionUID    string                  `json:"missionUID"`
	PlanID        string                  `json:"planID"`
	Attempt       int32                   `json:"attempt"`
	Purpose       string                  `json:"purpose"`
	Source        spacev1.DomainReference `json:"source"`
	Destination   spacev1.DomainReference `json:"destination"`
	DataID        string                  `json:"dataID"`
	Bytes         int64                   `json:"bytes"`
	PayloadDigest string                  `json:"payloadDigest"`
	LeaseEpoch    int64                   `json:"leaseEpoch,omitempty"`
	TokenHash     string                  `json:"tokenHash,omitempty"`
	StartedAt     time.Time               `json:"startedAt"`
	CompletedAt   time.Time               `json:"completedAt"`
}

type assemblyState struct {
	TransferID    string    `json:"transferID"`
	DataID        string    `json:"dataID"`
	TotalBytes    int64     `json:"totalBytes"`
	PayloadDigest string    `json:"payloadDigest"`
	ChunkCount    int32     `json:"chunkCount"`
	Seen          []int32   `json:"seen"`
	StartedAt     time.Time `json:"startedAt"`
}

type FileAssembler struct {
	mu       sync.Mutex
	Root     string
	MaxBytes int64
}

func DataPath(root, id string) (string, error) {
	id = strings.TrimSpace(id)
	if id == "" || strings.Contains(id, "/") || strings.Contains(id, "\\") || id == "." || id == ".." {
		return "", fmt.Errorf("unsafe data ID")
	}
	root = filepath.Clean(root)
	path := filepath.Join(root, id)
	if filepath.Dir(path) != root {
		return "", fmt.Errorf("data path escapes root")
	}
	return path, nil
}
func (a *FileAssembler) Accept(chunk TransferChunk) (bool, *TransferAck, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.MaxBytes <= 0 {
		a.MaxBytes = 1 << 40
	}
	if chunk.TotalBytes < 0 || chunk.TotalBytes > a.MaxBytes || chunk.ChunkCount < 1 || chunk.ChunkCount > 1_000_000 || chunk.ChunkIndex < 0 || chunk.ChunkIndex >= chunk.ChunkCount || chunk.Offset < 0 || chunk.Offset+int64(len(chunk.Data)) > chunk.TotalBytes {
		return false, nil, fmt.Errorf("invalid transfer chunk bounds")
	}
	dest, err := DataPath(a.Root, chunk.DataID)
	if err != nil {
		return false, nil, err
	}
	incoming := filepath.Join(a.Root, ".incoming")
	if err := os.MkdirAll(incoming, 0700); err != nil {
		return false, nil, err
	}
	sum := sha256.Sum256([]byte(chunk.TransferID))
	key := hex.EncodeToString(sum[:12])
	part := filepath.Join(incoming, key+".part")
	statePath := filepath.Join(incoming, key+".json")
	state := assemblyState{TransferID: chunk.TransferID, DataID: chunk.DataID, TotalBytes: chunk.TotalBytes, PayloadDigest: chunk.PayloadDigest, ChunkCount: chunk.ChunkCount, StartedAt: chunk.StartedAt.UTC()}
	if raw, err := os.ReadFile(statePath); err == nil {
		if err := json.Unmarshal(raw, &state); err != nil {
			return false, nil, err
		}
		if state.TransferID != chunk.TransferID || state.DataID != chunk.DataID || state.TotalBytes != chunk.TotalBytes || state.PayloadDigest != chunk.PayloadDigest || state.ChunkCount != chunk.ChunkCount {
			return false, nil, fmt.Errorf("transfer assembly identity changed")
		}
	}
	seen := map[int32]struct{}{}
	for _, i := range state.Seen {
		seen[i] = struct{}{}
	}
	if _, ok := seen[chunk.ChunkIndex]; !ok {
		f, err := os.OpenFile(part, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return false, nil, err
		}
		if err := f.Truncate(chunk.TotalBytes); err == nil {
			_, err = f.WriteAt(chunk.Data, chunk.Offset)
		}
		if err == nil {
			err = f.Sync()
		}
		_ = f.Close()
		if err != nil {
			return false, nil, err
		}
		seen[chunk.ChunkIndex] = struct{}{}
		state.Seen = state.Seen[:0]
		for i := range seen {
			state.Seen = append(state.Seen, i)
		}
		sort.Slice(state.Seen, func(i, j int) bool { return state.Seen[i] < state.Seen[j] })
		if err := writeAtomic(statePath, state); err != nil {
			return false, nil, err
		}
	}
	if len(seen) != int(chunk.ChunkCount) {
		return false, nil, nil
	}
	f, err := os.Open(part)
	if err != nil {
		return false, nil, err
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	_ = f.Close()
	if err != nil {
		return false, nil, err
	}
	if n != chunk.TotalBytes || hex.EncodeToString(h.Sum(nil)) != chunk.PayloadDigest {
		return false, nil, fmt.Errorf("assembled payload digest/size mismatch")
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
		return false, nil, err
	}
	if err := os.Rename(part, dest); err != nil {
		return false, nil, err
	}
	_ = os.Remove(statePath)
	return true, &TransferAck{IntentName: chunk.IntentName, TransferID: chunk.TransferID, MissionUID: chunk.MissionUID, PlanID: chunk.PlanID, Attempt: chunk.Attempt, Purpose: chunk.Purpose, Source: chunk.Destination, Destination: chunk.Source, DataID: chunk.DataID, Bytes: chunk.TotalBytes, PayloadDigest: chunk.PayloadDigest, LeaseEpoch: chunk.LeaseEpoch, TokenHash: chunk.TokenHash, StartedAt: chunk.StartedAt.UTC(), CompletedAt: time.Now().UTC()}, nil
}

func ReadChunks(intent *spacev1.SpaceTransferIntent, root string, maxChunk int) ([]TransferChunk, error) {
	if intent == nil {
		return nil, fmt.Errorf("transfer intent required")
	}
	if maxChunk < 1024 {
		return nil, fmt.Errorf("chunk size too small")
	}
	path, err := DataPath(root, intent.Spec.DataID)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) != intent.Spec.Bytes {
		return nil, fmt.Errorf("source data size does not match transfer intent")
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != intent.Spec.PayloadDigest {
		return nil, fmt.Errorf("source data digest does not match transfer intent")
	}
	count := (len(raw) + maxChunk - 1) / maxChunk
	if count == 0 {
		count = 1
	}
	started := time.Now().UTC()
	out := make([]TransferChunk, 0, count)
	for i := 0; i < count; i++ {
		start := i * maxChunk
		end := start + maxChunk
		if end > len(raw) {
			end = len(raw)
		}
		data := append([]byte(nil), raw[start:end]...)
		out = append(out, TransferChunk{IntentName: intent.Name, TransferID: intent.Spec.TransferID, MissionUID: intent.Spec.MissionUID, PlanID: intent.Spec.PlanID, Attempt: intent.Spec.Attempt, Purpose: intent.Spec.Purpose, Source: intent.Spec.Source, Destination: intent.Spec.Destination, DataID: intent.Spec.DataID, TotalBytes: intent.Spec.Bytes, PayloadDigest: intent.Spec.PayloadDigest, LeaseEpoch: intent.Spec.LeaseEpoch, TokenHash: intent.Spec.TokenHash, ChunkIndex: int32(i), ChunkCount: int32(count), Offset: int64(start), StartedAt: started, Data: data})
	}
	return out, nil
}
