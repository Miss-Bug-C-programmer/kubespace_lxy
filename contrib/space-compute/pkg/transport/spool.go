package transport

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type queuedEnvelope struct {
	Envelope    Envelope  `json:"envelope"`
	Attempts    int       `json:"attempts"`
	NextAttempt time.Time `json:"nextAttempt"`
	CreatedAt   time.Time `json:"createdAt"`
	LastError   string    `json:"lastError,omitempty"`
}

type DiskQueue struct {
	mu     sync.Mutex
	dir    string
	limits Limits
}

func OpenDiskQueue(dir string, limits Limits) (*DiskQueue, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	q := &DiskQueue{dir: dir, limits: limits}
	if err := q.pruneLocked(time.Now().UTC()); err != nil {
		return nil, err
	}
	return q, nil
}
func (q *DiskQueue) Enqueue(e *Envelope) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if int64(len(raw)) > q.limits.MaxMessageBytes {
		return fmt.Errorf("serialized envelope exceeds message bound")
	}
	target := q.path(e.ID, e.Sequence)
	if _, statErr := os.Stat(target); statErr == nil {
		var existing queuedEnvelope
		if err := readJSON(target, &existing); err != nil {
			return err
		}
		old := existing.Envelope
		if old.Kind == e.Kind && old.Source == e.Source && old.Destination == e.Destination && old.MissionUID == e.MissionUID && old.PlanID == e.PlanID && old.Attempt == e.Attempt && old.PayloadDigest == e.PayloadDigest && bytes.Equal(old.Payload, e.Payload) {
			return nil
		}
		return fmt.Errorf("envelope identity collision for %s sequence %d", e.ID, e.Sequence)
	} else if !os.IsNotExist(statErr) {
		return statErr
	}
	files, bytesUsed, err := q.statsLocked()
	if err != nil {
		return err
	}
	if files >= q.limits.MaxQueueItems || bytesUsed+int64(len(raw)) > q.limits.MaxQueueBytes {
		return fmt.Errorf("persistent transport queue is full")
	}
	item := queuedEnvelope{Envelope: *e, CreatedAt: time.Now().UTC(), NextAttempt: time.Now().UTC()}
	return writeAtomic(target, item)
}
func (q *DiskQueue) Contains(id string, sequence int64) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, err := os.Stat(q.path(id, sequence))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (q *DiskQueue) Due(now time.Time, limit int) ([]queuedEnvelope, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if limit < 1 || limit > q.limits.MaxConcurrent {
		limit = q.limits.MaxConcurrent
	}
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	out := make([]queuedEnvelope, 0, limit)
	for _, entry := range entries {
		if len(out) >= limit || entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		var item queuedEnvelope
		if err := readJSON(filepath.Join(q.dir, entry.Name()), &item); err != nil {
			return nil, err
		}
		if !item.NextAttempt.After(now) {
			out = append(out, item)
		}
	}
	return out, nil
}
func (q *DiskQueue) Ack(id string, sequence int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	err := os.Remove(q.path(id, sequence))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
func (q *DiskQueue) Fail(item queuedEnvelope, err error, next time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	item.Attempts++
	item.NextAttempt = next.UTC()
	if err != nil {
		item.LastError = err.Error()
		if len(item.LastError) > 512 {
			item.LastError = item.LastError[:512]
		}
	}
	if item.Attempts >= q.limits.MaxRetries {
		return os.Remove(q.path(item.Envelope.ID, item.Envelope.Sequence))
	}
	return writeAtomic(q.path(item.Envelope.ID, item.Envelope.Sequence), item)
}
func (q *DiskQueue) path(id string, sequence int64) string {
	sum := fmt.Sprintf("%x", shaKey(id))
	return filepath.Join(q.dir, fmt.Sprintf("%s-%020d.json", sum[:24], sequence))
}
func (q *DiskQueue) statsLocked() (int, int64, error) {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return 0, 0, err
	}
	n := 0
	var b int64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return 0, 0, err
		}
		n++
		b += info.Size()
	}
	return n, b, nil
}
func (q *DiskQueue) pruneLocked(now time.Time) error {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		p := filepath.Join(q.dir, e.Name())
		var item queuedEnvelope
		if readJSON(p, &item) != nil {
			_ = os.Remove(p)
			continue
		}
		if item.CreatedAt.Add(q.limits.DiskRetention).Before(now) {
			_ = os.Remove(p)
		}
	}
	return nil
}

// DedupeStore persists accepted envelope ID+sequence so receiver restarts remain
// idempotent. Entries age out only after the configured disk-retention horizon.
type DedupeStore struct {
	mu        sync.Mutex
	path      string
	retention time.Duration
	entries   map[string]time.Time
}

func OpenDedupeStore(path string, retention time.Duration) (*DedupeStore, error) {
	if retention <= 0 {
		return nil, fmt.Errorf("retention must be positive")
	}
	d := &DedupeStore{path: path, retention: retention, entries: map[string]time.Time{}}
	_ = readJSON(path, &d.entries)
	d.prune(time.Now().UTC())
	return d, nil
}
func (d *DedupeStore) Seen(id string, sequence int64, now time.Time) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.prune(now)
	_, ok := d.entries[fmt.Sprintf("%s#%d", id, sequence)]
	return ok, nil
}
func (d *DedupeStore) Record(id string, sequence int64, now time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.prune(now)
	d.entries[fmt.Sprintf("%s#%d", id, sequence)] = now.UTC()
	return writeAtomic(d.path, d.entries)
}
func (d *DedupeStore) SeenOrRecord(id string, sequence int64, now time.Time) (bool, error) {
	seen, err := d.Seen(id, sequence, now)
	if err != nil || seen {
		return seen, err
	}
	return false, d.Record(id, sequence, now)
}
func (d *DedupeStore) prune(now time.Time) {
	for k, t := range d.entries {
		if t.Add(d.retention).Before(now) {
			delete(d.entries, k)
		}
	}
}

func shaKey(v string) [32]byte { return sha256.Sum256([]byte(v)) }
func writeAtomic(path string, value interface{}) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0600); err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_RDWR, 0600)
	if err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
	return os.Rename(tmp, path)
}
func readJSON(path string, out interface{}) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}
