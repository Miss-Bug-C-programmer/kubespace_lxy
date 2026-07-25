package transport

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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

func TestEnvelopeSignatureDigestExpiry(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	src, dst := domains()
	now := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
	e := NewEnvelope("env-1", "test", src, dst, "m", "plan", 1, 1, now, now.Add(time.Minute), []byte("payload"))
	if err := e.Sign(priv); err != nil {
		t.Fatal(err)
	}
	if err := e.Verify(pub, now, DefaultLimits()); err != nil {
		t.Fatalf("valid envelope: %v", err)
	}
	e.Payload = []byte("forged")
	if err := e.Verify(pub, now, DefaultLimits()); err == nil {
		t.Fatal("payload forgery accepted")
	}
	e.Payload = []byte("payload")
	if err := e.Verify(pub, now.Add(5*time.Minute), DefaultLimits()); err == nil {
		t.Fatal("expired envelope accepted")
	}
}

func TestDiskQueuePersistsBoundsAndIdempotency(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxQueueItems = 1
	limits.MaxQueueBytes = 2 << 20
	dir := t.TempDir()
	q, err := OpenDiskQueue(dir, limits)
	if err != nil {
		t.Fatal(err)
	}
	src, dst := domains()
	now := time.Now().UTC()
	e := NewEnvelope("same", "test", src, dst, "m", "plan", 1, 1, now, now.Add(time.Hour), []byte("x"))
	if err := q.Enqueue(e); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(e); err != nil {
		t.Fatalf("idempotent enqueue: %v", err)
	}
	e2 := NewEnvelope("second", "test", src, dst, "m", "plan", 1, 1, now, now.Add(time.Hour), []byte("y"))
	if err := q.Enqueue(e2); err == nil {
		t.Fatal("queue item bound not enforced")
	}
	q2, err := OpenDiskQueue(dir, limits)
	if err != nil {
		t.Fatal(err)
	}
	due, err := q2.Due(now.Add(time.Second), 1)
	if err != nil || len(due) != 1 || due[0].Envelope.ID != "same" {
		t.Fatalf("restart recovery due=%v err=%v", due, err)
	}
}

func TestDedupePersistsAcrossRestartAndFailedHandlerCanRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dedupe.json")
	d, err := OpenDedupeStore(path, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	seen, _ := d.Seen("e", 1, now)
	if seen {
		t.Fatal("new envelope already seen")
	}
	if err := d.Record("e", 1, now); err != nil {
		t.Fatal(err)
	}
	d2, _ := OpenDedupeStore(path, time.Hour)
	seen, _ = d2.Seen("e", 1, now)
	if !seen {
		t.Fatal("dedupe did not persist")
	}
	seen, _ = d2.Seen("retry", 1, now)
	if seen {
		t.Fatal("failed/unrecorded handler would be suppressed")
	}
}

func TestFileAssemblerPersistsAndVerifiesDigest(t *testing.T) {
	root := t.TempDir()
	payload := []byte(strings.Repeat("0123456789", 1000))
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])
	src, dst := domains()
	intent := &spacev1.SpaceTransferIntent{ObjectMeta: metav1.ObjectMeta{Name: "intent"}, Spec: spacev1.SpaceTransferIntentSpec{TransferID: "transfer-one", MissionUID: "m", PlanID: "plan", Attempt: 1, Purpose: spacev1.TransferPurposeInput, Source: src, Destination: dst, DataID: "sensor", Bytes: int64(len(payload)), PayloadDigest: digest, ExpiresAt: metav1.NewTime(time.Now().Add(time.Hour))}}
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "sensor"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	chunks, err := ReadChunks(intent, source, 2048)
	if err != nil {
		t.Fatal(err)
	}
	assembler := &FileAssembler{Root: root, MaxBytes: 1 << 20}
	for i, c := range chunks {
		complete, ack, err := assembler.Accept(c)
		if err != nil {
			t.Fatal(err)
		}
		if i < len(chunks)-1 && complete {
			t.Fatal("completed before all chunks")
		}
		if i == len(chunks)-1 {
			if !complete || ack == nil || ack.PayloadDigest != digest {
				t.Fatalf("final complete=%v ack=%v", complete, ack)
			}
		}
	}
	stored, err := os.ReadFile(filepath.Join(root, "sensor"))
	if err != nil || string(stored) != string(payload) {
		t.Fatalf("assembled payload mismatch err=%v", err)
	}
}

func domains() (spacev1.DomainReference, spacev1.DomainReference) {
	return spacev1.DomainReference{Name: "ground", ClusterID: "g", OrbitClass: spacev1.OrbitGround}, spacev1.DomainReference{Name: "leo", ClusterID: "l", OrbitClass: spacev1.OrbitLEO}
}
