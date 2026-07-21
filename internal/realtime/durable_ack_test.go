package realtime

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/memory"
)

// controlStorage wraps a real backend so a test can gate (block) or fail
// the Store path, exercising the off-loop publish worker.
type controlStorage struct {
	inner storage.Storage

	mu      sync.Mutex
	gate    chan struct{} // Store blocks receiving from this until closed; nil passes through
	failErr error         // when set, Store returns it without delegating
}

func (s *controlStorage) Channel(ctx context.Context, name string, ap storage.Appender) (storage.ChannelStore, error) {
	cs, err := s.inner.Channel(ctx, name, ap)
	if err != nil {
		return nil, err
	}
	return &controlStore{parent: s, ChannelStore: cs}, nil
}

func (s *controlStorage) Close() error { return s.inner.Close() }

func (s *controlStorage) setGate(g chan struct{}) {
	s.mu.Lock()
	s.gate = g
	s.mu.Unlock()
}

func (s *controlStorage) setFail(err error) {
	s.mu.Lock()
	s.failErr = err
	s.mu.Unlock()
}

type controlStore struct {
	parent *controlStorage
	storage.ChannelStore
}

func (c *controlStore) Store(ctx context.Context, msgs []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
	c.parent.mu.Lock()
	gate, failErr := c.parent.gate, c.parent.failErr
	c.parent.mu.Unlock()
	if gate != nil {
		<-gate
	}
	if failErr != nil {
		return nil, false, failErr
	}
	return c.ChannelStore.Store(ctx, msgs)
}

// newControlServer builds a realtime server backed by controlStorage.
func newControlServer(t *testing.T, hb time.Duration) (*httptest.Server, *controlStorage) {
	t.Helper()
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	cs := &controlStorage{inner: memory.New(memory.Options{})}
	manager := core.NewManager(cs)
	rt := NewServer([]auth.APIKey{parsed}, manager, hb, logging.New(slog.DiscardHandler), nil, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, cs
}

func sendPublish(t *testing.T, ws *websocket.Conn, channel string, msgSerial int64, data string) {
	t.Helper()
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   new(channel),
		MsgSerial: msgSerialPtr(msgSerial),
		Messages:  []*protocol.Message{{Data: data}},
	})
}

// TestPublishAckAfterCommitNonBlocking: while a publish's store is gated
// (in flight), the read loop stays responsive — an ATTACH is answered
// promptly — and the ACK is emitted only once the store commits.
func TestPublishAckAfterCommitNonBlocking(t *testing.T) {
	srv, cs := newControlServer(t, time.Hour)
	gate := make(chan struct{})
	cs.setGate(gate)

	ws := dialClient(t, srv, "alice")
	drainConnected(t, ws)
	attach(t, ws, "room", protocol.FlagPublish)

	// Publish; its store blocks on the gate.
	sendPublish(t, ws, "room", 1, "v1")

	// The read loop must not be blocked by the in-flight store: an ATTACH
	// to another channel is answered while the store is still gated.
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: new("other"),
	})
	if f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionAttached || f.GetChannel() != "other" {
		t.Fatalf("read loop blocked on in-flight store: got %v/%q, want ATTACHED/other", f.Action, f.GetChannel())
	}

	// The publish's ACK cannot have been emitted yet: the worker emits it
	// only after Store returns, which is still gated. Releasing the gate
	// lets storage commit; the ACK follows (ACK-after-durable-commit).
	close(gate)
	ack := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if ack.Action != protocol.ActionAck || ack.GetMsgSerial() != 1 {
		t.Fatalf("post-commit frame = %v/msgSerial %d, want ACK/1", ack.Action, ack.GetMsgSerial())
	}
}

// TestPublishNackOnStoreFailure: a failed durable write NACKs the publish
// rather than optimistically ACKing.
func TestPublishNackOnStoreFailure(t *testing.T) {
	srv, cs := newControlServer(t, time.Hour)
	cs.setFail(errors.New("disk on fire"))

	ws := dialClient(t, srv, "alice")
	drainConnected(t, ws)
	attach(t, ws, "room", protocol.FlagPublish)

	sendPublish(t, ws, "room", 1, "v1")
	f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionNack || f.GetMsgSerial() != 1 {
		t.Fatalf("frame = %v/msgSerial %d, want NACK/1 on store failure", f.Action, f.GetMsgSerial())
	}
}

// TestPublishAckOrderingUnderSlowStore: two publishes whose stores are
// released together still draw ACKs in msgSerial order — the FIFO worker
// preserves per-connection ACK ordering.
func TestPublishAckOrderingUnderSlowStore(t *testing.T) {
	srv, cs := newControlServer(t, time.Hour)
	gate := make(chan struct{})
	cs.setGate(gate)

	ws := dialClient(t, srv, "alice")
	drainConnected(t, ws)
	attach(t, ws, "room", protocol.FlagPublish)

	sendPublish(t, ws, "room", 1, "first")
	sendPublish(t, ws, "room", 2, "second")

	// Both stores are gated; release them together.
	close(gate)

	first := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	second := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if first.Action != protocol.ActionAck || first.GetMsgSerial() != 1 {
		t.Fatalf("first ACK = %v/msgSerial %d, want ACK/1", first.Action, first.GetMsgSerial())
	}
	if second.Action != protocol.ActionAck || second.GetMsgSerial() != 2 {
		t.Fatalf("second ACK = %v/msgSerial %d, want ACK/2", second.Action, second.GetMsgSerial())
	}
}
