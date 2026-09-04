package server

import (
	"context"
	"net/http"
	"sync"
)

// connTracker cancels the contexts of in-flight requests, which is how a
// connection is shed.
//
// A websocket handler runs for as long as its connection lasts, so
// http.Server.Shutdown waits for it forever: it stops accepting and then
// blocks on handlers that have no reason to return. Cancelling the request's
// context is what gives them one, and the protocol code closes the transport
// when it sees that — the same way realtime sheds a connection.
type connTracker struct {
	mu      sync.Mutex
	cancels map[*http.Request]context.CancelFunc
}

func newConnTracker() *connTracker {
	return &connTracker{cancels: make(map[*http.Request]context.CancelFunc)}
}

// Handler wraps next so that each request it serves can be cancelled.
func (t *connTracker) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		r = r.WithContext(ctx)
		t.add(r, cancel)
		defer t.remove(r)

		next.ServeHTTP(w, r)
	})
}

// CloseAll cancels every request in flight, so their handlers return and the
// HTTP server's own shutdown can finish.
func (t *connTracker) CloseAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, cancel := range t.cancels {
		cancel()
	}
}

func (t *connTracker) add(r *http.Request, cancel context.CancelFunc) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cancels[r] = cancel
}

func (t *connTracker) remove(r *http.Request) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.cancels, r)
}
