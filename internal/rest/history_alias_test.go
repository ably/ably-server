package rest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/ably/ably-server/internal/protocol"
)

// TestHistoryAliasRoute: GET /channels/{name}/history (the path ably-go's
// REST History() uses, TASK-57) returns the same collapsed message
// history as GET /channels/{name}/messages.
func TestHistoryAliasRoute(t *testing.T) {
	srv, _ := newTestServer(t)
	publishOne(t, srv, "room", &protocol.Message{Name: "n", Data: "hello"})

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		srv.URL+"/channels/room/history?direction=forwards", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.SetBasicAuth("app.key", "secret")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("GET /history: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/history status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var msgs []*protocol.Message
	if err := json.Unmarshal(body, &msgs); err != nil {
		t.Fatalf("decode: %v (body %s)", err, body)
	}
	if len(msgs) != 1 || msgs[0].Data != "hello" {
		t.Fatalf("/history = %+v, want one message with data 'hello'", msgs)
	}
}
