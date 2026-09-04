package handles

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/config"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage/memory"

	"github.com/gorilla/websocket"

	"github.com/ably/server-protocol/go/conf"
)

const testKey = "ably-server.keyid:keysecret"

// TestServeWebSocket_Connects stands the shared WebSocket transport and
// connection manager up against this server and connects a real client to it.
//
// Everything from the upgrade to the CONNECTED frame is the shared code's; what
// this server supplies is the app, its keys, its channels and its logger. It is
// the whole connection seam answered at once, which is the point — the pieces
// compiling separately says less than one client getting a connection id.
func TestServeWebSocket_Connects(t *testing.T) {
	client := dial(t, newTestServers(t))
	defer client.Close()

	msg := readFrame(t, client)
	if action, _ := msg["action"].(float64); int(action) != 4 {
		t.Fatalf("first frame was action %v, want CONNECTED (4): %v", msg["action"], msg)
	}

	details, _ := msg["connectionDetails"].(map[string]any)
	if details == nil {
		t.Fatalf("CONNECTED carried no connectionDetails: %v", msg)
	}
	if id, _ := msg["connectionId"].(string); id == "" {
		t.Errorf("CONNECTED carried no connectionId: %v", msg)
	}
	// The key a client resumes with is in the connection details, and carries
	// the instance that minted it ahead of the signed part.
	key, _ := details["connectionKey"].(string)
	if !strings.Contains(key, "!") {
		t.Errorf("connectionKey = %q, want one carrying the instance that minted it", key)
	}
	// The site code a client is told has to be the one its serials carry: a
	// LiveObjects client applying its own operation on the ACK keys it by
	// this, and the echo that follows keys it by the serial's (DESIGN.md
	// §15.1).
	if site, _ := details["siteCode"].(string); site != serial.SiteCode {
		t.Errorf("siteCode = %q, want %q — the one every serial carries", site, serial.SiteCode)
	}
	t.Logf("connected: id=%v key=%v serverId=%v", msg["connectionId"], key, details["serverId"])
}

// TestServeWebSocket_AttachesToAChannel takes the same connection through an
// ATTACH, which runs the shared attachment against this server's storage.
func TestServeWebSocket_AttachesToAChannel(t *testing.T) {
	client := dial(t, newTestServers(t))
	defer client.Close()

	readFrame(t, client) // CONNECTED

	if err := client.WriteJSON(map[string]any{"action": 10, "channel": "probe"}); err != nil {
		t.Fatalf("sending ATTACH: %s", err)
	}

	msg := readFrame(t, client)
	action, _ := msg["action"].(float64)
	if int(action) != 11 {
		t.Fatalf("response to ATTACH was action %v, want ATTACHED (11): %v", msg["action"], msg)
	}
	if ch, _ := msg["channel"].(string); ch != "probe" {
		t.Errorf("ATTACHED for channel %q, want %q", ch, "probe")
	}
	t.Logf("attached: channel=%v channelSerial=%v flags=%v",
		msg["channel"], msg["channelSerial"], msg["flags"])
}

// testStack is the shared protocol code mounted on a real HTTP server, which
// is what a client connects to.
type testStack struct {
	*Protocol
	url string
}

func newTestServers(t *testing.T) *testStack {
	t.Helper()

	key, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parsing the test key: %s", err)
	}
	// Two configured namespaces, so tests can check that a channel falling in
	// one is given its settings rather than the defaults. The second is a
	// match expression rather than a name segment, and a leading wildcard at
	// that, so nothing but the generalised rule can select the channels it
	// applies to.
	app, err := NewApp("ably-server", []auth.APIKey{key}, []config.Namespace{
		{ID: "mutable", MutableMessages: true},
		{ID: "*:edits", Mode: "matcher", MutableMessages: true},
	}, nil)
	if err != nil {
		t.Fatalf("building the app: %s", err)
	}

	// Quiet unless something goes wrong; raise this to LevelTrace to watch a
	// connection being served.
	log := logging.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level:       slog.LevelWarn,
		ReplaceAttr: logging.ReplaceAttr,
	}))
	c := conf.Default()
	c.Auth.ConnectionMacKey.Store([]byte("a-secret-for-signing-connection-keys"))

	channels := core.NewManager(memory.New(memory.Options{}))
	p, err := New(
		t.Context(),
		c,
		NewManager(app, channels, nil, log),
		NewChannelManager(channels, c, app.Namespaces(), log),
		NewAuthManager(c.Auth, log),
		log,
	)
	if err != nil {
		t.Fatalf("assembling the shared protocol code: %s", err)
	}
	t.Cleanup(p.Close)

	// The whole protocol surface, at the paths the module puts it at, which is
	// how a client reaches it in this server too.
	mux := http.NewServeMux()
	for _, route := range p.Routes() {
		mux.Handle(route.Pattern, route.Handler)
	}
	httpServer := httptest.NewServer(mux)
	t.Cleanup(httpServer.Close)
	return &testStack{Protocol: p, url: httpServer.URL}
}

func dial(t *testing.T, stack *testStack) *websocket.Conn {
	t.Helper()

	// The newest protocol version, so a probe exercises what a current client
	// gets rather than the version the server falls back to when a client
	// names none.
	url := "ws" + strings.TrimPrefix(stack.url, "http") + "/?v=6&key=" + testKey
	// Basic auth over a plaintext transport is refused, as it should be, so
	// the request declares the TLS termination a proxy would have done.
	headers := http.Header{}
	headers.Set("X-Forwarded-Proto", "https")

	client, resp, err := websocket.DefaultDialer.Dial(url, headers)
	if err != nil {
		if resp != nil {
			t.Fatalf("dialing the shared websocket server: %s (HTTP %s)", err, resp.Status)
		}
		t.Fatalf("dialing the shared websocket server: %s", err)
	}
	return client
}

func readFrame(t *testing.T, client *websocket.Conn) map[string]any {
	t.Helper()
	if err := client.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("setting a read deadline: %s", err)
	}
	_, data, err := client.ReadMessage()
	if err != nil {
		t.Fatalf("reading a frame: %s", err)
	}
	var msg map[string]any
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("decoding a frame (%s): %s", data, err)
	}
	return msg
}

// TestServeWebSocket_PublishAndReceive is the whole path in one: a client
// publishes over the shared connection, the shared publisher validates it, it
// is committed to this server's storage, read back out through the shared
// stream, and delivered to a subscriber on another connection.
//
// It is the question the handles compiling could not answer — whether this
// server's storage model actually fits behind them.
func TestServeWebSocket_PublishAndReceive(t *testing.T) {
	stack := newTestServers(t)

	subscriber := dial(t, stack)
	defer subscriber.Close()
	readFrame(t, subscriber) // CONNECTED

	if err := subscriber.WriteJSON(map[string]any{"action": 10, "channel": "pubsub"}); err != nil {
		t.Fatalf("subscriber ATTACH: %s", err)
	}
	if attached := readFrame(t, subscriber); actionOf(attached) != 11 {
		t.Fatalf("subscriber did not attach: %v", attached)
	}

	publisher := dial(t, stack)
	defer publisher.Close()
	readFrame(t, publisher) // CONNECTED

	msgSerial := 0
	err := publisher.WriteJSON(map[string]any{
		"action":    15,
		"channel":   "pubsub",
		"msgSerial": msgSerial,
		"messages":  []map[string]any{{"name": "greeting", "data": "hello"}},
	})
	if err != nil {
		t.Fatalf("publishing: %s", err)
	}

	// The publisher is ACKed once storage has committed, and told the serial
	// the message was given.
	ack := readFrame(t, publisher)
	if actionOf(ack) != 1 {
		t.Fatalf("publisher got action %v, want ACK (1): %v", ack["action"], ack)
	}
	res, _ := ack["res"].([]any)
	if len(res) == 0 {
		t.Fatalf("ACK carried no per-message serials: %v", ack)
	}
	first, _ := res[0].(map[string]any)
	serials, _ := first["serials"].([]any)
	if len(serials) != 1 || serials[0] == nil {
		t.Fatalf("ACK carried no serial for the published message: %v", ack)
	}
	t.Logf("published: ack serial=%v", serials[0])

	// And the subscriber is sent it. Every attachment is offered a presence
	// sync first — this server always says a channel may have presence, since
	// looking costs what deciding whether to look would — so skip past it.
	msg := readFrame(t, subscriber)
	if actionOf(msg) == actionSync {
		msg = readFrame(t, subscriber)
	}
	if actionOf(msg) != 15 {
		t.Fatalf("subscriber got action %v, want MESSAGE (15): %v", msg["action"], msg)
	}
	if ch, _ := msg["channel"].(string); ch != "pubsub" {
		t.Errorf("delivered on channel %q, want %q", ch, "pubsub")
	}
	messages, _ := msg["messages"].([]any)
	if len(messages) != 1 {
		t.Fatalf("delivered %d messages, want 1: %v", len(messages), msg)
	}
	delivered, _ := messages[0].(map[string]any)
	if name, _ := delivered["name"].(string); name != "greeting" {
		t.Errorf("delivered name %q, want %q", name, "greeting")
	}
	if data, _ := delivered["data"].(string); data != "hello" {
		t.Errorf("delivered data %q, want %q", data, "hello")
	}
	if serial, _ := delivered["serial"].(string); serial == "" {
		t.Errorf("delivered message carried no serial: %v", delivered)
	}
	t.Logf("delivered: %v", delivered)
}

func actionOf(msg map[string]any) int {
	action, _ := msg["action"].(float64)
	return int(action)
}

// TestServeWebSocket_DeliveredMessagesCarryTheirPublisher checks that a
// delivered message says which connection sent it.
//
// A client reads it to tell its own messages from other people's, and the
// protocol code reads it to honour echo=false — so a message that crosses this
// server's storage without it is delivered to a connection that asked not to
// be sent its own.
func TestServeWebSocket_DeliveredMessagesCarryTheirPublisher(t *testing.T) {
	stack := newTestServers(t)

	subscriber := dial(t, stack)
	defer subscriber.Close()
	readFrame(t, subscriber) // CONNECTED
	if err := subscriber.WriteJSON(map[string]any{"action": 10, "channel": "publisher-id"}); err != nil {
		t.Fatalf("subscriber ATTACH: %s", err)
	}
	if attached := readFrame(t, subscriber); actionOf(attached) != 11 {
		t.Fatalf("subscriber did not attach: %v", attached)
	}

	publisher := dial(t, stack)
	defer publisher.Close()
	connected := readFrame(t, publisher)
	publisherConnID, _ := connected["connectionId"].(string)
	if publisherConnID == "" {
		t.Fatalf("CONNECTED carried no connectionId: %v", connected)
	}

	if err := publisher.WriteJSON(map[string]any{
		"action":    15,
		"channel":   "publisher-id",
		"msgSerial": 0,
		"messages":  []map[string]any{{"name": "greeting", "data": "hello"}},
	}); err != nil {
		t.Fatalf("publishing: %s", err)
	}

	msg := readFrame(t, subscriber)
	if actionOf(msg) == actionSync {
		msg = readFrame(t, subscriber)
	}
	if actionOf(msg) != 15 {
		t.Fatalf("subscriber got action %v, want MESSAGE (15): %v", msg["action"], msg)
	}
	messages, _ := msg["messages"].([]any)
	if len(messages) != 1 {
		t.Fatalf("delivered %d messages, want 1: %v", len(messages), msg)
	}
	delivered, _ := messages[0].(map[string]any)
	if got, _ := delivered["connectionId"].(string); got != publisherConnID {
		t.Errorf("delivered message connectionId = %q, want the publisher's %q", got, publisherConnID)
	}
}
