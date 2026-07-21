package realtime

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/protocol"
)

// TestCrossFormatBinaryFanout exercises the core cross-format case end-to-end
// through the realtime server: a binary payload published by a msgpack
// connection, fanned out to a JSON connection, must arrive as a base64 string
// with "base64" appended to the encoding (so a JSON SDK decodes it back to
// bytes). This is the server-side analogue of ably-js's
// crypto single_send_binary_text.
func TestCrossFormatBinaryFanout(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	deadbeef := []byte{0xde, 0xad, 0xbe, 0xef}
	const deadbeefBase64 = "3q2+7w==" // base64.StdEncoding of deadbeef

	// JSON subscriber.
	sub := dial(t, srv, "json")
	drainConnected(t, sub)
	attach(t, sub, "bin", protocol.FlagSubscribe)

	// msgpack publisher.
	pub := dial(t, srv, "msgpack")
	if f := readFrame(t, pub, protocol.FormatMsgpack, 2*time.Second); f.Action != protocol.ActionConnected {
		t.Fatalf("msgpack first frame = %v, want CONNECTED", f.Action)
	}
	sendFrame(t, pub, protocol.FormatMsgpack, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: new("bin"),
		Flags:   protocol.FlagPublish,
	})
	if f := readFrame(t, pub, protocol.FormatMsgpack, 2*time.Second); f.Action != protocol.ActionAttached {
		t.Fatalf("msgpack attach response = %v, want ATTACHED", f.Action)
	}

	// Publish the raw binary payload over msgpack with a cipher-style
	// encoding so we also confirm the suffix is appended (not replaced).
	sendFrame(t, pub, protocol.FormatMsgpack, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   new("bin"),
		MsgSerial: msgSerialPtr(1),
		Messages:  []*protocol.Message{{Data: deadbeef, Encoding: "utf-8/cipher+aes-256-cbc"}},
	})

	// Read raw JSON frames on the subscriber (bypassing Message.UnmarshalJSON,
	// which would normalise the base64 away) to inspect the exact wire shape.
	data, encoding := readRawDeliveredMessage(t, sub)
	if data != deadbeefBase64 {
		t.Errorf("delivered data = %q, want %q (base64 of the raw bytes)", data, deadbeefBase64)
	}
	if !strings.HasSuffix(encoding, "base64") {
		t.Errorf("delivered encoding = %q, want a trailing base64 step", encoding)
	}
	if encoding != "utf-8/cipher+aes-256-cbc/base64" {
		t.Errorf("delivered encoding = %q, want %q", encoding, "utf-8/cipher+aes-256-cbc/base64")
	}
}

// readRawDeliveredMessage reads raw JSON frames until it sees a MESSAGE
// delivery, returning the first message's data and encoding as they appear on
// the wire (strings), without round-tripping through Message.UnmarshalJSON.
func readRawDeliveredMessage(t *testing.T, ws *websocket.Conn) (data, encoding string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := ws.SetReadDeadline(deadline); err != nil {
			t.Fatalf("SetReadDeadline: %v", err)
		}
		_, raw, err := ws.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var frame struct {
			Action   int `json:"action"`
			Messages []struct {
				Data     string `json:"data"`
				Encoding string `json:"encoding"`
			} `json:"messages"`
		}
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("unmarshal raw frame %q: %v", raw, err)
		}
		if protocol.Action(frame.Action) == protocol.ActionMessage && len(frame.Messages) > 0 {
			return frame.Messages[0].Data, frame.Messages[0].Encoding
		}
	}
	t.Fatal("never received a MESSAGE delivery")
	return "", ""
}
