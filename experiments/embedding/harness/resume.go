package main

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/protocol"
)

// scenarioResume proves resume-after-drop end to end, at the wire-protocol
// level so the drop is deterministic and independent of any SDK's internal
// reconnect timing:
//
//  1. open a WS, ATTACH fresh, and receive 3 live messages (publish via
//     REST); remember the last delivered channelSerial — the resume cursor
//  2. hard-close the WS (the "drop") and publish 3 more messages into the
//     gap while disconnected
//  3. open a NEW WS and ATTACH with the cursor; the server must set the
//     RESUMED flag and replay exactly the 3 gap messages, in order
//
// This exercises the server's computeResumeReplay path (internal/realtime)
// through whatever is in front of it. Through a Node/.NET proxy it also
// proves the proxy forwards a fresh WS upgrade and the resume handshake
// without mangling frames.
func scenarioResume(ctx context.Context, c config) (map[string]any, error) {
	channel := chanName("resume")

	// --- Phase 1: live attach, receive 3, capture the cursor. ---
	ws1, err := dialWS(c, channel)
	if err != nil {
		return nil, fmt.Errorf("dial #1: %w", err)
	}
	if err := awaitConnected(ws1, c.opTimeout); err != nil {
		ws1.Close()
		return nil, fmt.Errorf("connect #1: %w", err)
	}
	if err := sendAttach(ws1, channel, ""); err != nil {
		ws1.Close()
		return nil, fmt.Errorf("attach #1: %w", err)
	}
	if _, err := awaitAttached(ws1, channel, c.opTimeout); err != nil {
		ws1.Close()
		return nil, fmt.Errorf("attached #1: %w", err)
	}

	// Publish the pre-drop batch and read it back live to find the cursor.
	pre := []string{"m1", "m2", "m3"}
	for _, d := range pre {
		if err := restPublish(ctx, c, channel, "ev", d); err != nil {
			ws1.Close()
			return nil, fmt.Errorf("publish pre %s: %w", d, err)
		}
	}
	cursor := ""
	gotPre := make([]string, 0, len(pre))
	for len(gotPre) < len(pre) {
		frame, err := readFrame(ws1, c.opTimeout)
		if err != nil {
			ws1.Close()
			return nil, fmt.Errorf("read pre-drop frame: %w", err)
		}
		if frame.Action != protocol.ActionMessage || frame.Channel != channel {
			continue
		}
		for _, m := range frame.Messages {
			if d, ok := m.Data.(string); ok {
				gotPre = append(gotPre, d)
			}
		}
		cursor = frame.ChannelSerial
	}
	if cursor == "" {
		ws1.Close()
		return nil, fmt.Errorf("no channelSerial cursor captured from pre-drop messages")
	}

	// --- Phase 2: the drop. Publish the gap while disconnected. ---
	ws1.Close()
	gap := []string{"m4", "m5", "m6"}
	for _, d := range gap {
		if err := restPublish(ctx, c, channel, "ev", d); err != nil {
			return nil, fmt.Errorf("publish gap %s: %w", d, err)
		}
	}

	// --- Phase 3: resume from the cursor; expect exactly the gap. ---
	ws2, err := dialWS(c, channel)
	if err != nil {
		return nil, fmt.Errorf("dial #2: %w", err)
	}
	defer ws2.Close()
	if err := awaitConnected(ws2, c.opTimeout); err != nil {
		return nil, fmt.Errorf("connect #2: %w", err)
	}
	if err := sendAttach(ws2, channel, cursor); err != nil {
		return nil, fmt.Errorf("attach #2 (resume): %w", err)
	}
	attached, err := awaitAttached(ws2, channel, c.opTimeout)
	if err != nil {
		return nil, fmt.Errorf("attached #2: %w", err)
	}
	resumed := attached.Flags&protocol.FlagResumed != 0
	if !resumed {
		return nil, fmt.Errorf("RESUMED flag not set on re-attach (flags=%d, err=%v)", attached.Flags, attached.Error)
	}

	// Read exactly len(gap) replayed messages, asserting order and that
	// nothing from before the cursor leaks through.
	gotGap := make([]string, 0, len(gap))
	for len(gotGap) < len(gap) {
		frame, err := readFrame(ws2, c.opTimeout)
		if err != nil {
			return nil, fmt.Errorf("read replay frame: %w", err)
		}
		if frame.Action != protocol.ActionMessage || frame.Channel != channel {
			continue
		}
		for _, m := range frame.Messages {
			if d, ok := m.Data.(string); ok {
				gotGap = append(gotGap, d)
			}
		}
	}
	for i, want := range gap {
		if gotGap[i] != want {
			return nil, fmt.Errorf("replay[%d] = %q, want %q (full=%v)", i, gotGap[i], want, gotGap)
		}
	}

	return map[string]any{
		"cursor":      cursor,
		"resumedFlag": resumed,
		"gapSize":     len(gap),
		"delivered":   len(gotGap),
		"lossless":    true,
	}, nil
}

// dialWS opens a raw WebSocket to the endpoint's root, authenticating with
// the API key in the query string (the server accepts ?key=…) and pinning
// the JSON protocol so we can speak internal/protocol over text frames.
func dialWS(c config, _ string) (*websocket.Conn, error) {
	q := url.Values{}
	q.Set("key", c.key)
	q.Set("format", "json")
	u := url.URL{Scheme: "ws", Host: c.addr(), Path: "/", RawQuery: q.Encode()}
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, resp, err := dialer.Dial(u.String(), nil)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("ws dial: %w (http %d)", err, resp.StatusCode)
		}
		return nil, fmt.Errorf("ws dial: %w", err)
	}
	return conn, nil
}

// sendAttach writes an ATTACH frame; a non-empty cursor requests a resume
// from that channelSerial.
func sendAttach(ws *websocket.Conn, channel, cursor string) error {
	frame := &protocol.ProtocolMessage{
		Action:        protocol.ActionAttach,
		Channel:       channel,
		ChannelSerial: cursor,
	}
	data, err := protocol.Marshal(frame, protocol.FormatJSON)
	if err != nil {
		return err
	}
	return ws.WriteMessage(websocket.TextMessage, data)
}

// readFrame reads and decodes one protocol frame, skipping HEARTBEATs.
func readFrame(ws *websocket.Conn, timeout time.Duration) (*protocol.ProtocolMessage, error) {
	for {
		_ = ws.SetReadDeadline(time.Now().Add(timeout))
		_, data, err := ws.ReadMessage()
		if err != nil {
			return nil, err
		}
		var m protocol.ProtocolMessage
		if err := protocol.Unmarshal(data, protocol.FormatJSON, &m); err != nil {
			return nil, fmt.Errorf("decode frame: %w", err)
		}
		if m.Action == protocol.ActionHeartbeat {
			continue
		}
		return &m, nil
	}
}

// awaitConnected reads until the CONNECTED frame.
func awaitConnected(ws *websocket.Conn, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		frame, err := readFrame(ws, timeout)
		if err != nil {
			return err
		}
		if frame.Action == protocol.ActionConnected {
			return nil
		}
		if frame.Action == protocol.ActionError {
			return fmt.Errorf("server ERROR before CONNECTED: %v", frame.Error)
		}
	}
	return fmt.Errorf("timeout waiting for CONNECTED")
}

// awaitAttached reads until the ATTACHED for channel, returning it.
func awaitAttached(ws *websocket.Conn, channel string, timeout time.Duration) (*protocol.ProtocolMessage, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		frame, err := readFrame(ws, timeout)
		if err != nil {
			return nil, err
		}
		if frame.Action == protocol.ActionAttached && frame.Channel == channel {
			return frame, nil
		}
		if frame.Action == protocol.ActionError {
			return nil, fmt.Errorf("server ERROR: %v", frame.Error)
		}
	}
	return nil, fmt.Errorf("timeout waiting for ATTACHED on %q", channel)
}
