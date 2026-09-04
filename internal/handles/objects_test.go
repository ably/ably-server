package handles

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/live"
	"github.com/ably/server-protocol/go/state/statebuilder"
	"github.com/ably/server-protocol/go/wire"
)

// TestPublishState_MaterialisesAndSyncs drives one LiveObjects publish through
// the seam an attached client publishes over, and reads it back the way an
// attaching client is served it: through the module's state cache, which fills
// itself from this server's StatePage.
//
// What it proves is the whole round trip. The publisher is told the serial
// each operation was given, the operations are applied to the channel's object
// set rather than only logged, and a client syncing afterwards is sent the
// objects they made.
func TestPublishState_MaterialisesAndSyncs(t *testing.T) {
	ctx := t.Context()
	held := newTestChannel(t, "objects-test")

	objectID, state := testMapCreateAndSet("counter-holder")
	cm := &wire.ChannelMessage{
		Action:       wire.ProtocolMessageAction_ACTION_STATE,
		Id:           "pub-1",
		ConnectionId: "conn-1",
		State:        state,
	}

	acks := live.NewQueue[channel.Response]()
	if errInfo := held.live().Publish(ctx, cm, acks); errInfo != nil {
		t.Fatalf("publishing state: %s", errInfo)
	}

	// The ACK names each operation by the id the module derives for it, and
	// carries the serial storage minted — which is what an SDK waits for
	// before treating its own operation as applied.
	ack := popResponse(t, acks)
	if ack.Count != 2 {
		t.Errorf("ack count = %d, want 2 operations", ack.Count)
	}
	for i := range state {
		id := "pub-1:" + strconv.Itoa(i)
		if ack.Timeserials[id] == nil {
			t.Errorf("the ack carries no serial for operation %q: %v", id, ack.Timeserials)
		}
	}

	// The set a syncing client is served, read through the module's cache so
	// that what is checked is what a client would actually be sent.
	msgs, errInfo := held.ref.Get().Objects().GetState(ctx, channel.StateParams{
		ChannelID: "objects-test",
		SyncID:    "sync1",
		Limit:     10,
		SyncType:  channel.SyncTypeState,
	}, held.live().ChannelSerial())
	if errInfo != nil {
		t.Fatalf("GetState: %s", errInfo)
	}

	got := map[string]*wire.StateObject{}
	for _, msg := range msgs {
		for _, sm := range msg.State {
			if object := sm.GetObject(); object != nil {
				got[object.GetObjectId()] = object
			}
		}
	}
	if got[objectID] == nil {
		t.Fatalf("the created object %q is missing from the sync: %v", objectID, got)
	}
	root := got[wire.RootObjectID]
	if root == nil {
		t.Fatalf("the root the operation set a key on is missing from the sync: %v", got)
	}
	if ref := root.GetMap().GetEntries()["counter-holder"].GetData().GetObjectId(); ref != objectID {
		t.Errorf("root[counter-holder] points at %q, want the created object %q", ref, objectID)
	}
}

// TestPublishState_ReachesSubscribers checks the other half of a publish: a
// state cm goes onto the channel's live list and out through the cache an
// attachment reads, as a STATE frame rather than a message.
func TestPublishState_ReachesSubscribers(t *testing.T) {
	ctx := t.Context()
	held := newTestChannel(t, "objects-stream-test")

	s := held.messages().Stream()
	select {
	case <-s.Attached():
	case <-time.After(2 * time.Second):
		t.Fatal("stream never became attached")
	}

	_, state := testMapCreateAndSet("k")
	if errInfo := held.live().Publish(ctx, &wire.ChannelMessage{
		Action:       wire.ProtocolMessageAction_ACTION_STATE,
		Id:           "pub-1",
		ConnectionId: "conn-1",
		State:        state,
	}, nil); errInfo != nil {
		t.Fatalf("publishing state: %s", errInfo)
	}

	// A publish reaches the cache over the channel's own tail, so the stream
	// is read until it arrives rather than once.
	var delivered *channel.CachedEncodingChannelMessage
	deadline := time.Now().Add(2 * time.Second)
	for delivered == nil && time.Now().Before(deadline) {
		for s.Next() {
			if msg := s.Message(); msg != nil {
				delivered = msg
			}
		}
	}
	if delivered == nil {
		t.Fatal("the state publish never reached the stream")
	}
	if !delivered.IsState() {
		t.Fatalf("the publish came through as action %v, want a state frame", delivered.Action)
	}
	if len(delivered.State) != len(state) {
		t.Fatalf("delivered %d operations, want %d", len(delivered.State), len(state))
	}
	if delivered.State[0].GetSerial() == nil {
		t.Error("a delivered operation carries no serial for the client to order it by")
	}
}

// TestPublishState_IsNotMessageHistory checks that state rides the channel's
// stream without joining its message history: a client reading history back
// gets what was published to the channel, not the operations on its objects.
func TestPublishState_IsNotMessageHistory(t *testing.T) {
	ctx := t.Context()
	held := newTestChannel(t, "objects-history-test")

	if _, _, err := held.stored.Publish(ctx, []*wire.Message{{Name: new("ev")}}); err != nil {
		t.Fatalf("publishing a message: %s", err)
	}
	_, state := testMapCreateAndSet("k")
	if errInfo := held.live().Publish(ctx, &wire.ChannelMessage{
		Action:       wire.ProtocolMessageAction_ACTION_STATE,
		Id:           "pub-1",
		ConnectionId: "conn-1",
		State:        state,
	}, nil); errInfo != nil {
		t.Fatalf("publishing state: %s", errInfo)
	}

	page, errInfo := held.live().HistorySlices(ctx, channel.HistoryRange{Limit: 100})
	if errInfo != nil {
		t.Fatalf("HistorySlices: %s", errInfo)
	}
	for _, slice := range page.Slices {
		for _, cm := range slice.Messages {
			if cm.IsState() {
				t.Errorf("a state cm turned up in message history at %s", cm.ChannelSerial.ToTimeserialString())
			}
		}
	}
}

// testMapCreateAndSet is one object as a client makes one: a map create, and
// the set on the root under key that makes it reachable. It returns the object
// id the create derives, which is what the set points at.
func testMapCreateAndSet(key string) (string, []*wire.StateMessage) {
	create := statebuilder.NewMapOp(wire.StateOperation_MAP_CREATE, wire.Map_LWW)
	//lint:ignore SA1019 the nonce is a V1 field on the wire but is still what an object id is derived from
	create.Nonce = statebuilder.RandomNonce()
	objectID := statebuilder.InferObjectId(time.Now(), create)

	return objectID, []*wire.StateMessage{
		{Operation: create},
		{Operation: &wire.StateOperation{
			Action:   wire.StateOperation_MAP_SET,
			ObjectId: wire.RootObjectID,
			MapSet:   &wire.MapSet{Key: key, Value: &wire.StateData{ObjectId: &objectID}},
		}},
	}
}

// popResponse takes the one publish outcome the ack queue is expected to hold.
func popResponse(t *testing.T, q *live.Queue[channel.Response]) channel.Response {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	resp, err := q.Pop(ctx)
	if err != nil {
		t.Fatalf("waiting for the publish outcome: %s", err)
	}
	return resp
}

// TestServeWebSocket_ObjectsRoundTrip is the LiveObjects path end to end over a
// real connection: a client attaches asking for objects, is told a sync
// follows and sent one, publishes an operation, is ACKed with the serial it was
// given, and a second client attaching afterwards is synced the object the
// operation made.
//
// Everything between the frames is the shared module's. What this proves is
// that the two things it asks a server for — take the publish, serve the set —
// are answered well enough for the whole feature to work over the wire.
func TestServeWebSocket_ObjectsRoundTrip(t *testing.T) {
	stack := newTestServers(t)

	client := dial(t, stack)
	defer client.Close()
	readFrame(t, client) // CONNECTED

	// OBJECT_PUBLISH | OBJECT_SUBSCRIBE, the two modes LiveObjects needs.
	const objectModes = 1<<25 | 1<<24
	if err := client.WriteJSON(map[string]any{
		"action": actionAttach, "channel": "objects", "flags": objectModes,
	}); err != nil {
		t.Fatalf("ATTACH: %s", err)
	}

	attached := readFrame(t, client)
	if actionOf(attached) != actionAttached {
		t.Fatalf("response to ATTACH was action %v, want ATTACHED: %v", attached["action"], attached)
	}
	flags, _ := attached["flags"].(float64)
	if int(flags)&hasStateFlag == 0 {
		t.Errorf("ATTACHED flags = %v, want HAS_STATE set so the SDK expects a sync", attached["flags"])
	}

	// The sync for a channel with no objects yet: it still arrives, because
	// that is how the client learns the set is empty rather than unknown.
	if sync := readStateSync(t, client); sync == nil {
		t.Fatal("no STATE_SYNC arrived after an attach that asked for objects")
	}

	if err := client.WriteJSON(map[string]any{
		"action": actionState, "channel": "objects", "msgSerial": 0,
		"state": []map[string]any{{"operation": map[string]any{
			"action":   int(wire.StateOperation_MAP_SET),
			"objectId": wire.RootObjectID,
			"mapSet":   map[string]any{"key": "greeting", "value": map[string]any{"string": "hello"}},
		}}},
	}); err != nil {
		t.Fatalf("publishing an operation: %s", err)
	}

	ack := nextFrameOfAction(t, client, actionACK)
	res, _ := ack["res"].([]any)
	if len(res) == 0 {
		t.Fatalf("ACK carried no per-operation serials: %v", ack)
	}
	first, _ := res[0].(map[string]any)
	serials, _ := first["serials"].([]any)
	if len(serials) != 1 || serials[0] == nil {
		t.Fatalf("ACK carried no serial for the published operation: %v", ack)
	}
	t.Logf("published: ack serial=%v", serials[0])

	// A second client attaching afterwards is synced the object set the
	// operation left behind — which is the whole point of materialising it.
	other := dial(t, stack)
	defer other.Close()
	readFrame(t, other) // CONNECTED

	if err := other.WriteJSON(map[string]any{
		"action": actionAttach, "channel": "objects", "flags": objectModes,
	}); err != nil {
		t.Fatalf("second ATTACH: %s", err)
	}
	if attached := nextFrameOfAction(t, other, actionAttached); attached == nil {
		t.Fatal("the second client did not attach")
	}

	sync := readStateSync(t, other)
	if sync == nil {
		t.Fatal("no STATE_SYNC arrived for the second client")
	}
	root := syncedObject(sync, wire.RootObjectID)
	if root == nil {
		t.Fatalf("the root object is missing from the sync: %v", sync)
	}
	entries, _ := root["map"].(map[string]any)
	if entries == nil {
		t.Fatalf("the synced root is not a map: %v", root)
	}
	greeting := digStr(entries, "entries", "greeting", "data", "string")
	if greeting != "hello" {
		t.Errorf("root[greeting] synced as %q, want %q: %v", greeting, "hello", entries)
	}
}

// The wire actions this test adds to the ones presence_serve_test.go names.
const (
	actionAttach    = 10
	actionState     = 19
	actionStateSync = 20

	// HAS_STATE on ATTACHED.flags, which tells the SDK a STATE_SYNC follows.
	hasStateFlag = 1 << 7
)

// readStateSync reads until a STATE_SYNC arrives, skipping the presence sync
// every attachment is also offered.
func readStateSync(t *testing.T, client *websocket.Conn) map[string]any {
	t.Helper()
	return nextFrameOfAction(t, client, actionStateSync)
}

// nextFrameOfAction reads frames until one of the wanted action arrives, so a
// test asserting on one frame is not derailed by the syncs and heartbeats that
// share the connection with it.
func nextFrameOfAction(t *testing.T, client *websocket.Conn, want int) map[string]any {
	t.Helper()
	for range 10 {
		msg := readFrame(t, client)
		if actionOf(msg) == want {
			return msg
		}
	}
	t.Fatalf("no frame of action %d arrived", want)
	return nil
}

// syncedObject is the object with the given id carried by a STATE_SYNC frame.
func syncedObject(sync map[string]any, objectID string) map[string]any {
	state, _ := sync["state"].([]any)
	for _, entry := range state {
		sm, _ := entry.(map[string]any)
		object, _ := sm["object"].(map[string]any)
		if id, _ := object["objectId"].(string); id == objectID {
			return object
		}
	}
	return nil
}

// digStr walks a decoded JSON frame down a path of object keys and returns the
// string it ends at, or "" if the path does not lead to one.
func digStr(m map[string]any, path ...string) string {
	var cur any = m
	for _, key := range path {
		obj, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = obj[key]
	}
	s, _ := cur.(string)
	return s
}

// TestREST_ObjectsPublishThenRead drives the LiveObjects REST surface the
// shared module now carries: an operation goes in through the objects publish
// handler, and the objects it made come back out of the enumerate handler.
//
// This is the fixture setup both ably-js liveobjects suites do before any of
// their own tests run, and the route was the reason neither suite could start.
// The handlers are the module's; what this checks is that they fit over this
// server's storage — that an operation published over REST lands in the same
// materialised set a realtime publish would, and is served from it.
func TestREST_ObjectsPublishThenRead(t *testing.T) {
	stack := newTestServers(t)

	// A counter create, which is what the ably-js fixtures publish before any
	// of their own tests run. The nonce is fixed so the object id it derives
	// is too, which is what lets the read below name it.
	publish := restRequest(t, "POST", "/channels/rest-objects/objects", `{
		"operation": "COUNTER_CREATE",
		"nonce": "a-fixed-nonce-so-the-object-id-is-predictable",
		"data": {"value": 5}
	}`)
	rec := httptest.NewRecorder()
	stack.REST().HandleObjectOperation(rec, publish)
	if rec.Code != http.StatusOK && rec.Code != http.StatusCreated {
		t.Fatalf("publishing an object operation returned %d: %s", rec.Code, rec.Body)
	}
	t.Logf("published: %d %s", rec.Code, rec.Body.String())

	var published struct {
		Channel   string   `json:"channel"`
		ObjectIDs []string `json:"objectIds"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &published); err != nil {
		t.Fatalf("decoding the publish response (%s): %s", rec.Body, err)
	}
	if len(published.ObjectIDs) != 1 {
		t.Fatalf("the publish reported %d object ids, want the one it created: %s", len(published.ObjectIDs), rec.Body)
	}
	created := published.ObjectIDs[0]

	read := restRequest(t, "GET", "/channels/rest-objects/objects", "")
	rec = httptest.NewRecorder()
	stack.REST().HandleObjectsEnumerate(rec, read)
	if rec.Code != http.StatusOK {
		t.Fatalf("reading the channel's objects returned %d: %s", rec.Code, rec.Body)
	}
	t.Logf("objects: %s", rec.Body.String())

	// The created counter is in the set the read serves, which is the whole
	// point: a REST publish lands in the same materialised state a realtime
	// one does, and is served back out of it.
	if !strings.Contains(rec.Body.String(), created) {
		t.Errorf("the object %q the publish created is missing from the objects read back: %s", created, rec.Body)
	}
}
