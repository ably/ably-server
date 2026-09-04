//go:build integration

package integrationtest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/ably/ably-go/ably"

	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// restGetMessage fetches GET /channels/{channel}/messages/{serial} from
// addr (the single-message latest-version read has no SDK method) and
// decodes it into an ably.Message.
func restGetMessage(t *testing.T, addr, channel, serial string) *ably.Message {
	t.Helper()
	u := (&url.URL{Scheme: "http", Host: addr, Path: "/channels/" + channel + "/messages/" + serial}).String()
	req, _ := http.NewRequest(http.MethodGet, u, nil)
	req.SetBasicAuth("app.key", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("REST get message: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("REST get message on %s: status %d", addr, resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var m ably.Message
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode single message: %v (body %s)", err, body)
	}
	return &m
}

// recvMessage reads one message from ch or fails on the test deadline.
func recvMessage(t *testing.T, ctx context.Context, ch chan *ably.Message, what string) *ably.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s: %v", what, ctx.Err())
		return nil
	}
}

// collapsedHistory reads the channel's collapsed message history through
// the SDK (REST History → /channels/{name}/history) into a slice.
func collapsedHistory(t *testing.T, ctx context.Context, ch *ably.RealtimeChannel) []*ably.Message {
	t.Helper()
	items, err := ch.History().Items(ctx)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	var out []*ably.Message
	for items.Next(ctx) {
		out = append(out, items.Item())
	}
	if err := items.Err(); err != nil {
		t.Fatalf("History iterate: %v", err)
	}
	return out
}

// TestIntegrationMutableMessagesSDK drives create → update → delete and
// the version reads through the ably-go SDK against a real server, and
// asserts the wire shape: a stable serial across versions, a version
// object on each, the right action values, the version serial returned
// by each operation, and collapsed history vs getMessageVersions.
func TestIntegrationMutableMessagesSDK(t *testing.T) {
	addr := startServer(t, "--config="+editableChannelsConfig(t, "edits"))
	client := newClientWithID(t, addr, "alice")
	connect(t, client)

	ctx, cancel := testCtx(t)
	defer cancel()

	ch := client.Channels.Get("edits")
	received := make(chan *ably.Message, 8)
	unsub, err := ch.SubscribeAll(ctx, func(m *ably.Message) { received <- m })
	if err != nil {
		t.Fatalf("SubscribeAll: %v", err)
	}
	defer unsub()

	// --- create -------------------------------------------------------
	pubRes, err := ch.PublishWithResult(ctx, "post", "v1")
	if err != nil {
		t.Fatalf("PublishWithResult: %v", err)
	}
	if pubRes.Serial == nil || *pubRes.Serial == "" {
		t.Fatal("PublishWithResult returned no serial (ACK serials missing?)")
	}
	serial := *pubRes.Serial

	create := recvMessage(t, ctx, received, "create")
	if create.Action != ably.MessageActionCreate {
		t.Errorf("create action = %v, want MESSAGE_CREATE", create.Action)
	}
	if create.Serial != serial {
		t.Errorf("create serial = %q, want PublishWithResult serial %q", create.Serial, serial)
	}
	if create.Version == nil || create.Version.Serial != serial {
		t.Errorf("create version = %+v, want version.serial == serial %q", create.Version, serial)
	}

	// --- update -------------------------------------------------------
	updRes, err := ch.UpdateMessage(ctx, &ably.Message{Serial: serial, Data: "v2"}, ably.UpdateWithDescription("fix typo"))
	if err != nil {
		t.Fatalf("UpdateMessage: %v", err)
	}
	if updRes.VersionSerial == nil || *updRes.VersionSerial == "" || *updRes.VersionSerial == serial {
		t.Errorf("UpdateMessage VersionSerial = %q, want a fresh version serial != identity %q", deref(updRes.VersionSerial), serial)
	}
	update := recvMessage(t, ctx, received, "update")
	if update.Action != ably.MessageActionUpdate {
		t.Errorf("update action = %v, want MESSAGE_UPDATE", update.Action)
	}
	if update.Serial != serial {
		t.Errorf("update serial = %q, want stable identity %q", update.Serial, serial)
	}
	if got, _ := update.Data.(string); got != "v2" {
		t.Errorf("update data = %v, want v2", update.Data)
	}
	if update.Version == nil || update.Version.Serial != *updRes.VersionSerial {
		t.Errorf("update version = %+v, want serial %q (the operation's VersionSerial)", update.Version, deref(updRes.VersionSerial))
	}

	// --- delete -------------------------------------------------------
	delRes, err := ch.DeleteMessage(ctx, &ably.Message{Serial: serial})
	if err != nil {
		t.Fatalf("DeleteMessage: %v", err)
	}
	if delRes.VersionSerial == nil || *delRes.VersionSerial == "" {
		t.Error("DeleteMessage returned no version serial")
	}
	del := recvMessage(t, ctx, received, "delete")
	if del.Action != ably.MessageActionDelete {
		t.Errorf("delete action = %v, want MESSAGE_DELETE", del.Action)
	}
	if del.Serial != serial {
		t.Errorf("delete serial = %q, want stable identity %q", del.Serial, serial)
	}

	// --- version history (REST via SDK) -------------------------------
	var versions []*ably.Message
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		page, err := ch.GetMessageVersions(serial, url.Values{"direction": {"forwards"}}).Pages(ctx)
		if err == nil && page.Next(ctx) {
			versions = page.Items()
			if len(versions) == 3 {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(versions) != 3 {
		t.Fatalf("getMessageVersions = %d versions, want 3 (create + update + delete)", len(versions))
	}
	wantActions := []ably.MessageAction{ably.MessageActionCreate, ably.MessageActionUpdate, ably.MessageActionDelete}
	for i, v := range versions {
		if v.Serial != serial {
			t.Errorf("version[%d] serial = %q, want stable identity %q", i, v.Serial, serial)
		}
		if v.Action != wantActions[i] {
			t.Errorf("version[%d] action = %v, want %v", i, v.Action, wantActions[i])
		}
	}

	// --- collapsed history shows the tombstone ------------------------
	hist := collapsedHistory(t, ctx, ch)
	if len(hist) != 1 {
		t.Fatalf("collapsed history = %d messages, want 1 (deleted message still positioned)", len(hist))
	}
	if hist[0].Serial != serial || hist[0].Action != ably.MessageActionDelete {
		t.Errorf("collapsed history[0] = serial %q / action %v, want %q / MESSAGE_DELETE", hist[0].Serial, hist[0].Action, serial)
	}
}

// TestIntegrationClusterMutationAcrossNodes boots two servers on a shared
// schema: an update published on node A reaches a subscriber on node B
// via the NOTIFY path, and the collapsed history / latest version is
// consistent across both nodes after the mutation.
func TestIntegrationClusterMutationAcrossNodes(t *testing.T) {
	pgc := pgtest.Start(t)
	dsn := pgc.FreshSchemaDSN(t)
	cfg := "--config=" + editableChannelsConfig(t, "x")
	addrA := startServerOnDSN(t, dsn, cfg)
	addrB := startServerOnDSN(t, dsn, cfg)

	ctx, cancel := testCtx(t)
	defer cancel()

	// Publisher on A — never attaches; a mutation is a write to the
	// channel stream and needs no attachment, like a create publish.
	clientA := newClientWithID(t, addrA, "alice")
	connect(t, clientA)
	chA := clientA.Channels.Get("x")

	// Subscriber on B.
	clientB := newClient(t, addrB)
	connect(t, clientB)
	chB := clientB.Channels.Get("x")
	recvB := make(chan *ably.Message, 8)
	unsubB, err := chB.SubscribeAll(ctx, func(m *ably.Message) { recvB <- m })
	if err != nil {
		t.Fatalf("B SubscribeAll: %v", err)
	}
	defer unsubB()

	// Create on A; B observes it; the serial comes from the publish result.
	pubRes, err := chA.PublishWithResult(ctx, "post", "v1")
	if err != nil {
		t.Fatalf("A PublishWithResult: %v", err)
	}
	if pubRes.Serial == nil {
		t.Fatal("A publish returned no serial")
	}
	serial := *pubRes.Serial
	if bc := recvMessage(t, ctx, recvB, "B create"); bc.Serial != serial {
		t.Fatalf("B create serial = %q, want %q", bc.Serial, serial)
	}

	// Update on A → B must see it (cross-node delivery via NOTIFY).
	if _, err := chA.UpdateMessage(ctx, &ably.Message{Serial: serial, Data: "v2"}); err != nil {
		t.Fatalf("A UpdateMessage: %v", err)
	}
	updB := recvMessage(t, ctx, recvB, "B update")
	if updB.Action != ably.MessageActionUpdate {
		t.Errorf("B mutation action = %v, want MESSAGE_UPDATE", updB.Action)
	}
	if updB.Serial != serial {
		t.Errorf("B mutation serial = %q, want stable identity %q", updB.Serial, serial)
	}
	if got, _ := updB.Data.(string); got != "v2" {
		t.Errorf("B mutation data = %v, want v2", updB.Data)
	}

	// Collapsed history is consistent across nodes after the mutation:
	// both A and B show the one message at its latest version (v2).
	for _, tc := range []struct {
		name string
		ch   *ably.RealtimeChannel
	}{{"A", chA}, {"B", chB}} {
		hist := collapsedHistory(t, ctx, tc.ch)
		if len(hist) != 1 {
			t.Fatalf("node %s collapsed history = %d, want 1", tc.name, len(hist))
		}
		if hist[0].Serial != serial {
			t.Errorf("node %s history serial = %q, want %q", tc.name, hist[0].Serial, serial)
		}
		if got, _ := hist[0].Data.(string); got != "v2" {
			t.Errorf("node %s latest = %v, want v2", tc.name, hist[0].Data)
		}
	}

	// Single-message reads (GET .../messages/{serial}) are also consistent
	// across nodes — both resolve the shared latest-version projection.
	ma := restGetMessage(t, addrA, "x", serial)
	mb := restGetMessage(t, addrB, "x", serial)
	if ma.Serial != serial || mb.Serial != serial {
		t.Errorf("single-read serials = A:%q B:%q, want %q", ma.Serial, mb.Serial, serial)
	}
	da, _ := ma.Data.(string)
	db, _ := mb.Data.(string)
	if da != "v2" || db != "v2" {
		t.Errorf("single-read latest = A:%v B:%v, want v2 on both", ma.Data, mb.Data)
	}
}

// deref is an optional string as a test message prints it.
func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
