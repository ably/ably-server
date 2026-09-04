package handles

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestREST_PublishThenHistory takes a message in through the shared REST
// publish handler and reads it back through the shared history handler, both
// against this server's storage.
//
// This is the read half of the same question the realtime path answered for
// writes: whether this server's storage fits behind the handles, or only
// compiles against them.
func TestREST_PublishThenHistory(t *testing.T) {
	stack := newTestServers(t)

	publish := restRequest(t, "POST", "/channels/rest-probe/messages",
		`{"name":"greeting","data":"hello"}`)
	rec := httptest.NewRecorder()
	stack.REST().HandlePublish(rec, publish)
	if rec.Code != http.StatusCreated && rec.Code != http.StatusOK {
		t.Fatalf("publish returned %d: %s", rec.Code, rec.Body)
	}
	t.Logf("published: %d %s", rec.Code, rec.Body.String())

	history := restRequest(t, "GET", "/channels/rest-probe/messages", "")
	rec = httptest.NewRecorder()
	stack.REST().HandleHistory(rec, history)
	if rec.Code != http.StatusOK {
		t.Fatalf("history returned %d: %s", rec.Code, rec.Body)
	}

	var messages []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &messages); err != nil {
		t.Fatalf("decoding history (%s): %s", rec.Body, err)
	}
	if len(messages) != 1 {
		t.Fatalf("history returned %d messages, want 1: %s", len(messages), rec.Body)
	}
	if name, _ := messages[0]["name"].(string); name != "greeting" {
		t.Errorf("history message name = %q, want %q", name, "greeting")
	}
	if data, _ := messages[0]["data"].(string); data != "hello" {
		t.Errorf("history message data = %q, want %q", data, "hello")
	}
	if serial, _ := messages[0]["serial"].(string); serial == "" {
		t.Errorf("history message carried no serial: %v", messages[0])
	}
	t.Logf("history: %v", messages[0])
}

// TestREST_HistoryOnAnEmptyChannel checks the shape a client gets when there
// is nothing to return, which is a page rather than an error.
func TestREST_HistoryOnAnEmptyChannel(t *testing.T) {
	stack := newTestServers(t)

	rec := httptest.NewRecorder()
	stack.REST().HandleHistory(rec, restRequest(t, "GET", "/channels/empty-probe/messages", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("history returned %d: %s", rec.Code, rec.Body)
	}

	var messages []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &messages); err != nil {
		t.Fatalf("decoding history (%s): %s", rec.Body, err)
	}
	if len(messages) != 0 {
		t.Errorf("history on an empty channel returned %d messages", len(messages))
	}
}

func restRequest(t *testing.T, method, path, body string) *http.Request {
	t.Helper()
	// The key goes in the query, so a path that already carries one is
	// extended rather than given a second "?".
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	req := httptest.NewRequest(method, path+sep+"key="+testKey, stringReader(body))
	req.Header.Set("X-Forwarded-Proto", "https")
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.SetPathValue("channelId", channelOf(path))
	return req
}

func stringReader(s string) *stringsReader { return &stringsReader{s: s} }

type stringsReader struct {
	s string
	i int
}

func (r *stringsReader) Read(p []byte) (int, error) {
	if r.i >= len(r.s) {
		return 0, errEOF
	}
	n := copy(p, r.s[r.i:])
	r.i += n
	return n, nil
}

var errEOF = errorString("EOF")

type errorString string

func (e errorString) Error() string { return string(e) }

func channelOf(path string) string {
	// /channels/<name>/messages, with any query string ignored
	const prefix = "/channels/"
	if i := strings.Index(path, "?"); i >= 0 {
		path = path[:i]
	}
	rest := path[len(prefix):]
	for i := 0; i < len(rest); i++ {
		if rest[i] == '/' {
			return rest[:i]
		}
	}
	return rest
}

// TestREST_HistoryDirection checks which end of the channel a history page
// starts from.
//
// Backwards — newest first — is what a client gets by default and what most
// SDK history reads ask for, so reading the direction flag the wrong way round
// returns every page in the opposite order to the one requested while looking
// entirely healthy: the right messages, the right count, the wrong end first.
func TestREST_HistoryDirection(t *testing.T) {
	stack := newTestServers(t)

	for _, name := range []string{"first", "second", "third"} {
		rec := httptest.NewRecorder()
		stack.REST().HandlePublish(rec, restRequest(t, "POST", "/channels/direction/messages",
			`{"name":"`+name+`","data":"x"}`))
		if rec.Code != http.StatusCreated && rec.Code != http.StatusOK {
			t.Fatalf("publishing %s returned %d: %s", name, rec.Code, rec.Body)
		}
	}

	for _, tc := range []struct {
		query string
		want  []string
	}{
		{"", []string{"third", "second", "first"}},
		{"?direction=backwards", []string{"third", "second", "first"}},
		{"?direction=forwards", []string{"first", "second", "third"}},
	} {
		rec := httptest.NewRecorder()
		stack.REST().HandleHistory(rec, restRequest(t, "GET", "/channels/direction/messages"+tc.query, ""))
		if rec.Code != http.StatusOK {
			t.Fatalf("history%s returned %d: %s", tc.query, rec.Code, rec.Body)
		}

		var messages []map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &messages); err != nil {
			t.Fatalf("decoding history%s (%s): %s", tc.query, rec.Body, err)
		}
		var got []string
		for _, m := range messages {
			name, _ := m["name"].(string)
			got = append(got, name)
		}
		if len(got) != len(tc.want) {
			t.Fatalf("history%s returned %v, want %v", tc.query, got, tc.want)
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Errorf("history%s returned %v, want %v", tc.query, got, tc.want)
				break
			}
		}
	}
}

// TestREST_ChannelTakesItsNamespaceSettings checks that a channel is given the
// settings configured for the namespace its name falls in.
//
// Every channel used to be handed one empty namespace, so a channel in a
// namespace with mutableMessages was still refused an update — this server was
// configured to allow something it then said it could not do. Eight ably-js
// tests failed on it, none of them about namespaces.
func TestREST_ChannelTakesItsNamespaceSettings(t *testing.T) {
	assertChannelMayBeEdited(t, "mutable:edits")
}

// TestREST_ChannelTakesAMatcherNamespacesSettings is the same check for a
// namespace whose id is a match expression rather than a name segment
// (generalised channel rules).
//
// The channel is named so that only the match expression can select it: its
// first segment is not a configured namespace id, and the expression's leading
// wildcard is a rule no namespace-mode id can express. So the settings
// reaching the channel at all means the expression was matched, rather than a
// first-segment lookup having happened to agree.
func TestREST_ChannelTakesAMatcherNamespacesSettings(t *testing.T) {
	assertChannelMayBeEdited(t, "unconfigured:edits")
}

// assertChannelMayBeEdited publishes to the channel and then edits what it
// published. Editing is only allowed where the channel's namespace says so, so
// the edit reaching storage at all is the namespace having been consulted.
func assertChannelMayBeEdited(t *testing.T, channelName string) {
	t.Helper()

	stack := newTestServers(t)
	path := "/channels/" + channelName + "/messages"

	rec := httptest.NewRecorder()
	stack.REST().HandlePublish(rec, restRequest(t, "POST", path,
		`{"name":"greeting","data":"hello"}`))
	if rec.Code != http.StatusCreated && rec.Code != http.StatusOK {
		t.Fatalf("publish returned %d: %s", rec.Code, rec.Body)
	}

	rec = httptest.NewRecorder()
	stack.REST().HandleHistory(rec, restRequest(t, "GET", path, ""))
	var messages []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &messages); err != nil {
		t.Fatalf("decoding history (%s): %s", rec.Body, err)
	}
	if len(messages) != 1 {
		t.Fatalf("history returned %d messages, want 1: %s", len(messages), rec.Body)
	}
	serial, _ := messages[0]["serial"].(string)
	if serial == "" {
		t.Fatalf("published message carried no serial: %v", messages[0])
	}

	update := restRequest(t, "PATCH", path+"/"+serial, `{"data":"edited"}`)
	update.SetPathValue("serial", serial)
	rec = httptest.NewRecorder()
	stack.REST().HandleUpdateMessage(rec, update)
	if rec.Code >= 400 {
		t.Fatalf("updating a message in a mutableMessages namespace returned %d: %s", rec.Code, rec.Body)
	}
}

// TestREST_UpdateCarriesWhoPerformedIt checks that an edited message says who
// edited it, which is not necessarily who published it.
//
// The operation carries its own clientId — a client may edit another client's
// message — and taking the message's instead reports the wrong author, or none
// at all when the edit named nobody at the message level.
func TestREST_UpdateCarriesWhoPerformedIt(t *testing.T) {
	stack := newTestServers(t)

	rec := httptest.NewRecorder()
	stack.REST().HandlePublish(rec, restRequest(t, "POST", "/channels/mutable:authorship/messages",
		`{"name":"greeting","data":"hello","clientId":"author"}`))
	if rec.Code >= 400 {
		t.Fatalf("publish returned %d: %s", rec.Code, rec.Body)
	}

	rec = httptest.NewRecorder()
	stack.REST().HandleHistory(rec, restRequest(t, "GET", "/channels/mutable:authorship/messages?v=6", ""))
	var messages []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &messages); err != nil {
		t.Fatalf("decoding history (%s): %s", rec.Body, err)
	}
	serial, _ := messages[0]["serial"].(string)

	update := restRequest(t, "PATCH", "/channels/mutable:authorship/messages/"+serial+"?v=6",
		`{"data":"edited","version":{"clientId":"editor","description":"why"}}`)
	update.SetPathValue("serial", serial)
	rec = httptest.NewRecorder()
	stack.REST().HandleUpdateMessage(rec, update)
	if rec.Code >= 400 {
		t.Fatalf("update returned %d: %s", rec.Code, rec.Body)
	}

	get := restRequest(t, "GET", "/channels/mutable:authorship/messages/"+serial+"?v=6", "")
	get.SetPathValue("serial", serial)
	rec = httptest.NewRecorder()
	stack.REST().HandleGetMessage(rec, get)
	if rec.Code >= 400 {
		t.Fatalf("reading the message back returned %d: %s", rec.Code, rec.Body)
	}

	var message map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &message); err != nil {
		t.Fatalf("decoding the message (%s): %s", rec.Body, err)
	}
	if got, _ := message["serial"].(string); got != serial {
		t.Errorf("an edited message changed serial: %q, want the original %q", got, serial)
	}
	version, _ := message["version"].(map[string]any)
	if version == nil {
		t.Fatalf("an edited message came back with no version: %s", rec.Body)
	}
	if got, _ := version["clientId"].(string); got != "editor" {
		t.Errorf("version clientId = %q, want the editor's %q", got, "editor")
	}
	if got, _ := version["description"].(string); got != "why" {
		t.Errorf("version description = %q, want %q", got, "why")
	}
}
