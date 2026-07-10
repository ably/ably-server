package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/protocol"
)

// annotationsURL builds the annotations path for a channel + target serial.
func annotationsURL(channel, serial string) string {
	return "/channels/" + channel + "/messages/" + serial + "/annotations"
}

// annotationsGet issues an authenticated GET on the annotations path.
func annotationsGet(t *testing.T, srv *httptest.Server, channel, serial, rawQuery string) *http.Response {
	t.Helper()
	u := srv.URL + annotationsURL(channel, serial)
	if rawQuery != "" {
		u += "?" + rawQuery
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.SetBasicAuth("app.key", "secret")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// TestPublishAndListAnnotationsJSON: publish an annotation on a message and
// read it back via GET, in JSON (TASK-65 AC#1, AC#2).
func TestPublishAndListAnnotationsJSON(t *testing.T) {
	srv, _ := newTestServer(t)
	target := publishOne(t, srv, "foo", &protocol.Message{Name: "post", Data: "hello"})

	body, _ := json.Marshal(&protocol.Annotation{
		Action: protocol.AnnotationCreate, Type: "reaction:multiple.v1", Name: "👍",
	})
	resp := request(t, srv, http.MethodPost, annotationsURL("foo", target), "application/json", body, true)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("publish annotation status = %d, want 201", resp.StatusCode)
	}
	var pubResp annotationResponse
	decodeJSON(t, resp, &pubResp)
	if pubResp.Serial == "" {
		t.Fatalf("publish response missing assigned serial: %+v", pubResp)
	}
	if pubResp.Channel != "foo" {
		t.Errorf("response channel = %q, want foo", pubResp.Channel)
	}

	got := annotationsGet(t, srv, "foo", target, "")
	if got.StatusCode != http.StatusOK {
		t.Fatalf("list annotations status = %d, want 200", got.StatusCode)
	}
	var anns []*protocol.Annotation
	decodeJSON(t, got, &anns)
	if len(anns) != 1 {
		t.Fatalf("listed %d annotations, want 1", len(anns))
	}
	if anns[0].MessageSerial != target {
		t.Errorf("annotation messageSerial = %q, want target %q", anns[0].MessageSerial, target)
	}
	if anns[0].Serial != pubResp.Serial {
		t.Errorf("listed serial = %q, want published %q", anns[0].Serial, pubResp.Serial)
	}
	if anns[0].Name != "👍" {
		t.Errorf("annotation name = %q, want 👍", anns[0].Name)
	}
}

// TestPublishAndListAnnotationsMsgpack round-trips via msgpack (AC#3).
func TestPublishAndListAnnotationsMsgpack(t *testing.T) {
	srv, _ := newTestServer(t)
	target := publishOne(t, srv, "foo", &protocol.Message{Name: "post", Data: "hi"})

	body, _ := msgpack.Marshal(&protocol.Annotation{
		Action: protocol.AnnotationCreate, Type: "reaction:multiple.v1", Name: "❤️",
	})
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+annotationsURL("foo", target), bytesReader(body))
	req.Header.Set("Content-Type", "application/x-msgpack")
	req.Header.Set("Accept", "application/x-msgpack")
	req.SetBasicAuth("app.key", "secret")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("publish status = %d, want 201", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-msgpack" {
		t.Errorf("response Content-Type = %q, want application/x-msgpack", ct)
	}

	// GET with msgpack Accept.
	greq, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+annotationsURL("foo", target), nil)
	greq.Header.Set("Accept", "application/x-msgpack")
	greq.SetBasicAuth("app.key", "secret")
	gresp, err := srv.Client().Do(greq)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	t.Cleanup(func() { gresp.Body.Close() })
	var anns []*protocol.Annotation
	dec := msgpack.NewDecoder(gresp.Body)
	if err := dec.Decode(&anns); err != nil {
		t.Fatalf("decode msgpack annotations: %v", err)
	}
	if len(anns) != 1 || anns[0].Name != "❤️" {
		t.Fatalf("msgpack annotations = %+v, want one named ❤️", anns)
	}
}

// TestListAnnotationsPagination: the Link rel=next cursor walks the
// annotations one page at a time (AC#2).
func TestListAnnotationsPagination(t *testing.T) {
	srv, _ := newTestServer(t)
	target := publishOne(t, srv, "foo", &protocol.Message{Name: "post"})

	for _, n := range []string{"a", "b", "c"} {
		body, _ := json.Marshal(&protocol.Annotation{Action: protocol.AnnotationCreate, Type: "reaction:multiple.v1", Name: n})
		resp := request(t, srv, http.MethodPost, annotationsURL("foo", target), "application/json", body, true)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("publish %s status = %d", n, resp.StatusCode)
		}
	}

	first := annotationsGet(t, srv, "foo", target, "limit=2")
	var page1 []*protocol.Annotation
	decodeJSON(t, first, &page1)
	if len(page1) != 2 {
		t.Fatalf("page1 = %d annotations, want 2", len(page1))
	}
	next := nextLink(t, first)
	if next == "" {
		t.Fatal("no rel=next link on a limited page with more results")
	}

	nreq, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+next, nil)
	nreq.SetBasicAuth("app.key", "secret")
	nresp, err := srv.Client().Do(nreq)
	if err != nil {
		t.Fatalf("next page: %v", err)
	}
	t.Cleanup(func() { nresp.Body.Close() })
	var page2 []*protocol.Annotation
	decodeJSON(t, nresp, &page2)
	if len(page2) != 1 {
		t.Fatalf("page2 = %d annotations, want 1 (the remaining one)", len(page2))
	}
	if page2[0].Name != "c" {
		t.Errorf("page2 name = %q, want c", page2[0].Name)
	}
}

// TestPublishAnnotationUnknownTarget: annotating a non-existent message is
// a 404 with the Ably error code.
func TestPublishAnnotationUnknownTarget(t *testing.T) {
	srv, _ := newTestServer(t)
	body, _ := json.Marshal(&protocol.Annotation{Action: protocol.AnnotationCreate, Type: "reaction:multiple.v1", Name: "👍"})
	resp := request(t, srv, http.MethodPost, annotationsURL("foo", "00000000000001-000@nope:000"), "application/json", body, true)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// TestAnnotationRESTCapability: POST requires annotation-publish, GET
// requires history (DESIGN.md §14.5).
func TestAnnotationRESTCapability(t *testing.T) {
	srv, _ := newTestServer(t)
	target := publishOne(t, srv, "foo", &protocol.Message{Name: "post"})
	body, _ := json.Marshal(&protocol.Annotation{Action: protocol.AnnotationCreate, Type: "reaction:distinct.v1", Name: "👍"})

	// A token with only `publish` cannot publish annotations (needs
	// annotation-publish) — 401.
	pubOnly := bearerTokenWithClientID(t, `{"foo":["publish"]}`, "alice")
	if resp := tokenRequest(t, srv, http.MethodPost, annotationsURL("foo", target), pubOnly, body); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("publish annotation with publish-only cap = %d, want 401", resp.StatusCode)
	}

	// annotation-publish grants it (concrete clientId via the claim so a
	// distinct.v1 publish is permitted). 201.
	annPub := bearerTokenWithClientID(t, `{"foo":["annotation-publish"]}`, "alice")
	if resp := tokenRequest(t, srv, http.MethodPost, annotationsURL("foo", target), annPub, body); resp.StatusCode != http.StatusCreated {
		t.Errorf("publish annotation with annotation-publish cap = %d, want 201", resp.StatusCode)
	}

	// GET requires history: an annotation-publish-only token is rejected.
	if resp := tokenRequest(t, srv, http.MethodGet, annotationsURL("foo", target), annPub, nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("list annotations with no history cap = %d, want 401", resp.StatusCode)
	}
	hist := bearerToken(t, `{"foo":["history"]}`)
	if resp := tokenRequest(t, srv, http.MethodGet, annotationsURL("foo", target), hist, nil); resp.StatusCode != http.StatusOK {
		t.Errorf("list annotations with history cap = %d, want 200", resp.StatusCode)
	}
}

// TestPublishAnnotationAnonymousMethodRejected: a wildcard/basic credential
// with no clientId is anonymous, so a distinct.v1 publish (identified-only)
// is rejected 400 while multiple.v1 succeeds (§14.1).
func TestPublishAnnotationAnonymousMethodRejected(t *testing.T) {
	srv, _ := newTestServer(t)
	target := publishOne(t, srv, "foo", &protocol.Message{Name: "post"})

	distinct, _ := json.Marshal(&protocol.Annotation{Action: protocol.AnnotationCreate, Type: "reaction:distinct.v1", Name: "👍"})
	if resp := request(t, srv, http.MethodPost, annotationsURL("foo", target), "application/json", distinct, true); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("anonymous distinct.v1 status = %d, want 400", resp.StatusCode)
	}
	multiple, _ := json.Marshal(&protocol.Annotation{Action: protocol.AnnotationCreate, Type: "reaction:multiple.v1", Name: "👍"})
	if resp := request(t, srv, http.MethodPost, annotationsURL("foo", target), "application/json", multiple, true); resp.StatusCode != http.StatusCreated {
		t.Errorf("anonymous multiple.v1 status = %d, want 201", resp.StatusCode)
	}
}
