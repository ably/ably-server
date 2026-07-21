package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ably/ably-server/internal/config"
)

// loadPostApps reads the vendored test-app-setup.json and returns its
// decoded post_apps body.
func loadPostApps(t *testing.T) *postAppsRequest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "test-app-setup.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var file struct {
		PostApps json.RawMessage `json:"post_apps"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	var req postAppsRequest
	if err := json.Unmarshal(file.PostApps, &req); err != nil {
		t.Fatalf("parse post_apps: %v", err)
	}
	return &req
}

func TestTranslateResponseKeys(t *testing.T) {
	req := loadPostApps(t)
	tr, err := translate(req)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}

	if len(tr.respKeys) != len(req.Keys) {
		t.Fatalf("respKeys = %d, want %d (harness asserts equality)", len(tr.respKeys), len(req.Keys))
	}

	for i, k := range tr.respKeys {
		keyName, _ := k["keyName"].(string)
		keySecret, _ := k["keySecret"].(string)
		keyStr, _ := k["keyStr"].(string)
		capStr, _ := k["capability"].(string)
		if keyName == "" || keySecret == "" {
			t.Fatalf("key #%d missing keyName/keySecret: %#v", i, k)
		}
		// keyName is "<appId>.<keyId>" and keyStr is "<keyName>:<keySecret>",
		// the format the SDKs parse.
		if !strings.HasPrefix(keyName, tr.appID+".") {
			t.Errorf("key #%d keyName %q not prefixed by appId %q", i, keyName, tr.appID)
		}
		if want := keyName + ":" + keySecret; keyStr != want {
			t.Errorf("key #%d keyStr = %q, want %q", i, keyStr, want)
		}
		if capStr == "" {
			t.Errorf("key #%d capability echoed empty", i)
		}
		// The capability echo is always a JSON string, never a nested object.
		if _, isObj := k["capability"].(map[string]any); isObj {
			t.Errorf("key #%d capability echoed as object, want stringified", i)
		}
	}

	// Key 0 is the bare {} entry: full capability.
	if got := tr.respKeys[0]["capability"]; got != fullCapability {
		t.Errorf("key 0 capability = %q, want full %q", got, fullCapability)
	}
	// Key 3 is subscribe-only; verify the requested capability is echoed.
	if got, _ := tr.respKeys[3]["capability"].(string); !strings.Contains(got, "subscribe") {
		t.Errorf("key 3 capability = %q, want a subscribe grant", got)
	}
	// Key 4 carries revocableTokens; unrecognised fields must round-trip.
	if got := tr.respKeys[4]["revocableTokens"]; got != true {
		t.Errorf("key 4 revocableTokens = %v, want true (unknown fields must echo)", got)
	}
}

func TestTranslateConfigRoundTrips(t *testing.T) {
	req := loadPostApps(t)
	tr, err := translate(req)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}

	// The emitted TOML must parse cleanly with the server's own loader.
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(tr.toml), 0o600); err != nil {
		t.Fatalf("write toml: %v", err)
	}
	f, err := config.Load(path)
	if err != nil {
		t.Fatalf("config.Load of emitted TOML failed: %v\n---\n%s", err, tr.toml)
	}

	if len(f.Keys) != len(req.Keys) {
		t.Errorf("config keys = %d, want %d", len(f.Keys), len(req.Keys))
	}
	if len(f.Namespaces) != len(req.Namespaces) {
		t.Errorf("config namespaces = %d, want %d", len(f.Namespaces), len(req.Namespaces))
	}
	if len(f.Channels) != 1 {
		t.Fatalf("config channels = %d, want 1", len(f.Channels))
	}

	// Namespace flags survive translation.
	var persistedSeen bool
	for _, ns := range f.Namespaces {
		if ns.ID == "persisted" {
			persistedSeen = true
			if !ns.Persisted {
				t.Errorf("namespace persisted flag lost")
			}
		}
	}
	if !persistedSeen {
		t.Errorf("persisted namespace missing from config")
	}

	ch := f.Channels[0]
	if ch.Name != "persisted:presence_fixtures" {
		t.Errorf("channel name = %q", ch.Name)
	}
	if len(ch.Presence) != 6 {
		t.Fatalf("presence members = %d, want 6", len(ch.Presence))
	}

	// Presence data with embedded quotes/braces must survive TOML escaping.
	byClient := map[string]config.PresenceMember{}
	for _, m := range ch.Presence {
		byClient[m.ClientID] = m
	}
	want := map[string]string{
		"client_json":    `{ "test": "This is a JSONObject clientData payload"}`,
		"client_decoded": `{"example":{"json":"Object"}}`,
		"client_string":  "This is a string clientData payload",
	}
	for cid, data := range want {
		m, ok := byClient[cid]
		if !ok {
			t.Errorf("presence member %q missing", cid)
			continue
		}
		if m.Data != data {
			t.Errorf("member %q data = %q, want %q", cid, m.Data, data)
		}
	}
	if byClient["client_encoded"].Encoding != "json/utf-8/cipher+aes-128-cbc/base64" {
		t.Errorf("client_encoded encoding lost: %q", byClient["client_encoded"].Encoding)
	}
}

func TestCapabilityString(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"absent", ``, ""},
		{"stringified", `"{\"a\":[\"publish\"]}"`, `{"a":["publish"]}`},
		{"object", `{"a":["publish"]}`, `{"a":["publish"]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var raw json.RawMessage
			if tc.in != "" {
				raw = json.RawMessage(tc.in)
			}
			got, err := capabilityString(raw)
			if err != nil {
				t.Fatalf("capabilityString: %v", err)
			}
			if got != tc.want {
				t.Errorf("capabilityString(%s) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestTranslateValidationErrors(t *testing.T) {
	t.Run("namespace without id", func(t *testing.T) {
		req := &postAppsRequest{Namespaces: []map[string]json.RawMessage{{"persisted": json.RawMessage(`true`)}}}
		if _, err := translate(req); err == nil {
			t.Fatal("expected error for namespace with no id")
		}
	})
	t.Run("presence member without clientId", func(t *testing.T) {
		req := &postAppsRequest{Channels: []channelSpec{{Name: "c", Presence: []presenceSpec{{Data: "x"}}}}}
		if _, err := translate(req); err == nil {
			t.Fatal("expected error for presence member with no clientId")
		}
	})
	t.Run("channel without name", func(t *testing.T) {
		req := &postAppsRequest{Channels: []channelSpec{{Presence: []presenceSpec{{ClientID: "c"}}}}}
		if _, err := translate(req); err == nil {
			t.Fatal("expected error for channel with no name")
		}
	})
}
