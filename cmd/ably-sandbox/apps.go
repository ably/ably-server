package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/ably/ably-server/internal/config"
)

// fullCapability is the x-ably-capability JSON granted to a key that
// carries no capability in the request (an empty `{}` key entry) — the
// full set, matching how the child treats an omitted capability.
const fullCapability = `{"*":["*"]}`

// postAppsRequest is the subset of the Ably test-app-setup `post_apps`
// body the provisioner acts on (DESIGN.md §15). Keys and namespaces are
// decoded as raw maps so unrecognised fields (e.g. revocableTokens)
// round-trip into the response verbatim; channels are typed since their
// shape drives the child's presence fixtures. Other fields (limits, …)
// are ignored.
type postAppsRequest struct {
	Keys       []map[string]json.RawMessage `json:"keys"`
	Namespaces []map[string]json.RawMessage `json:"namespaces"`
	Channels   []channelSpec                `json:"channels"`
	Cipher     json.RawMessage              `json:"cipher,omitempty"`
}

type channelSpec struct {
	Name     string         `json:"name"`
	Presence []presenceSpec `json:"presence"`
}

type presenceSpec struct {
	ClientID string `json:"clientId"`
	Data     string `json:"data"`
	Encoding string `json:"encoding"`
}

// appResponse is the sandbox-shaped app JSON the harness consumes
// (DESIGN.md §15). It carries the standard fields (appId/accountId/keys/
// namespaces/channels, plus a cipher echo when present) extended with
// endpoint/port/tls so a client can route to the child directly.
type appResponse struct {
	AppID      string                       `json:"appId"`
	AccountID  string                       `json:"accountId"`
	Keys       []map[string]any             `json:"keys"`
	Namespaces []map[string]json.RawMessage `json:"namespaces"`
	Channels   []channelSpec                `json:"channels"`
	Cipher     json.RawMessage              `json:"cipher,omitempty"`
	Endpoint   string                       `json:"endpoint"`
	Port       int                          `json:"port"`
	TLS        bool                         `json:"tls"`
}

// translation is the result of turning a post_apps body into a child
// config plus the response's per-key blocks (before the child's port is
// known). Keeping the two together lets provisioning emit both from one
// pass over the request.
type translation struct {
	appID     string
	accountID string
	toml      string
	respKeys  []map[string]any
}

// translate turns a decoded post_apps body into the child's TOML config
// and the response's key blocks. It generates the appId/accountId and,
// per key, an `appId.keyId:secret` triple the SDKs parse, normalises each
// key's capability to a JSON string, and reuses the internal/config types
// (so escaping matches the server's own parser) to emit the [[keys]],
// [[namespaces]] and [[channels]] sections.
func translate(req *postAppsRequest) (translation, error) {
	appID := randToken(9)
	accountID := "a-" + randToken(6)

	cfg := childConfig{}
	respKeys := make([]map[string]any, 0, len(req.Keys))

	for i, k := range req.Keys {
		keyID := randToken(6)
		keyName := appID + "." + keyID
		keySecret := randToken(24)
		keyStr := keyName + ":" + keySecret

		capStr, err := capabilityString(k["capability"])
		if err != nil {
			return translation{}, fmt.Errorf("key #%d capability: %w", i, err)
		}

		// The child reads an empty capability as the full set; only emit a
		// [[keys]] capability when the request narrowed it.
		entry := config.KeyEntry{Key: keyStr}
		if capStr != "" {
			entry.Capability = capStr
		}
		cfg.Keys = append(cfg.Keys, entry)

		// Echo every requested field, adding the generated identifiers. The
		// capability is echoed as a JSON string (the sandbox's shape), full
		// when the request omitted it.
		resp := make(map[string]any, len(k)+4)
		for field, raw := range k {
			var v any
			if err := json.Unmarshal(raw, &v); err != nil {
				return translation{}, fmt.Errorf("key #%d field %q: %w", i, field, err)
			}
			resp[field] = v
		}
		resp["keyName"] = keyName
		resp["keySecret"] = keySecret
		resp["keyStr"] = keyStr
		if capStr != "" {
			resp["capability"] = capStr
		} else {
			resp["capability"] = fullCapability
		}
		respKeys = append(respKeys, resp)
	}

	for i, ns := range req.Namespaces {
		id, _ := jsonString(ns["id"])
		if id == "" {
			return translation{}, fmt.Errorf("namespace #%d has no id", i)
		}
		cfg.Namespaces = append(cfg.Namespaces, config.Namespace{
			ID:              id,
			Persisted:       jsonBool(ns["persisted"]),
			MutableMessages: jsonBool(ns["mutableMessages"]),
			PushEnabled:     jsonBool(ns["pushEnabled"]),
		})
	}

	for i, ch := range req.Channels {
		if ch.Name == "" {
			return translation{}, fmt.Errorf("channel #%d has no name", i)
		}
		members := make([]config.PresenceMember, 0, len(ch.Presence))
		for mi, m := range ch.Presence {
			if m.ClientID == "" {
				return translation{}, fmt.Errorf("channel %q presence member #%d has no clientId", ch.Name, mi)
			}
			members = append(members, config.PresenceMember{
				ClientID: m.ClientID,
				Data:     m.Data,
				Encoding: m.Encoding,
			})
		}
		cfg.Channels = append(cfg.Channels, config.Channel{Name: ch.Name, Presence: members})
	}

	var buf bytes.Buffer
	if err := toml.NewEncoder(&buf).Encode(cfg); err != nil {
		return translation{}, fmt.Errorf("encode config: %w", err)
	}

	return translation{
		appID:     appID,
		accountID: accountID,
		toml:      buf.String(),
		respKeys:  respKeys,
	}, nil
}

// childConfig is the emitted child TOML. It reuses the internal/config
// entry types so the encoding round-trips through the server's own
// decoder, and only carries the fixture sections — mode, listen and the
// addr-file are passed to the child as flags instead.
type childConfig struct {
	Keys       []config.KeyEntry  `toml:"keys"`
	Namespaces []config.Namespace `toml:"namespaces"`
	Channels   []config.Channel   `toml:"channels"`
}

// capabilityString normalises a request key's capability into an
// x-ably-capability JSON string. Ably's post_apps shape has historically
// carried it either as an already-stringified JSON object or as a nested
// object; both are accepted. Absent → "" (full capability).
func capabilityString(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		return asString, nil
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return "", fmt.Errorf("not a string or object: %w", err)
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// jsonString decodes a raw JSON value as a string.
func jsonString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

// jsonBool decodes a raw JSON value as a bool, defaulting to false.
func jsonBool(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var b bool
	_ = json.Unmarshal(raw, &b)
	return b
}

// randToken returns a URL-safe random token of n bytes. The alphabet
// (base64url, no padding) excludes '.' and ':', so tokens compose into an
// `appId.keyId:secret` key the SDKs parse unambiguously.
func randToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("sandbox: crypto/rand failed: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// handleCreateApp implements POST /apps: it translates the body into a
// child config, boots an ably-server child, and returns the sandbox app
// JSON (201) with the child's endpoint/port.
func (p *provisioner) handleCreateApp(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var req postAppsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "parse body: "+err.Error(), http.StatusBadRequest)
		return
	}

	tr, err := translate(&req)
	if err != nil {
		http.Error(w, "invalid app spec: "+err.Error(), http.StatusBadRequest)
		return
	}

	c, err := p.startChild(tr.appID, tr.toml)
	if err != nil {
		p.logger.Error("start child", "appId", tr.appID, "err", err)
		http.Error(w, "provision child: "+err.Error(), http.StatusInternalServerError)
		return
	}

	p.mu.Lock()
	p.children[tr.appID] = c
	p.lastTouched[tr.appID] = time.Now()
	p.mu.Unlock()

	p.logger.Info("provisioned app", "appId", tr.appID, "port", c.port, "keys", len(tr.respKeys))

	resp := appResponse{
		AppID:      tr.appID,
		AccountID:  tr.accountID,
		Keys:       tr.respKeys,
		Namespaces: req.Namespaces,
		Channels:   req.Channels,
		Cipher:     req.Cipher,
		Endpoint:   "127.0.0.1",
		Port:       c.port,
		TLS:        false,
	}
	if resp.Namespaces == nil {
		resp.Namespaces = []map[string]json.RawMessage{}
	}
	if resp.Channels == nil {
		resp.Channels = []channelSpec{}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

// handleDeleteApp implements DELETE /apps/{appId}: it terminates the
// child and forgets it. Idempotent — deleting an unknown app is a no-op
// that still returns 204.
func (p *provisioner) handleDeleteApp(w http.ResponseWriter, r *http.Request) {
	appID := r.PathValue("appId")
	p.mu.Lock()
	c := p.children[appID]
	delete(p.children, appID)
	delete(p.lastTouched, appID)
	p.mu.Unlock()

	if c != nil {
		p.logger.Info("deleting app", "appId", appID, "port", c.port)
		p.terminate(c)
	}
	w.WriteHeader(http.StatusNoContent)
}

// handlePostStats implements POST /stats: the harness posts stats
// fixtures to the provisioning host, which the provisioner accepts and
// discards (mirroring the server's stub, DESIGN.md §1, §15).
func (p *provisioner) handlePostStats(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 1<<20))
	w.WriteHeader(http.StatusCreated)
}
