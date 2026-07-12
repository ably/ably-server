package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// serverBin is the ably-server binary built once for the whole package by
// TestMain; the provisioner boots it as the child under test.
var serverBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "ably-server-bin-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, "mktemp:", err)
		os.Exit(1)
	}
	serverBin = filepath.Join(dir, "ably-server")
	build := exec.Command("go", "build", "-o", serverBin, "github.com/ably/ably-server/cmd/ably-server")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build ably-server:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// startProvisioner boots the provisioner in-process on an ephemeral port
// and returns its base URL. It is torn down (killing all children) on
// t.Cleanup.
func startProvisioner(t *testing.T, extraArgs ...string) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan net.Addr, 1)
	done := make(chan int, 1)
	args := append([]string{
		"--listen=127.0.0.1:0",
		"--server-bin=" + serverBin,
		"--log-dir=" + t.TempDir(),
		"--log-level=error",
	}, extraArgs...)
	go func() {
		done <- run(ctx, runOpts{
			Args:   args,
			Getenv: func(string) string { return "" },
			Out:    io.Discard,
			Ready:  ready,
		})
	}()
	var addr net.Addr
	select {
	case addr = <-ready:
	case code := <-done:
		t.Fatalf("provisioner exited before ready (code=%d)", code)
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("provisioner not ready within 10s")
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("provisioner did not shut down within 15s")
		}
	})
	return "http://" + addr.String()
}

// provisionApp POSTs the vendored test-app-setup post_apps body and
// returns the decoded response.
func provisionApp(t *testing.T, base string) appResult {
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
	resp, err := http.Post(base+"/apps", "application/json", bytes.NewReader(file.PostApps))
	if err != nil {
		t.Fatalf("POST /apps: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /apps status = %d, body: %s", resp.StatusCode, body)
	}
	var app appResult
	if err := json.Unmarshal(body, &app); err != nil {
		t.Fatalf("decode app: %v; body: %s", err, body)
	}
	return app
}

type appResult struct {
	AppID     string           `json:"appId"`
	AccountID string           `json:"accountId"`
	Keys      []map[string]any `json:"keys"`
	Port      int              `json:"port"`
	Endpoint  string           `json:"endpoint"`
	TLS       bool             `json:"tls"`
}

func (a appResult) keyStr(i int) string {
	s, _ := a.Keys[i]["keyStr"].(string)
	return s
}

func (a appResult) childURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", a.Port)
}

func TestProvisionAndSmoke(t *testing.T) {
	base := startProvisioner(t)
	app := provisionApp(t, base)

	if app.AppID == "" || app.AccountID == "" {
		t.Fatalf("missing appId/accountId: %#v", app)
	}
	if len(app.Keys) != 6 {
		t.Fatalf("keys = %d, want 6", len(app.Keys))
	}
	if app.Port == 0 || app.Endpoint != "127.0.0.1" || app.TLS {
		t.Fatalf("bad endpoint fields: endpoint=%q port=%d tls=%v", app.Endpoint, app.Port, app.TLS)
	}

	child := app.childURL()
	const channel = "persisted:presence_fixtures"

	// AC#2 (publish): the full-capability key (keys[0]) may publish.
	if code, body := publish(t, child, app.keyStr(0), channel, `{"data":"hello"}`); code != http.StatusCreated {
		t.Fatalf("publish with full key: status = %d, body: %s", code, body)
	}

	// AC#1 (per-key caps): keys[3] is {"*":["subscribe"]}; publishing is
	// out of capability and must be denied.
	if code, _ := publish(t, child, app.keyStr(3), channel, `{"data":"denied"}`); code != http.StatusUnauthorized {
		t.Errorf("publish with subscribe-only key: status = %d, want 401", code)
	}

	// AC#1 (qualifier wildcard, TASK-114): keys[5] is the sandbox
	// all-access key {"[*]*":["*"]} — the standard SDK / AIT-suite key.
	// Its "[*]" qualifier must match a plain channel, so publishing on an
	// arbitrary channel succeeds (previously denied with 40160).
	if code, body := publish(t, child, app.keyStr(5), "arbitrary:channel", `{"data":"hello"}`); code != http.StatusCreated {
		t.Fatalf("publish with all-access key keys[5] [*]*: status = %d, body: %s", code, body)
	}

	// AC#1/#2 (presence fixtures): the seeded members are readable.
	members := readPresence(t, child, app.keyStr(0), channel)
	if len(members) != 6 {
		t.Fatalf("presence members = %d, want 6", len(members))
	}
	ids := map[string]bool{}
	for _, m := range members {
		if cid, ok := m["clientId"].(string); ok {
			ids[cid] = true
		}
	}
	for _, want := range []string{"client_bool", "client_string", "client_json", "client_encoded"} {
		if !ids[want] {
			t.Errorf("seeded presence member %q missing; got %v", want, ids)
		}
	}

	// AC#3 (delete): DELETE kills the child.
	deleteApp(t, base, app.AppID, http.StatusNoContent)
	waitChildDead(t, child)

	// DELETE is idempotent.
	deleteApp(t, base, app.AppID, http.StatusNoContent)
}

func TestPostStatsAcceptAndDiscard(t *testing.T) {
	base := startProvisioner(t)
	resp, err := http.Post(base+"/stats", "application/json", strings.NewReader(`[{"stats":"fixture"}]`))
	if err != nil {
		t.Fatalf("POST /stats: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /stats status = %d, want 201", resp.StatusCode)
	}
}

func TestConcurrentProvisionIsolatedChildren(t *testing.T) {
	base := startProvisioner(t)
	const n = 4
	var wg sync.WaitGroup
	apps := make([]appResult, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			apps[i] = provisionApp(t, base)
		}(i)
	}
	wg.Wait()

	ports := map[int]bool{}
	appIDs := map[string]bool{}
	for i, a := range apps {
		if a.Port == 0 {
			t.Fatalf("app %d has no port", i)
		}
		if ports[a.Port] {
			t.Errorf("duplicate child port %d — children not isolated", a.Port)
		}
		ports[a.Port] = true
		if appIDs[a.AppID] {
			t.Errorf("duplicate appId %q", a.AppID)
		}
		appIDs[a.AppID] = true
		// Each child is independently reachable.
		if code, body := publish(t, a.childURL(), a.keyStr(0), "c", `{"data":"x"}`); code != http.StatusCreated {
			t.Errorf("app %d publish status = %d, body: %s", i, code, body)
		}
	}
}

func TestIdleReaperKillsChild(t *testing.T) {
	base := startProvisioner(t, "--idle-ttl=800ms")
	app := provisionApp(t, base)
	// The child is alive right after provisioning.
	if code, _ := publish(t, app.childURL(), app.keyStr(0), "c", `{"data":"x"}`); code != http.StatusCreated {
		t.Fatalf("child not alive after provision: status %d", code)
	}
	// The reaper kills it once idle past the TTL.
	waitChildDead(t, app.childURL())
}

// publish POSTs a single message to the child's REST publish endpoint
// using Basic auth, returning the status code and body.
func publish(t *testing.T, childURL, key, channel, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, childURL+"/channels/"+channel+"/messages", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	setBasicAuth(req, key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("publish request: %v", err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(rb)
}

// readPresence GETs the current presence set from the child.
func readPresence(t *testing.T, childURL, key, channel string) []map[string]any {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, childURL+"/channels/"+channel+"/presence", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	setBasicAuth(req, key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("presence request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET presence status = %d, body: %s", resp.StatusCode, body)
	}
	var members []map[string]any
	if err := json.Unmarshal(body, &members); err != nil {
		t.Fatalf("decode presence: %v; body: %s", err, body)
	}
	return members
}

func deleteApp(t *testing.T, base, appID string, wantStatus int) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, base+"/apps/"+appID, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Fatalf("DELETE status = %d, want %d", resp.StatusCode, wantStatus)
	}
}

// waitChildDead polls the child until it stops answering, failing if it
// is still alive after the timeout.
func waitChildDead(t *testing.T, childURL string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	client := &http.Client{Timeout: 500 * time.Millisecond}
	for time.Now().Before(deadline) {
		resp, err := client.Get(childURL + "/time")
		if err != nil {
			return // connection refused: the child is gone
		}
		_ = resp.Body.Close()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("child at %s still alive after timeout", childURL)
}

func setBasicAuth(req *http.Request, key string) {
	name, secret, _ := strings.Cut(key, ":")
	req.SetBasicAuth(name, secret)
}
