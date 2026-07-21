package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// startMemoryServer boots Run() in memory mode on ephemeral ports,
// wiring Ready/DebugReady so the caller can discover the bound
// addresses. It returns the two channels; the server is torn down via
// t.Cleanup.
func startMemoryServer(t *testing.T, extraArgs ...string) (ready, debugReady chan net.Addr) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ready = make(chan net.Addr, 1)
	debugReady = make(chan net.Addr, 1)
	done := make(chan int, 1)

	args := append([]string{
		"--keys=app.key:secret",
		"--listen=127.0.0.1:0",
	}, extraArgs...)

	go func() {
		done <- Run(ctx, Opts{
			Args:       args,
			Getenv:     emptyEnv,
			Out:        io.Discard,
			Ready:      ready,
			DebugReady: debugReady,
		})
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server did not shut down within 5s")
		}
	})

	select {
	case <-ready:
	case code := <-done:
		t.Fatalf("server exited before ready (code=%d)", code)
	case <-time.After(5 * time.Second):
		t.Fatal("server did not become ready within 5s")
	}
	return ready, debugReady
}

func TestDebugListenServesPprof(t *testing.T) {
	_, debugReady := startMemoryServer(t, "--debug-listen=127.0.0.1:0")

	var debugAddr net.Addr
	select {
	case debugAddr = <-debugReady:
	case <-time.After(5 * time.Second):
		t.Fatal("debug listener did not become ready within 5s")
	}

	resp, err := http.Get("http://" + debugAddr.String() + "/debug/pprof/")
	if err != nil {
		t.Fatalf("GET /debug/pprof/: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /debug/pprof/ on debug listener = %d, want 200", resp.StatusCode)
	}
}

func TestDebugListenUnsetStartsNoListener(t *testing.T) {
	_, debugReady := startMemoryServer(t)

	select {
	case addr := <-debugReady:
		t.Errorf("debug listener started at %s despite --debug-listen being unset", addr)
	case <-time.After(200 * time.Millisecond):
		// Expected: no debug listener starts.
	}
}
