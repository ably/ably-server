package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ably/ably-server/internal/logging"
)

// childShutdownGrace is how long a child ably-server is given to exit on
// SIGTERM before it is escalated to SIGKILL. A memory-mode child exits
// promptly, so this is only a safety net.
const childShutdownGrace = 5 * time.Second

// childReadyTimeout bounds how long provisioning waits for a freshly
// booted child to write its --addr-file (i.e. finish binding).
const childReadyTimeout = 20 * time.Second

// serverCommand describes how to invoke the ably-server child: a program
// name plus any leading args (e.g. {"go", ["run", "./cmd/ably-server"]}).
// The per-app flags are appended to prefixArgs at spawn time.
type serverCommand struct {
	name       string
	prefixArgs []string
}

// child is one provisioned app's running ably-server process.
type child struct {
	appID   string
	port    int
	cmd     *exec.Cmd
	logFile *os.File
	tmpDir  string
	// done is closed once cmd.Wait returns; waitErr holds its result.
	done    chan struct{}
	waitErr error
}

// provisioner owns the set of running child servers and their lifecycle
// (DESIGN.md §17).
type provisioner struct {
	serverCmd serverCommand
	logDir    string
	idleTTL   time.Duration
	logger    *logging.Logger

	mu          sync.Mutex
	children    map[string]*child
	lastTouched map[string]time.Time
}

func newProvisioner(cmd serverCommand, logDir string, idleTTL time.Duration, logger *logging.Logger) *provisioner {
	return &provisioner{
		serverCmd:   cmd,
		logDir:      logDir,
		idleTTL:     idleTTL,
		logger:      logger,
		children:    make(map[string]*child),
		lastTouched: make(map[string]time.Time),
	}
}

// resolveServerCommand decides how to run the ably-server child
// (DESIGN.md §17): an explicit --server-bin wins; otherwise a sibling
// named "ably-server" next to this executable is used; otherwise it falls
// back to `go run ./cmd/ably-server`, which relies on the working
// directory being the module root (the case when the provisioner itself
// is run via `go run ./cmd/ably-local-sandbox`).
func resolveServerCommand(serverBin string) (serverCommand, error) {
	if serverBin != "" {
		return serverCommand{name: serverBin}, nil
	}
	if exe, err := os.Executable(); err == nil {
		sibling := filepath.Join(filepath.Dir(exe), "ably-server")
		if info, err := os.Stat(sibling); err == nil && !info.IsDir() {
			return serverCommand{name: sibling}, nil
		}
	}
	return serverCommand{name: "go", prefixArgs: []string{"run", "./cmd/ably-server"}}, nil
}

// startChild writes the child's config, boots the ably-server process,
// and waits for it to bind. On success the returned child is registered
// with the caller (handleCreateApp); on failure any partial state is
// cleaned up.
func (p *provisioner) startChild(appID, tomlConfig string) (*child, error) {
	tmpDir, err := os.MkdirTemp(p.logDir, "app-"+appID+"-*")
	if err != nil {
		return nil, fmt.Errorf("create app dir: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(tmpDir) }

	cfgPath := filepath.Join(tmpDir, "config.toml")
	if err := os.WriteFile(cfgPath, []byte(tomlConfig), 0o600); err != nil {
		cleanup()
		return nil, fmt.Errorf("write config: %w", err)
	}
	addrPath := filepath.Join(tmpDir, "addr")

	logPath := filepath.Join(tmpDir, "server.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("create log file: %w", err)
	}

	args := append([]string{}, p.serverCmd.prefixArgs...)
	args = append(args,
		"--config", cfgPath,
		"--mode", "memory",
		"--listen", "127.0.0.1:0",
		"--addr-file", addrPath,
		// SDK compat suites POST stats before reading them back;
		// the core server has the stub off by default (DESIGN.md §1), so
		// every provisioned child needs it explicitly enabled.
		"--enable-stats-stub",
	)
	cmd := exec.Command(p.serverCmd.name, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	// Strip ABLY_SERVER_* from the child's environment so a variable set
	// for the provisioner (e.g. ABLY_SERVER_KEYS, which resolves ahead
	// of the config file) can't override the per-app config we just wrote.
	cmd.Env = filterEnv(os.Environ(), "ABLY_SERVER_")
	// Run the child in its own process group so termination can signal the
	// whole group — which reaches the real server even when the go-run
	// fallback puts a `go` parent in front of it.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		cleanup()
		return nil, fmt.Errorf("start ably-server: %w", err)
	}

	c := &child{
		appID:   appID,
		cmd:     cmd,
		logFile: logFile,
		tmpDir:  tmpDir,
		done:    make(chan struct{}),
	}
	go func() {
		c.waitErr = cmd.Wait()
		close(c.done)
	}()

	port, err := waitForAddr(addrPath, c.done, childReadyTimeout)
	if err != nil {
		tail := scanLog(logPath, 10)
		p.terminate(c)
		if tail != "" {
			return nil, fmt.Errorf("child did not become ready: %w; server log:\n%s", err, tail)
		}
		return nil, fmt.Errorf("child did not become ready: %w", err)
	}
	c.port = port
	return c, nil
}

// waitForAddr polls addrPath until the child writes its bound address,
// returning the port. It fails fast if the child exits first.
func waitForAddr(addrPath string, done <-chan struct{}, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if b, err := os.ReadFile(addrPath); err == nil && len(b) > 0 {
			_, portStr, err := net.SplitHostPort(strings.TrimSpace(string(b)))
			if err != nil {
				return 0, fmt.Errorf("parse addr %q: %w", string(b), err)
			}
			port, err := strconv.Atoi(portStr)
			if err != nil {
				return 0, fmt.Errorf("parse port %q: %w", portStr, err)
			}
			return port, nil
		}
		select {
		case <-done:
			return 0, fmt.Errorf("process exited before binding")
		case <-ticker.C:
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("timed out after %s", timeout)
		}
	}
}

// terminate stops a child: SIGTERM to its process group, escalating to
// SIGKILL after the grace window, then closes its log file and removes
// its temp dir. Safe to call once per child.
func (p *provisioner) terminate(c *child) {
	if c.cmd.Process != nil {
		_ = syscall.Kill(-c.cmd.Process.Pid, syscall.SIGTERM)
		select {
		case <-c.done:
		case <-time.After(childShutdownGrace):
			_ = syscall.Kill(-c.cmd.Process.Pid, syscall.SIGKILL)
			<-c.done
		}
	}
	if c.logFile != nil {
		_ = c.logFile.Close()
	}
	_ = os.RemoveAll(c.tmpDir)
}

// reapLoop periodically kills children not touched within idleTTL,
// returning when ctx is cancelled.
func (p *provisioner) reapLoop(ctx context.Context) {
	interval := p.idleTTL
	if interval > time.Minute {
		interval = time.Minute
	}
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.reap()
		}
	}
}

// reap kills and forgets every child idle longer than idleTTL.
func (p *provisioner) reap() {
	now := time.Now()
	p.mu.Lock()
	var stale []*child
	for id, c := range p.children {
		if now.Sub(p.lastTouched[id]) > p.idleTTL {
			stale = append(stale, c)
			delete(p.children, id)
			delete(p.lastTouched, id)
		}
	}
	p.mu.Unlock()
	for _, c := range stale {
		p.logger.Info("reaping idle app", "appId", c.appID, "port", c.port)
		p.terminate(c)
	}
}

// shutdown terminates every child and clears the registry. Called when
// the provisioner receives SIGTERM.
func (p *provisioner) shutdown() {
	p.mu.Lock()
	all := make([]*child, 0, len(p.children))
	for _, c := range p.children {
		all = append(all, c)
	}
	p.children = make(map[string]*child)
	p.lastTouched = make(map[string]time.Time)
	p.mu.Unlock()

	var wg sync.WaitGroup
	for _, c := range all {
		wg.Add(1)
		go func(c *child) {
			defer wg.Done()
			p.terminate(c)
		}(c)
	}
	wg.Wait()
}

// filterEnv returns environ with every entry whose name has the given
// prefix removed.
func filterEnv(environ []string, prefix string) []string {
	out := make([]string, 0, len(environ))
	for _, e := range environ {
		if strings.HasPrefix(e, prefix) {
			continue
		}
		out = append(out, e)
	}
	return out
}

// scanLog returns up to the last n lines of a child's log file, used to
// enrich a boot-failure error. Best-effort: returns "" on any read error.
func scanLog(path string, n int) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines = append(lines, sc.Text())
		if len(lines) > n {
			lines = lines[1:]
		}
	}
	return strings.Join(lines, "\n")
}
