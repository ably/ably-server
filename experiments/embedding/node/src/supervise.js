// Supervisor for an embedded ably-server child process.
//
// Responsibilities (EMBEDDING-POC.md §5):
//   1. Resolve the prebuilt per-platform binary path.
//   2. Pick a FREE TCP port and pass --listen 127.0.0.1:<port>.
//   3. Spawn the child with the API key in the environment.
//   4. Poll /readyz until green (with a timeout).
//   5. Restart the child on UNEXPECTED crash (with backoff).
//   6. stop(): SIGTERM the child and wait for it to exit.
//
// No dependencies beyond Node core.

import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { EventEmitter } from 'node:events';

const __dirname = dirname(fileURLToPath(import.meta.url));

/**
 * Resolve the path to the prebuilt ably-server binary.
 *
 * Real-package behaviour (not implemented in the PoC): resolve the
 * platform-specific optionalDependency, e.g.
 *   require.resolve(`@ably/embedded-server-${process.platform}-${process.arch}/bin/ably-server`)
 * mirroring esbuild's os/cpu keyed optionalDependencies.
 *
 * PoC behaviour: honour ABLY_SERVER_BINARY, else fall back to ../bin/ably-server
 * (with a .exe suffix on Windows).
 *
 * @returns {string} absolute path to the binary
 */
export function resolveBinaryPath() {
  if (process.env.ABLY_SERVER_BINARY) {
    return process.env.ABLY_SERVER_BINARY;
  }
  const exe = process.platform === 'win32' ? 'ably-server.exe' : 'ably-server';
  return join(__dirname, '..', 'bin', exe);
}

/**
 * Ask the OS for a free TCP port on 127.0.0.1 by binding to :0 and reading
 * back the assigned port, then releasing it. There is an inherent (tiny)
 * race between release and the child re-binding; in practice the child grabs
 * it immediately. This is the standard Node idiom for "give me a free port".
 *
 * @returns {Promise<number>}
 */
export function pickFreePort() {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.unref();
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
  });
}

/**
 * Poll http://127.0.0.1:<port>/readyz until it returns 200 or we time out.
 *
 * @param {number} port
 * @param {{ timeoutMs?: number, intervalMs?: number, signal?: AbortSignal }} [opts]
 * @returns {Promise<void>}
 */
export async function waitForReady(port, opts = {}) {
  const timeoutMs = opts.timeoutMs ?? 10_000;
  const intervalMs = opts.intervalMs ?? 100;
  const deadline = Date.now() + timeoutMs;
  let lastErr;
  while (Date.now() < deadline) {
    if (opts.signal?.aborted) throw new Error('aborted while waiting for /readyz');
    try {
      const res = await fetch(`http://127.0.0.1:${port}/readyz`, {
        signal: AbortSignal.timeout(Math.min(intervalMs * 5, 2000)),
      });
      if (res.ok) return;
      lastErr = new Error(`/readyz returned ${res.status}`);
    } catch (err) {
      lastErr = err;
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(
    `ably-server did not become ready on 127.0.0.1:${port} within ${timeoutMs}ms` +
      (lastErr ? ` (last: ${lastErr.message})` : ''),
  );
}

/**
 * Supervises a single embedded ably-server child process.
 *
 * Emits:
 *   'ready'   (port)            child is up and /readyz is green
 *   'spawn'   (pid)             a child process was started
 *   'exit'    ({code, signal})  the child exited
 *   'restart' (attempt)         a restart is being attempted after a crash
 *   'error'   (err)             a fatal supervision error (e.g. can't start)
 */
export class AblyServer extends EventEmitter {
  /**
   * @param {object} [opts]
   * @param {string} [opts.apiKey]        API key appId.keyId:keySecret (default app.key:secret or $ABLY_SERVER_API_KEY)
   * @param {string} [opts.binaryPath]    override binary path (default resolveBinaryPath())
   * @param {number} [opts.port]          fixed internal port (default: OS-assigned free port)
   * @param {string} [opts.mode]          storage mode: 'memory' (default), 'disk' (needs dataDir), 'cluster' (needs dbDsn)
   * @param {string} [opts.dataDir]       data dir for disk mode (passed as --data-dir)
   * @param {string} [opts.dbDsn]         Postgres DSN for cluster mode (passed as --db-dsn)
   * @param {string} [opts.logLevel]      server log level (default 'info')
   * @param {string} [opts.shutdownGrace] graceful-shutdown window passed to the child (default '10s')
   * @param {number} [opts.readyTimeoutMs] ready poll timeout (default 10000)
   * @param {number} [opts.maxRestarts]   max consecutive crash-restarts before giving up (default 5)
   * @param {(line:string)=>void} [opts.onChildLog] receive child stdout/stderr lines (default: write to process.stderr; pass () => {} to mute)
   */
  constructor(opts = {}) {
    super();
    this.apiKey = opts.apiKey ?? process.env.ABLY_SERVER_API_KEY ?? 'app.key:secret';
    this.binaryPath = opts.binaryPath ?? resolveBinaryPath();
    this.fixedPort = opts.port ?? null;
    this.mode = opts.mode ?? 'memory';
    this.dataDir = opts.dataDir ?? null;
    this.dbDsn = opts.dbDsn ?? null;
    this.logLevel = opts.logLevel ?? 'info';
    this.shutdownGrace = opts.shutdownGrace ?? '10s';
    this.readyTimeoutMs = opts.readyTimeoutMs ?? 10_000;
    this.maxRestarts = opts.maxRestarts ?? 5;
    // Forward the child's logs so the embedded server is not silent by
    // default; pass a custom function to redirect, or () => {} to mute.
    this.onChildLog = opts.onChildLog ?? ((line) => process.stderr.write(`[ably-server] ${line}\n`));

    /** @type {import('node:child_process').ChildProcess | null} */
    this.child = null;
    /** @type {number | null} chosen internal port */
    this.port = null;
    this._stopping = false;
    this._restartCount = 0;
    this._started = false;
  }

  /**
   * Start the child and wait until /readyz is green.
   * @returns {Promise<{ port: number, pid: number }>}
   */
  async start() {
    if (this._started) throw new Error('supervisor already started');
    this._started = true;

    if (!existsSync(this.binaryPath)) {
      // Throw (not emit): start() is awaited, so a rejected promise is the
      // idiomatic surface for a startup misconfiguration. Emitting 'error'
      // here as well would, with no listener attached, crash the process.
      throw new Error(
        `ably-server binary not found at "${this.binaryPath}". ` +
          `Build it first (go build -o bin/ably-server ./cmd/ably-server) ` +
          `or set ABLY_SERVER_BINARY to its path.`,
      );
    }

    this.port = this.fixedPort ?? (await pickFreePort());
    await this._spawnChild();
    await waitForReady(this.port, { timeoutMs: this.readyTimeoutMs });
    this.emit('ready', this.port);
    return { port: this.port, pid: this.child.pid };
  }

  async _spawnChild() {
    const args = [
      '--mode', this.mode,
      '--listen', `127.0.0.1:${this.port}`,
      '--log-level', this.logLevel,
      '--shutdown-grace', this.shutdownGrace,
    ];
    if (this.dataDir) args.push('--data-dir', this.dataDir);
    if (this.dbDsn) args.push('--db-dsn', this.dbDsn);
    const child = spawn(this.binaryPath, args, {
      env: { ...process.env, ABLY_SERVER_API_KEY: this.apiKey },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    this.child = child;
    this.emit('spawn', child.pid);

    const pump = (stream) => {
      stream.setEncoding('utf8');
      let buf = '';
      stream.on('data', (chunk) => {
        buf += chunk;
        let nl;
        while ((nl = buf.indexOf('\n')) >= 0) {
          const line = buf.slice(0, nl);
          buf = buf.slice(nl + 1);
          if (this.onChildLog && line.length) this.onChildLog(line);
        }
      });
    };
    if (this.onChildLog) {
      pump(child.stdout);
      pump(child.stderr);
    } else {
      child.stdout.resume();
      child.stderr.resume();
    }

    child.on('exit', (code, signal) => {
      this.emit('exit', { code, signal });
      this.child = null;
      if (this._stopping) return;
      // Unexpected crash: restart on a backoff.
      this._restartCount += 1;
      if (this._restartCount > this.maxRestarts) {
        this.emit(
          'error',
          new Error(`ably-server crashed ${this._restartCount} times; giving up`),
        );
        return;
      }
      const backoffMs = Math.min(100 * 2 ** (this._restartCount - 1), 2000);
      this.emit('restart', this._restartCount);
      setTimeout(() => {
        if (this._stopping) return;
        this._spawnChild()
          .then(() => waitForReady(this.port, { timeoutMs: this.readyTimeoutMs }))
          .then(() => {
            // A clean re-ready resets the crash counter so isolated, far-apart
            // crashes don't accumulate toward the give-up threshold.
            this._restartCount = 0;
            this.emit('ready', this.port);
          })
          .catch((err) => this.emit('error', err));
      }, backoffMs);
    });

    child.on('error', (err) => {
      if (!this._stopping) this.emit('error', err);
    });
  }

  /**
   * Gracefully stop the child: SIGTERM then wait for exit (SIGKILL on grace
   * timeout). Idempotent.
   * @param {{ timeoutMs?: number }} [opts]
   * @returns {Promise<void>}
   */
  async stop(opts = {}) {
    this._stopping = true;
    const child = this.child;
    if (!child || child.exitCode !== null || child.signalCode !== null) {
      return;
    }
    const timeoutMs = opts.timeoutMs ?? 12_000;
    await new Promise((resolve) => {
      const onExit = () => {
        clearTimeout(killTimer);
        resolve();
      };
      child.once('exit', onExit);
      const killTimer = setTimeout(() => {
        // Grace exceeded: force-kill.
        try {
          child.kill('SIGKILL');
        } catch {
          /* already gone */
        }
      }, timeoutMs);
      try {
        child.kill('SIGTERM');
      } catch {
        onExit();
      }
    });
    this.child = null;
  }
}

/**
 * Convenience: construct + start an AblyServer in one call.
 * @param {ConstructorParameters<typeof AblyServer>[0]} [opts]
 * @returns {Promise<AblyServer>}
 */
export async function startEmbeddedServer(opts) {
  const server = new AblyServer(opts);
  await server.start();
  return server;
}

// Back-compat alias. AblyServer is the name a developer holds; "supervisor"
// is the internal role, not the public noun.
export const AblyServerSupervisor = AblyServer;
