// run.mjs — run the ably-ai-transport-js integration suite against a local
// Ably-compatible server and emit a shared JSON report for compat-gate to diff
// against compat/known-failures/ably-ai-transport-js.toml.
//
// This runner lives in ably-server (not ably-ai-transport-js): ably-server's CI
// is the only caller. It drives a caller-supplied ably-ai-transport-js checkout
// (--sdk-dir), which needs only the inert ABLY_LOCAL_SANDBOX_URL support in its
// own test suite. See compat/README.md for the shared contract, verdict
// vocabulary, and policy.
//
// A running local sandbox provisioner is a prerequisite — the caller manages it,
// like a database. It must expose the Ably test-app provisioning API: POST /apps
// (returns the app's keys plus the endpoint/port/tls to reach the isolated
// server it booted for that app), DELETE /apps/{id}, and POST /stats. For the
// reference open-source server:
//
//     go run ./cmd/ably-local-sandbox --listen :9010
//
// Point this script at it with --url / ABLY_LOCAL_SANDBOX_URL (default
// http://localhost:9010). Each integration test file runs in its OWN vitest
// process (so a crash against an unimplemented endpoint doesn't abort the batch)
// across a worker pool; every process runs the suite's globalSetup, which
// provisions its own fresh app via the local sandbox and routes its clients at
// the server the sandbox returns (see test/helper/test-setup.ts and
// realtime-client.ts in the ably-ai-transport-js checkout).
//
// The suite is TypeScript run directly by vitest (no build step); it imports the
// library from src/, so only an installed node_modules is required. vitest's
// JSON reporter writes per-file results only when the file completes, so a file
// killed at its cap yields no per-test rows — it is reported as a single
// `timeout` entry rather than partial results (unlike the ably-js runner's
// streaming reporter).
//
// The runner is policy-free: it reports pass/fail/skip/timeout/error per test
// and a completed run exits 0 regardless of individual verdicts. Deciding which
// failures are acceptable (diffing against a known-failures list) is compat-gate's
// job. It exits non-zero only when the run could not complete (no local sandbox,
// missing node_modules, or no matching files).
//
// Usage:
//   node compat/runners/ably-ai-transport-js/run.mjs [options] [files...]
//
// Options:
//   -s, --sdk-dir DIR     path to the ably-ai-transport-js checkout to test
//                         (default $ABLY_AI_TRANSPORT_JS_DIR or
//                         ../ably-ai-transport-js beside ably-server)
//   -u, --url URL         local sandbox provisioner base URL
//                         (default $ABLY_LOCAL_SANDBOX_URL or http://localhost:9010)
//   -j, --jobs N          test files to run concurrently (default 4)
//   -t, --timeout SEC     per-file timeout in seconds (default 120)
//   -o, --out PATH        JSON report path
//                         (default compat-results-ably-ai-transport-js.json)
//   -h, --help            show this help
//   files...              run only these test files (path or basename match);
//                         defaults to every test/**/*.integration.test.ts
import { spawn, execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const serverRoot = path.resolve(scriptDir, '..', '..', '..');
const reportDir = path.join(serverRoot, 'tmp', 'compatibility', 'ably-ai-transport-js');

// --- arg parsing ------------------------------------------------------------
const argv = process.argv.slice(2);
const opts = {
  sdkDir: process.env.ABLY_AI_TRANSPORT_JS_DIR || path.resolve(serverRoot, '..', 'ably-ai-transport-js'),
  url: process.env.ABLY_LOCAL_SANDBOX_URL || 'http://localhost:9010',
  jobs: 4,
  timeoutSec: 120,
  out: path.join(serverRoot, 'compat-results-ably-ai-transport-js.json'),
  files: [],
};
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  const next = () => argv[++i];
  switch (a) {
    case '-s':
    case '--sdk-dir':
      opts.sdkDir = next();
      break;
    case '-u':
    case '--url':
      opts.url = next();
      break;
    case '-j':
    case '--jobs':
      opts.jobs = Number(next());
      break;
    case '-t':
    case '--timeout':
      opts.timeoutSec = Number(next());
      break;
    case '-o':
    case '--out':
      opts.out = path.resolve(next());
      break;
    case '-h':
    case '--help':
      printHelp();
      process.exit(0);
      break;
    default:
      if (a.startsWith('-')) fail(`unknown option: ${a}`);
      opts.files.push(a);
  }
}

function printHelp() {
  // Print the leading comment block (the usage docs) verbatim.
  const src = fs.readFileSync(fileURLToPath(import.meta.url), 'utf8');
  const lines = src.split('\n');
  for (const line of lines) {
    if (!line.startsWith('//')) break;
    console.log(line.replace(/^\/\/ ?/, ''));
  }
}

function fail(msg) {
  console.error(`error: ${msg}`);
  process.exit(2);
}

const capMs = opts.timeoutSec * 1000;
const localSandboxURL = opts.url.replace(/\/+$/, '');
const sdkDir = path.resolve(opts.sdkDir);

// --- preconditions ----------------------------------------------------------
if (!fs.existsSync(sdkDir)) {
  fail(
    `ably-ai-transport-js checkout not found at ${sdkDir} — ` +
      'pass --sdk-dir DIR or set ABLY_AI_TRANSPORT_JS_DIR',
  );
}
const vitestBin = path.join(sdkDir, 'node_modules', '.bin', 'vitest');
if (!fs.existsSync(vitestBin)) {
  fail(
    `vitest not found in ${sdkDir} — install dependencies first:\n` + '  (cd ' + sdkDir + ' && pnpm install)',
  );
}

// POST /stats is the local sandbox's cheapest always-on endpoint (accepts and
// discards); use it as a liveness probe before enumerating anything.
try {
  const res = await fetch(`${localSandboxURL}/stats`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: '[]',
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
} catch (err) {
  console.error(`error: no local sandbox reachable at ${localSandboxURL} (${err.message || err})`);
  console.error('       start one first, e.g. for the reference open-source server:');
  console.error(`         (cd ${serverRoot} && go run ./cmd/ably-local-sandbox --listen :9010)`);
  console.error('       then re-run, or point --url / ABLY_LOCAL_SANDBOX_URL at your server.');
  process.exit(1);
}

// --- select files -----------------------------------------------------------
function walk(dir) {
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...walk(full));
    else if (entry.name.endsWith('.integration.test.ts')) out.push(full);
  }
  return out;
}
const testRoot = path.join(sdkDir, 'test');
const allFiles = fs.existsSync(testRoot) ? walk(testRoot).map((f) => path.relative(sdkDir, f)) : [];
let files = allFiles;
if (opts.files.length) {
  files = allFiles.filter((f) => opts.files.some((o) => f === o || f.endsWith('/' + o) || f.endsWith(o)));
  const unmatched = opts.files.filter((o) => !files.some((f) => f === o || f.endsWith('/' + o) || f.endsWith(o)));
  if (unmatched.length) fail(`no test files matched: ${unmatched.join(', ')}`);
}
if (!files.length) fail('no matching test files');
// Longest-first (file size as a duration proxy) so big suites don't tail the run.
files.sort((a, b) => fs.statSync(path.join(sdkDir, b)).size - fs.statSync(path.join(sdkDir, a)).size);

function gitHead() {
  try {
    return execFileSync('git', ['-C', sdkDir, 'rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
  } catch {
    return null;
  }
}

// --- run one file in its own vitest process ---------------------------------
function runVitest(file, reportPath) {
  const env = {
    ...process.env,
    ABLY_LOCAL_SANDBOX_URL: localSandboxURL,
  };
  // Invoke the vitest binary directly (a pnpm/npx wrapper would orphan the real
  // process on kill). --config selects the integration project (globalSetup +
  // *.integration.test.ts include); --reporter=json + --outputFile capture the
  // per-test verdicts; the file arg restricts this process to a single file.
  return new Promise((resolve) => {
    const child = spawn(
      vitestBin,
      ['run', '--config', 'vitest.config.integration.ts', '--reporter=json', `--outputFile=${reportPath}`, file],
      { cwd: sdkDir, env },
    );
    let stderr = '';
    child.stdout.resume(); // discard; results arrive via the JSON report file
    child.stderr.on('data', (d) => (stderr += d));
    const cap = setTimeout(() => child.kill('SIGKILL'), capMs);
    child.on('exit', (code) => {
      clearTimeout(cap);
      resolve({ code, stderr });
    });
    child.on('error', (err) => {
      clearTimeout(cap);
      resolve({ code: null, stderr: String(err) });
    });
  });
}

// --- worker pool ------------------------------------------------------------
fs.mkdirSync(reportDir, { recursive: true });
const queue = [...files];
const fileResults = []; // per-file summary, in completion order

function verdictOf(status) {
  return status === 'passed' ? 'pass' : status === 'failed' ? 'fail' : 'skip';
}

async function runFile(workerId, file) {
  const slug = file.replace(/\W+/g, '-');
  const reportPath = path.join(reportDir, `${slug}.json`);
  fs.rmSync(reportPath, { force: true });
  const started = Date.now();
  const run = await runVitest(file, reportPath);
  const durationMs = Date.now() - started;

  let report = null;
  try {
    report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
  } catch {
    /* no report file */
  }

  // No JSON report at all ⇒ the process crashed, failed to start, or was killed
  // at the cap. A kill leaves nothing behind; distinguish a timeout (SIGKILL, no
  // exit code) from a crash so the report carries the right verdict.
  if (!report || !Array.isArray(report.testResults)) {
    const timedOut = run.code === null || durationMs >= capMs;
    fileResults.push({
      file,
      durationMs,
      timedOut,
      error: timedOut
        ? `killed at ${opts.timeoutSec}s cap`
        : `no reporter output (exit ${run.code}); stderr tail: ${run.stderr.slice(-500).trim()}`,
      tests: [],
    });
    console.log(
      `[w${workerId}] ${file}: ${timedOut ? 'TIMED OUT' : 'ERROR'} after ${Math.round(durationMs / 1000)}s`,
    );
    return;
  }

  const tests = report.testResults.flatMap((tr) =>
    (tr.assertionResults || []).map((a) => ({
      name: a.fullName,
      verdict: verdictOf(a.status),
      duration: a.duration != null ? Number((a.duration / 1000).toFixed(3)) : 0,
      err: a.failureMessages && a.failureMessages.length ? a.failureMessages.join('\n') : undefined,
    })),
  );

  fileResults.push({ file, durationMs, tests });
  const p = tests.filter((t) => t.verdict === 'pass').length;
  const f = tests.filter((t) => t.verdict === 'fail').length;
  const s = tests.filter((t) => t.verdict === 'skip').length;
  console.log(`[w${workerId}] ${file}: ${p} passed, ${f} failed, ${s} skipped (${Math.round(durationMs / 1000)}s)`);
}

function writeReport(complete) {
  // Flat per-test verdict list, plus a synthetic entry per file that could not
  // produce per-test results so nothing is silently dropped. `name` is
  // file-qualified (file::fullName) — vitest titles aren't unique across files,
  // so the file prefix is what makes a name a stable key for compat-gate and the
  // known-failures list to match on.
  const results = [];
  for (const r of fileResults) {
    for (const t of r.tests) {
      results.push({ file: r.file, name: `${r.file}::${t.name}`, verdict: t.verdict, duration: t.duration, err: t.err });
    }
    if (r.timedOut) {
      results.push({ file: r.file, name: `${r.file}::(suite did not complete)`, verdict: 'timeout', duration: 0 });
    } else if (r.error) {
      results.push({ file: r.file, name: `${r.file}::(file did not run)`, verdict: 'error', duration: 0, err: r.error });
    }
  }
  results.sort((a, b) => a.name.localeCompare(b.name));

  const tally = {};
  for (const r of results) tally[r.verdict] = (tally[r.verdict] || 0) + 1;

  const report = {
    generatedAt: new Date().toISOString(),
    sdk: 'ably-ai-transport-js',
    sdkRev: gitHead(),
    localSandboxURL,
    jobs: opts.jobs,
    transports: null,
    complete,
    tally,
    files: fileResults.map((r) => ({
      file: r.file,
      durationMs: r.durationMs,
      timedOut: r.timedOut,
      error: r.error,
      passes: r.tests.filter((t) => t.verdict === 'pass').length,
      failures: r.tests.filter((t) => t.verdict === 'fail').length,
      skipped: r.tests.filter((t) => t.verdict === 'skip').length,
    })),
    results,
  };
  fs.mkdirSync(path.dirname(opts.out), { recursive: true });
  fs.writeFileSync(opts.out, JSON.stringify(report, null, 2) + '\n');
  return report;
}

async function worker(workerId) {
  while (queue.length) {
    const file = queue.shift();
    await runFile(workerId, file);
    writeReport(false); // incremental: results visible while the run is live
  }
}

console.log(`>> testing ably-ai-transport-js checkout at ${sdkDir}`);
console.log(`>> using local sandbox at ${localSandboxURL}`);
console.log(`>> ${files.length} test files across ${opts.jobs} workers, ${opts.timeoutSec}s cap each`);

await Promise.all(Array.from({ length: Math.min(opts.jobs, files.length) }, (_, i) => worker(i)));

const report = writeReport(true);

// --- text tally -------------------------------------------------------------
console.log();
console.log('================ compatibility summary ================');
for (const verdict of ['pass', 'fail', 'skip', 'timeout', 'error']) {
  if (report.tally[verdict]) console.log(`${verdict.toUpperCase().padEnd(8)} ${report.tally[verdict]}`);
}
console.log('=======================================================');
for (const verdict of ['fail', 'timeout', 'error']) {
  for (const r of report.results.filter((r) => r.verdict === verdict)) {
    console.log(`${verdict.toUpperCase().padEnd(8)} ${r.name}`);
  }
}
console.log(`>> wrote JSON report to ${opts.out}`);

// A completed run exits 0 regardless of individual verdicts — acceptability is
// compat-gate's call, not this policy-free runner's.
process.exit(0);
