// run.mjs — run the ably-js node test suite against a local Ably-compatible
// server and emit a shared JSON report for compat-gate to diff against
// compat/known-failures/ably-js.toml.
//
// This runner lives in ably-server (not ably-js): ably-server's CI is the only
// caller. It drives a caller-supplied ably-js checkout (--sdk-dir), which needs
// only the inert ABLY_LOCAL_SANDBOX_URL support in its own test suite. See
// compat/README.md for the shared contract, verdict vocabulary, and policy.
//
// A running local sandbox provisioner is a prerequisite — the caller manages it, like
// a database. It must expose the Ably test-app provisioning API: POST /apps
// (returns the app's keys plus the endpoint/port/tls to reach the isolated
// server it booted for that app), DELETE /apps/{id}, and POST /stats. For the
// reference open-source server:
//
//     go run ./cmd/ably-local-sandbox --listen :9010
//
// Point this script at it with --url / ABLY_LOCAL_SANDBOX_URL (default
// http://localhost:9010). Each test file runs in its OWN mocha process (so a
// panic/crash against an unimplemented endpoint doesn't abort the batch) across
// a worker pool; every process provisions its own fresh app via the local sandbox and
// routes its clients at the server the local sandbox returns (see testapp_manager.js
// and client_module.js in the ably-js checkout). A streaming NDJSON reporter
// records each test as it settles, so a file killed at its cap still yields
// results for everything that ran.
//
// The runner is policy-free: it reports pass/fail/skip/timeout/error per test
// and a completed run exits 0 regardless of individual verdicts. Deciding which
// failures are acceptable (diffing against a known-failures list) is compat-gate's
// job. It exits non-zero only when the run could not complete (no local sandbox,
// missing build, or no matching files).
//
// Usage:
//   node compat/runners/ably-js/run.mjs [options] [files...]
//
// Options:
//   -s, --sdk-dir DIR     path to the ably-js checkout to test
//                         (default $ABLY_JS_DIR or ../ably-js beside ably-server)
//   -u, --url URL         local sandbox provisioner base URL
//                         (default $ABLY_LOCAL_SANDBOX_URL or http://localhost:9010)
//   -j, --jobs N          test files to run concurrently (default 4)
//   -t, --timeout SEC     per-file timeout in seconds (default 300)
//   -o, --out PATH        JSON report path (default compat-results-ably-js.json)
//       --transports LIST comma-separated transports to restrict the suite to
//                         (sets ABLY_TEST_TRANSPORTS, e.g. web_socket)
//   -h, --help            show this help
//   files...              run only these test files (path or basename match);
//                         defaults to test/{unit,rest,realtime}/*.test.js
import { spawn, execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const serverRoot = path.resolve(scriptDir, '..', '..', '..');
const reporter = path.join(scriptDir, 'ndjson-reporter.cjs');
const streamDir = path.join(serverRoot, 'tmp', 'compatibility', 'stream');

// --- arg parsing ------------------------------------------------------------
const argv = process.argv.slice(2);
const opts = {
  sdkDir: process.env.ABLY_JS_DIR || path.resolve(serverRoot, '..', 'ably-js'),
  url: process.env.ABLY_LOCAL_SANDBOX_URL || 'http://localhost:9010',
  jobs: 4,
  timeoutSec: 300,
  out: path.join(serverRoot, 'compat-results-ably-js.json'),
  transports: process.env.ABLY_TEST_TRANSPORTS || null,
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
    case '--transports':
      opts.transports = next();
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
  fail(`ably-js checkout not found at ${sdkDir} — pass --sdk-dir DIR or set ABLY_JS_DIR`);
}
if (!fs.existsSync(path.join(sdkDir, 'build', 'ably-node.js'))) {
  fail(
    `build/ably-node.js not found in ${sdkDir} — build the library first:\n` +
      '  (cd ' + sdkDir + ' && npm run build:node && npm run build:push && npm run build:liveobjects)',
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
const allFiles = ['unit', 'rest', 'realtime'].flatMap((d) => {
  const dir = path.join(sdkDir, 'test', d);
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir)
    .filter((f) => f.endsWith('.test.js'))
    .map((f) => `test/${d}/${f}`);
});
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

// --- run one file in its own mocha process ----------------------------------
const mochaBin = path.join(sdkDir, 'node_modules', '.bin', 'mocha');

function runMocha(file, streamPath) {
  const env = {
    ...process.env,
    ABLY_LOCAL_SANDBOX_URL: localSandboxURL,
    MOCHA_NDJSON_OUT: streamPath,
  };
  if (opts.transports) env.ABLY_TEST_TRANSPORTS = opts.transports;
  // Invoke the mocha binary directly (an npx wrapper would orphan the real
  // process on kill); --reporter overrides the repo's default reporter (loaded
  // by absolute path from this runner); --exit because some suites leave
  // dangling handles that hold the event loop open.
  return new Promise((resolve) => {
    const child = spawn(mochaBin, ['--exit', '--reporter', reporter, file], { cwd: sdkDir, env });
    let stderr = '';
    child.stdout.resume(); // discard; results arrive via the NDJSON stream
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
fs.mkdirSync(streamDir, { recursive: true });
const queue = [...files];
const fileResults = []; // per-file summary, in completion order

function verdictOf(event) {
  return event === 'pass' ? 'pass' : event === 'fail' ? 'fail' : 'skip';
}

async function runFile(workerId, file) {
  const slug = file.replace(/\W+/g, '-');
  const streamPath = path.join(streamDir, `${slug}.ndjson`);
  fs.rmSync(streamPath, { force: true });
  const started = Date.now();
  const run = await runMocha(file, streamPath);
  const durationMs = Date.now() - started;

  let events = [];
  try {
    events = fs
      .readFileSync(streamPath, 'utf8')
      .split('\n')
      .filter(Boolean)
      .map((l) => JSON.parse(l));
  } catch {
    /* no stream file */
  }

  // No reporter output at all ⇒ the process crashed or failed to start; the
  // whole file is a harness-level error (e.g. a server the process can't reach,
  // or a build/require failure).
  if (!events.length) {
    fileResults.push({
      file,
      durationMs,
      error: `no reporter output (exit ${run.code}); stderr tail: ${run.stderr.slice(-500).trim()}`,
      tests: [],
    });
    console.log(`[w${workerId}] ${file}: ERROR after ${Math.round(durationMs / 1000)}s`);
    return;
  }

  const tests = events
    .filter((e) => e.event === 'pass' || e.event === 'fail' || e.event === 'pending')
    .map((e) => ({
      name: e.fullTitle,
      verdict: verdictOf(e.event),
      duration: e.duration != null ? Number((e.duration / 1000).toFixed(3)) : 0,
      err: e.err,
    }));
  // No 'end' event ⇒ killed at the cap: partial results, unreached tail unknown.
  const truncated = !events.some((e) => e.event === 'end');

  fileResults.push({ file, durationMs, truncated: truncated || undefined, tests });
  const p = tests.filter((t) => t.verdict === 'pass').length;
  const f = tests.filter((t) => t.verdict === 'fail').length;
  const s = tests.filter((t) => t.verdict === 'skip').length;
  console.log(
    `[w${workerId}] ${file}: ${p} passed, ${f} failed, ${s} skipped (${Math.round(durationMs / 1000)}s)` +
      (truncated ? ' TIMED OUT AT CAP' : ''),
  );
}

function writeReport(complete) {
  // Flat per-test verdict list, plus a synthetic entry per file that could not
  // produce per-test results so nothing is silently dropped. `name` is
  // file-qualified (file::fullTitle) — mocha titles aren't unique across files,
  // so the file prefix is what makes a name a stable key for compat-gate and the
  // known-failures list to match on.
  const results = [];
  for (const r of fileResults) {
    for (const t of r.tests) {
      results.push({ file: r.file, name: `${r.file}::${t.name}`, verdict: t.verdict, duration: t.duration, err: t.err });
    }
    if (r.error) {
      results.push({ file: r.file, name: `${r.file}::(file did not run)`, verdict: 'error', duration: 0, err: r.error });
    } else if (r.truncated) {
      results.push({ file: r.file, name: `${r.file}::(suite did not complete)`, verdict: 'timeout', duration: 0 });
    }
  }
  results.sort((a, b) => a.name.localeCompare(b.name));

  const tally = {};
  for (const r of results) tally[r.verdict] = (tally[r.verdict] || 0) + 1;

  const report = {
    generatedAt: new Date().toISOString(),
    sdk: 'ably-js',
    sdkRev: gitHead(),
    localSandboxURL,
    jobs: opts.jobs,
    transports: opts.transports || null,
    complete,
    tally,
    files: fileResults.map((r) => ({
      file: r.file,
      durationMs: r.durationMs,
      truncated: r.truncated,
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

console.log(`>> testing ably-js checkout at ${sdkDir}`);
console.log(`>> using local sandbox at ${localSandboxURL}`);
console.log(`>> ${files.length} test files across ${opts.jobs} workers, ${opts.timeoutSec}s cap each`);
if (opts.transports) console.log(`>> restricting transports to: ${opts.transports}`);

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
