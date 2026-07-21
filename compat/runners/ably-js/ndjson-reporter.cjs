'use strict';
// Streaming mocha reporter for the compatibility harness: one NDJSON line per
// test event, appended to the file named by MOCHA_NDJSON_OUT as each test
// settles. Unlike mocha's built-in json reporter (which emits only at the very
// end), results survive the process being killed at the per-file cap — only the
// unreached tail is lost.
//
// Deliberately dependency-free (no require('mocha')): a reporter is just a
// constructor receiving the runner, so subscribing by event name keeps this
// loadable by absolute path on any mocha version — including the one in the
// ably-js checkout this runner drives.
const fs = require('fs');

module.exports = function NdjsonReporter(runner) {
  const out = process.env.MOCHA_NDJSON_OUT;
  const write = (obj) => {
    if (!out) return;
    // appendFileSync: durable per line; volumes are small (hundreds of tests).
    fs.appendFileSync(out, JSON.stringify(obj) + '\n');
  };

  runner.on('start', () => write({ event: 'start', at: Date.now() }));
  runner.on('pass', (test) => write({ event: 'pass', fullTitle: test.fullTitle(), duration: test.duration }));
  runner.on('fail', (test, err) =>
    write({
      event: 'fail',
      fullTitle: test.fullTitle(),
      duration: test.duration,
      err: err && err.message,
    }),
  );
  runner.on('pending', (test) => write({ event: 'pending', fullTitle: test.fullTitle() }));
  runner.on('end', () => write({ event: 'end', at: Date.now(), stats: runner.stats }));
};
