#!/usr/bin/env node
// Tiny helper: print the resolved binary path (used in the README / debugging).
import { resolveBinaryPath } from './supervise.js';
process.stdout.write(resolveBinaryPath() + '\n');
