---
id: TASK-79
title: Fix idempotent-publishing tests still failing after TASK-18
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 19:21'
updated_date: '2026-07-09 19:52'
labels:
  - protocol
  - compat
dependencies:
  - TASK-18
priority: high
ordinal: 79000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md was generated at HEAD, which includes TASK-18 (batch id stamping + storage idempotency), yet TestIdempotentPublishing and TestIdempotent_retry still report duplicate publishes not being de-duplicated. Diagnose against ably-go's idempotent REST publishing (RSL1k: the SDK pre-stamps client-side ids of the form <base>:<idx> and retries the same request): trace where the duplicate slips through — id validation, the idempotency index lookup, or response mapping — and fix it.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 TestIdempotentPublishing passes against a local server
- [ ] #2 TestIdempotent_retry passes against a local server
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Reproduce TestIdempotentPublishing + TestIdempotent_retry with the harness.
2. Diagnose each failing subtest.
3. Fix server-side gaps; document any harness-side failures with evidence.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Findings:

TestIdempotentPublishing — server-side idempotency (RSL1k2/RSL1k5) already de-dupes correctly (the 'when ID is included' subtest, 3 identical publishes -> 1 stored message, PASSES). The ONE failing subtest was RSL1k3: publishing 3 messages with the same non-conforming id ('randomStr', no ':idx'). The server rejected it (correct) but with a plain-text 400; the SDK, unable to parse a code from the body, defaulted it to 40000, while the test expects 40031. Fix: map storage.ErrInvalidMessageID to an Ably error envelope with code 40031 ('invalid publish request (invalid client-specified id)') in HandlePublish (internal/rest/server.go), via the writeErrorInfo helper (body + X-Ably-Errorcode header). -> TestIdempotentPublishing PASSES.

TestIdempotent_retry — HARNESS ARTIFACT, not a server gap. The test builds its proxy target as defaultURL := url.Parse(ably.ApplyOptionsWithDefaults(nopts...).RestURL()), where nopts omits WithPort(LocalPort). Under the local-server hack the port (9010) is injected only by app.Options, so defaultURL resolves to http://localhost with the default HTTP port 80. Token requests are proxied there and fail with 'proxyconnect tcp: dial tcp [::1]:80: connect: connection refused' (retryCount stays 0 — the request never reaches the server). This is the same class of failure the compat report classifies as a harness artifact for the host-fallback tests (single hostname endpoint, port injected out-of-band). The server cannot influence where the SDK's HTTP proxy dials. Server-side idempotent-retry semantics are covered by the passing RSL1k5 dedup subtest and a new unit test.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Return Ably code 40031 for non-conforming idempotent publish ids.

TestIdempotentPublishing: the only failing subtest was RSL1k3 — a multi-message publish whose client-supplied ids don't follow the required <batchID>:<idx> shape. The server correctly rejected it but with a plain-text 400, so the SDK defaulted the code to 40000 instead of the expected 40031. Server-side idempotency (dedup of repeated publishes) already worked.

Change: HandlePublish (internal/rest/server.go) now maps storage.ErrInvalidMessageID to an Ably error envelope with code 40031 ('invalid publish request (invalid client-specified id)') via writeErrorInfo, which also sets X-Ably-Errorcode/Errormessage.

Tests:
- New TestPublishRejectsNonConformingBatchIDs (40031 body + header) and TestPublishIdempotentDuplicateReturnsOnce (repeated publish -> single history entry).
- go build/vet/test ./... pass; harness TestIdempotentPublishing PASSES.

TestIdempotent_retry (AC#2) left unchecked — harness artifact, not a server gap: the test computes its proxy target from options that omit the local WithPort, so token requests are proxied to localhost:80 and fail with connection-refused before reaching the server (retryCount=0). Same class as the compat report's host-fallback harness artifacts. Server-side retry idempotency is verified via the passing RSL1k5 dedup subtest and the new unit test.
<!-- SECTION:FINAL_SUMMARY:END -->
