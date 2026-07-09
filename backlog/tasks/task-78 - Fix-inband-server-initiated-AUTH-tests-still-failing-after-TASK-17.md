---
id: TASK-78
title: Fix inband/server-initiated AUTH tests still failing after TASK-17
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 19:21'
updated_date: '2026-07-09 20:51'
labels:
  - auth
  - compat
dependencies:
  - TASK-17
priority: high
ordinal: 78000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md was generated at HEAD (63273b0), which includes the TASK-17 inband AUTH implementation — yet RTN22_RTC8_Integration_ServerInitiatedAuth fails and RTN22a_RTN15h2 / RTC8a_ExplicitAuthorizeWhileConnected panic, with the report noting 'server never initiates AUTH; authorize-while-connected has nothing to talk to'. Diagnose against ably-go's expectations: the RTN22 tests likely need the server to send AUTH in scenarios beyond the 30s-before-expiry prompt (e.g. short-TTL tokens where the prompt window exceeds the TTL), and RTC8a exercises client-initiated AUTH mid-connection — verify the inbound AUTH path responds the way the SDK expects (CONNECTED with updated ConnectionDetails) and fix whatever mismatch remains.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 RTN22_RTC8_Integration_ServerInitiatedAuth passes against a local server
- [ ] #2 RTN22a_RTN15h2 and RTC8a_ExplicitAuthorizeWhileConnected no longer panic and pass
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Reproduce RTN22_RTC8, RTN22a_RTN15h2, RTC8a against HEAD (TASK-17 is in HEAD, report predates it).
2. RTN22 (33s token): expect AUTH prompt ~3s before expiry -> inband renew -> UPDATE, looped. Verify preExpiryWindow logic and CONNECTED reply.
3. RTN22a (3s token < 30s window): SDK expects token to expire -> DISCONNECTED(40142) -> reconnect. Server must NOT prompt AUTH when TTL <= preExpiryWindow; just disconnect at expiry.
4. RTC8a: client-initiated AUTH mid-connection -> CONNECTED with updated details -> SDK UPDATE event.
5. Fix genuine divergences; add internal/realtime unit tests pinning behaviour.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
ROOT CAUSES (diagnosed against ably-go server-testing @5c7a727, ably-server HEAD which DOES include TASK-17 d0768f1 — the 2026-07-09 compat report predates TASK-17 at 63273b0):

1. RTN22 and RTN22a are HARNESS ARTIFACTS, not server gaps. Both build ably.NewRealtime() directly with only WithEndpoint(ablytest.Endpoint); they omit the WithPort(LocalPort)+WithTLS(false)+WithInsecureAllowBasicAuthWithoutTLS() overrides the harness applies only inside app.Options()/NewREST()/app.NewRealtime(). So they dial https://localhost:443 (default TLS port), which the plaintext :9010 server does not serve. Evidence: every dial logs 'dial tcp [::1]:443: connect: connection refused'; zero server-side connection traces fire. RTN22a's PANIC is a per-test timeout because the connection never establishes. RTC8a4 (a subtest) has the same defect.

2. RTC8a (uses app.NewRealtime, so it DOES reach the server) exposed one genuine server divergence: a failed inband AUTH must move the SDK connection to FAILED, but the server was replying with DISCONNECTED code 40140. 40140-40149 (status 401) is the SDK's *renewable* token-error range (realtime_client.go isTokenError), so with a renewable authCallback the SDK ran the RTN15h2 reconnect loop instead of failing -> RTC8a2 hung to timeout.

FIXES (internal/realtime/reauth.go):
- failReauth now sends an ERROR frame (was DISCONNECTED) with a non-renewable credential code: 40101 invalid/missing token, 40102 incompatible clientId. A connection-level ERROR with a non-token code drives the SDK to FAILED (failedConnSideEffects). Token *expiry* stays DISCONNECTED 40142 (renewable -> reconnect), which is correct for RTN15h2/RTN22a.
- authLoop: only arm the AUTH prompt when remaining token lifetime > noWarningMargin (5500ms); shorter tokens expire without a prompt. This matches the reference server (go/realtime lib/auth/auth.go ExpiryTimers.Reset: expiresSoon set only when expiresIn > NoWarningMargin; WarnBeforeExpiryTime=30s, NoWarningMargin=5.5s) and is exactly the RTN22 (33s->prompt+renew) vs RTN22a (3s->expire+reconnect) distinction.

HARNESS VERIFICATION (local server @ working tree):
- RTC8a1 (successful reauth x2): PASS; RTC8a2 (failed reauth -> FAILED): PASS; RTC8a3 (authorize waits for update): PASS. RTC8a no longer panics.
- RTC8a1 'capabilities downgrade' subtest still FAILs: it expects an attached channel to move to FAILED when the reauth narrows capability — that is capability enforcement on reauth, TASK-12 (not yet landed), out of scope here.
- RTC8a4 and RTN22/RTN22a cannot be exercised: harness artifact (see #1).

Server behaviour pinned by unit tests in internal/realtime/reauth_test.go: TestReauthSuccess (prompt+inband renew->CONNECTED survives = RTN22 server side), TestShortTokenExpiresWithoutPrompt (sub-margin token -> DISCONNECTED 40142 with no prompt = RTN22a server side), TestReauthInvalidToken/TestReauthMissingToken/TestReauthIncompatibleClientID (bad inband token -> ERROR 40101/40102 = RTC8a2 server side), TestAuthPromptAndExpiry.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Fixed the one genuine server divergence in the inband-AUTH suite and pinned the corrected behaviour with unit tests; the residual test failures are a harness artifact and an out-of-scope capability-enforcement gap.

Server changes (internal/realtime/reauth.go): (a) a rejected inband AUTH now emits an ERROR frame with a non-renewable credential code (40101/40102) so the SDK moves the connection to FAILED (RTC8a2), rather than a DISCONNECTED 40140 that the SDK treated as renewable and reconnect-looped; token expiry keeps DISCONNECTED 40142 (renewable). (b) the AUTH prompt is armed only when remaining token lifetime exceeds a 5.5s no-warning margin, matching the reference server — short-lived tokens expire without a prompt (RTN22a), longer ones prompt ~30s before expiry (RTN22). DESIGN.md §3 updated.

Harness verification: RTC8a1/RTC8a2/RTC8a3 PASS; RTC8a no longer panics. RTC8a1 capabilities-downgrade subtest still fails (needs capability enforcement on reauth = TASK-12). RTN22, RTN22a and RTC8a4 cannot run against the local server: those cases construct ably.NewRealtime() manually without the harness WithPort(9010)/WithTLS(false) overrides, so they dial :443 and never reach the server (evidence: 'dial tcp [::1]:443: connection refused', zero server traces).

ACs left unchecked: AC#1 (RTN22) and AC#2 name tests that cannot pass via this harness for the reasons above; the corresponding server behaviour is verified by new/adjusted unit tests in internal/realtime/reauth_test.go.
<!-- SECTION:FINAL_SUMMARY:END -->
