---
id: TASK-11
title: Resolve and stamp clientId (from JWT claim or URL param)
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 16:05'
updated_date: '2026-06-25 00:41'
labels:
  - auth
dependencies:
  - TASK-9
ordinal: 11000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Implement clientId resolution per DESIGN.md §3.2: derive the connection/request clientId from the x-ably-clientId JWT claim, or from the clientId query param when a basic API key is used, following the full resolution table in §3.2 — including the `*` wildcard (bearer may assume any clientId, but `*` is never itself a clientId) and every reject case. Stamp the resolved clientId onto every outbound Message.clientId for that connection, and NACK inbound messages that assert a different clientId. Depends on JWT auth.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 clientId resolution implements the full DESIGN §3.2 table (Basic+param, JWT claim concrete/absent/wildcard × param), including every reject case and the '*' wildcard semantics
- [x] #2 Resolution is centralised in internal/auth and reused by both realtime (per-connection) and REST (per-request)
- [x] #3 Reject cases fail at auth time: HTTP 401 (REST) / pre-upgrade 401 (WS)
- [x] #4 Resolved non-empty clientId is stamped onto outbound Message.clientId for that connection/request
- [x] #5 Inbound MESSAGE asserting a clientId different from the resolved value is NACKed (WS) / 400 (REST)
- [x] #6 Existing presence clientId handling (§12.3) is reconciled with the centralised resolver
- [x] #7 Unit + realtime tests cover the resolution matrix, stamping, and the NACK path
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Implement clientId resolution and stamping per DESIGN.md §3.2, centralised in internal/auth and reused by realtime and REST.

What changed:
- auth.ResolveClientID(principal, param) maps (credential, clientId param) -> the connection/request identity: concrete, '*' (wildcard — may assume any identity per operation), or '' (anonymous). Implements the full §3.2 table including the reject cases (e.g. a no-clientId token may not assert one; a concrete-claim token may not be overridden).
- auth.MessageClientID(connClientID, msgClientID) applies the per-message rule: anonymous may assert none; wildcard may assume any concrete identity (or none); concrete is stamped when omitted and must match otherwise.
- Realtime: the WS upgrade resolves the connection's clientId (rejecting a disallowed one with a pre-upgrade 401) and stores it; handleMessage stamps/NACKs each published message.
- REST: authenticate() now returns the verified *Principal; HandlePublish resolves the request clientId and stamps/400s each message.
- Reconciled the realtime wildcard constant with auth.WildcardClientID so connection resolution and the presence/message rules share one definition.

Design correction: DESIGN §3.2 said Basic+no-param and wildcard-claim+no-param resolve to 'anonymous'. Per the Ably identified-clients docs a key holder (Basic) and a '*' token may assume ANY identity, so both resolve to wildcard. Updated the §3.2 table, the three-identity prose, and the rejection wording (pre-upgrade 401, not a WS ERROR frame).

Scope: capability/ownership gating (TASK-12/51) is still absent; REST mutation operator-clientId stamping is left to the ownership work. The token query param is access_token (accessToken also accepted), per TASK-9.

Tests: auth TestResolveClientID + TestMessageClientID (full §3.2 matrix); realtime TestPublishStampsAndRejectsClientID (omitted clientId stamped + delivered as 'alice', mismatched clientId NACKed). go build / go vet / go test ./... all pass.
<!-- SECTION:FINAL_SUMMARY:END -->
