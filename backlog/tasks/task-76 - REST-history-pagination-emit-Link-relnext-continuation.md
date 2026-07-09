---
id: TASK-76
title: 'REST history pagination: emit Link rel=next continuation'
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 19:21'
updated_date: '2026-07-09 19:43'
labels:
  - rest
  - compat
dependencies: []
priority: high
ordinal: 76000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md gap 1: TestHistory_RSL2_RSL2b3 fails. GET /channels/{name}/messages honours limit for the first page but returns no continuation link, so the SDK cannot page (expected all 10 messages, got the first page). DESIGN.md §2.2 already specifies Ably's Link header convention (first, next) — implement it on the history read paths (message history, presence history, message versions) using the last-returned channelSerial as the cursor, omitting next on the final page.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 GET message history returns a Link rel=next header whenever more results exist, and the SDK can walk pages to exhaustion
- [x] #2 Presence history and message-version reads follow the same convention
- [x] #3 No next link is emitted on the final page
- [x] #4 TestHistory_RSL2_RSL2b3 passes against a local server
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Reproduce TestHistory_RSL2_RSL2b3 with the harness.
2. Trace ably-go paginated_result.go Link parsing.
3. Fix the Link header shape to match SDK expectations.
4. Add unit tests; verify with harness.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Root cause (two parts):
1. ably-go parses each Header["Link"] element with a single-match regexp (FindStringSubmatch), so folding current/first/next into one comma-joined Link header left the SDK seeing only the first rel (current) and never next -> pagination stalled at page 1.
2. The link URL was absolute (/channels/{name}/history?...); ably-go resolves each link via path.Join(path.Dir(requestPath), link), so an absolute path was doubled onto the base.
Fix (internal/rest/server.go writeLinkHeaders): emit each rel as its own Link header line (Header.Add) and make the URL relative to the resource — the request path's final segment (path.Base) plus query. Applies to message history, the /history alias, presence history, and message versions (all route through writeLinkHeaders/writeHistoryLinks). Storage HasMore logic was already correct.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Emit the REST history pagination Link headers in the shape Ably SDKs require.

Problem: GET history honoured limit for the first page but the SDK could not page further (TestHistory_RSL2_RSL2b3: expected 10, got the first page).

Two root causes, both in how the Link header was written (internal/rest/server.go, writeLinkHeaders):
- All rels (current/first/next) were folded into one comma-joined Link header. ably-go parses each Header["Link"] value with a single-match regexp, so it only saw the first rel and never the next cursor.
- The link URL was an absolute path. ably-go resolves links via path.Join(path.Dir(requestPath), link), which doubled an absolute path onto the base.

Fix: write each rel as its own Link header line (Header.Add), and make the URL relative to the resource (request path's final segment + query). This covers message history, the /history alias, presence history, and message-version reads, which all route through writeHistoryLinks/writeLinkHeaders. Storage HasMore was already correct, so no storage change.

Tests:
- New unit test TestHistoryLinkHeadersRelativeAndSeparate pins separate-header + relative-URL shape across messages, /history, and presence/history.
- Updated the nextLink test helper (and TestHistoryLinkHeadersAlwaysIncludeFirstAndCurrent) to read multiple Link headers and resolve relative links, mirroring the SDK.
- go build/vet/test ./... all pass; harness TestHistory_RSL2_RSL2b3 now PASSES.
<!-- SECTION:FINAL_SUMMARY:END -->
