---
id: TASK-78
title: Fix inband/server-initiated AUTH tests still failing after TASK-17
status: To Do
assignee: []
created_date: '2026-07-09 19:21'
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
