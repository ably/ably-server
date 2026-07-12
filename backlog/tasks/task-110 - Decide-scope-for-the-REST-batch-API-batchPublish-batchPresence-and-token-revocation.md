---
id: TASK-110
title: >-
  Decide scope for the REST batch API (batchPublish/batchPresence) and token
  revocation
status: To Do
assignee: []
created_date: '2026-07-12 13:59'
labels:
  - compat
  - ably-js
  - scoping
dependencies: []
ordinal: 110000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
rest/batch (2026-07-12 ably-js run): batchPublish 'when invoked with an array of specs' / 'a single spec' and revokeTokens x2 get 404 'requested resource not found' (POST /messages batch endpoint and POST /keys/{keyName}/revokeTokens don't exist); 'performs a batch presence fetch' times out (GET /presence batch). These are Ably-cloud product surface not listed in DESIGN.md section 1's non-goals. Decision needed (mirrors TASK-34/35/99): implement the batch endpoints (batch publish is plausibly in scope — it's just multi-channel publish; batch presence a get across channels), and/or declare token revocation a non-goal (it presupposes revocable tokens, which the single-key model doesn't have). Document the outcome in DESIGN.md either way.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Decision recorded for batchPublish, batchPresence, and revokeTokens: in scope or documented non-goal
<!-- AC:END -->
