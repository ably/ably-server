---
id: TASK-70
title: Add a LICENSE file per the PDR-090 licensing decision
status: To Do
assignee: []
created_date: '2026-07-09 11:06'
labels: []
dependencies: []
documentation:
  - 'https://ably.atlassian.net/wiki/spaces/product/pages/5171281935'
priority: high
ordinal: 70000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The repo has no LICENSE file, which blocks any source-available release. PDR-090's licensing section leans open-source ('the simplest approach that maximises our chances of having developer and LLM mindshare is to go open-source') and DESIGN.md §15 already states Apache 2.0; the RFC leaves per-directory licensing of the future protocol-core mirror as an open question. Confirm the decision with the PDR-090 approvers and add the LICENSE (plus any per-file headers or NOTICE file the chosen licence requires).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Licence confirmed against the PDR-090 outcome
- [ ] #2 LICENSE file present at the repo root; NOTICE/headers added if required
- [ ] #3 README states the licence
<!-- AC:END -->
