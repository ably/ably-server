---
name: add-task
description: Turn a brief one-line outline into a single well-formed, actionable Backlog.md task. Use when the user gives a short/terse task note and wants it fleshed out and filed into the backlog — e.g. "add a task to …", "expand this into a task", "flesh this out and add it to the backlog", or invokes /add-task with a brief outline.
---

# Add one actionable backlog task from a brief outline

Take the user's terse outline (usually just a few words) and expand it into a single properly-formed task in this repo's Backlog.md backlog (`backlog/` directory). Tasks are created with the `backlog` CLI — see `backlog/AGENT_GUIDELINES.md` for the full command reference.

The workflow is **outline in → expanded task created**.

## Procedure

### 1. Take the outline

Use the outline from the user's message or `/add-task` arguments. This skill produces exactly one task per invocation.

### 2. Understand the repo before expanding

A good expansion is grounded in *this* codebase, not generic boilerplate. Before drafting, do light investigation so the task references real files, packages, and conventions:

- Skim `DESIGN.md` and the relevant `internal/` packages for the area the outline touches.
- Check existing tasks (`backlog task list --plain`) to avoid duplicates and match style.

Keep this proportional — a few targeted reads, not a full audit.

### 3. Expand the outline

The goal is an **actionable task** — enough that someone can pick it up and know what to do. Produce:

- **Title** — imperative and specific (e.g. "Add retry/backoff to the bbolt write path", not "fix storage").
- **Description** — 1–4 sentences: what the task is, why it matters, and any relevant context/pointers to files or packages you found in step 2. This is the heart of the task — make it clear and concrete.
- **Optional** — only when the outline or context clearly warrants it: a short acceptance criterion or two (`--ac`) where "done" would otherwise be ambiguous, or `--priority` / `-l/--labels` / `--depends-on`. Don't pad tasks with criteria for their own sake, and don't invent metadata.

**If the outline is too vague to expand confidently** — you can't tell what "done" looks like, which part of the codebase it touches, or what the intent is — **stop and ask the user** a brief clarifying question before drafting. Don't guess.

### 4. Create the task

Create it with the CLI — usually just a title and description. Pass the description via a quoted `cat` heredoc (the same pattern used for commit messages) so newlines, backticks, quotes, and `$` in the body don't need escaping:

```bash
backlog task create "Add retry/backoff to the bbolt write path" \
  -d "$(cat <<'EOF'
Transient bbolt write failures currently surface directly to publishers.
Wrap writes in internal/storage with bounded retry so brief contention
doesn't drop messages.
EOF
)"
```

The `'EOF'` (quoted) disables interpolation, so the body is taken literally. Add `--ac "…"` (repeatable) only when a criterion is genuinely worth pinning down. After creating, confirm with the task ID and title.

Note: `auto_commit` is off in this repo, so the created task file is left uncommitted for the user to review and commit (directly to `main` — see project memory).

## When not to use this skill

- The user wants to *work on* an existing task, not create a new one.
- A fully-specified task with complete detail is already provided — just create it directly.
- Tracking that doesn't belong in the backlog (throwaway reminders, chat-only TODOs).
