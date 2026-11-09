# Coordinating issue work with Orca

This runbook describes how one agent session acts as a **coordinator** that dispatches a
dedicated **worker** agent per Linear issue through Orca, supervises each worker to an
explicit outcome, and reports back to the developer.

It applies only when the developer asks an agent to coordinate, supervise, or dispatch issue
work. A session that is simply asked to fix one issue works directly, following `CLAUDE.md`.

Command syntax is owned by the installed Orca CLI, not by this file. Before running any Orca
command, load the version-matched guides:

```
orca skills get orca-cli
orca skills get orchestration
```

Everything below is this repository's operating policy. Where the policy and the CLI guide
disagree about what a command does, the CLI guide is right.

## Roles

| | Coordinator | Worker |
|---|---|---|
| Runs in | The main checkout (or its own coordinator workspace) | One Orca worktree per issue |
| Owns | Scope, sequencing, task specs, answering questions, relaying owner decisions, the final report | Plan, code, tests, commits, push, PR, review responses for its one issue |
| Edits code | Never | Only inside its own worktree and declared ownership boundary |
| Talks to the developer | Yes | No. Every question goes to the coordinator through `ask` |

The coordinator does not implement issue work, and does not substitute generic subagents
(the `Agent` tool, background tasks) for Orca workers. Read-only helpers for the
coordinator's own research are fine; issue ownership always goes through Orca so that it is
recorded.

## Before dispatching

1. **Read each issue.** Read the issue, its comments, relations, and any open PR or branch.
   Confirm it is still valid against `main` (a fix may already have landed).
2. **Open every wave with a decision discussion.** Before dispatching a wave, present the
   developer with the candidate issues and the key decision points for each: what would
   change, who is affected, the options, and a recommendation. Dispatch only what the
   developer selects. Proposing a wave is not approval to start it.
3. **Treat issues the developer did not file as suggestions.** An issue filed by anyone
   else, including one filed by a coordinator or worker from a finding, records a request
   or an observation, not a decision to do the work. The developer triages it in the wave
   discussion: accept, reshape, defer, or decline. Check the issue's creator before
   assuming its proposed fix or its priority is settled.
4. **Settle scope with the developer.** Agree which issues are in the wave and which
   decisions the developer has already made. An issue that needs a design decision is not
   ready to dispatch until the decision is made, or the spec says to stop and ask.
5. **Check for overlap.** Two workers must not edit the same file region. Where overlap is
   unavoidable (`DESIGN_ISSUES.md`, `CLAUDE.md`, a shared operator base class), name the
   overlap in both specs and say who owns the shared part. Sequence the issues instead if
   the overlap is substantial.
6. **Move the issues to In Progress** in Linear once the developer approves the wave.

## Dispatch

Bind one coordinator-owned Run, then start the whole independent wave before waiting:

```
orca status --json
orca orchestration run-create --objective "<objective>" --json
orca orchestration worker-start --spec "<task spec>" --worktree new-top-level --name <short-name> --agent <agent> --setup run --json
orca orchestration check --wait --types "worker_done,escalation,question" --timeout-ms 900000 --json
```

Rules:

- **One worktree per issue**, created from `origin/main`. The branch is the exact
  `gitBranchName` Linear returns for the issue. Never reuse another issue's branch or
  worktree, and never dispatch a worker into the main checkout.
- **One active editor per worktree.** Check ownership before reusing a terminal.
- **Save the receipt.** Record the actual Run, Task, Dispatch, worktree, and terminal IDs
  that `worker-start` returns. Never invent a handle or infer delivery from a visible pane.
- **A failed `worker-start` is not retried blindly.** Read the receipt's `failedStage` and
  `residualResources`, then load `references/recovery-and-cleanup.md`.
- Pass `--model` only when the developer named one.
- Use `task-create --deps` only for real ordering. Prefer parallel waves over long chains.

### Task spec

A worker sees nothing but its spec and the repository. Each spec is self-contained and
states:

- **Target.** The Linear issue (the worker reads it; its description is the scope of
  record), the branch name, and the base commit.
- **State.** What is already true that the issue text does not say: what landed on `main`
  since the issue was filed, what the coordinator already verified, related open PRs.
- **Change.** The concrete result to produce.
- **Ownership.** Files and modules the worker may edit, and the do-not-touch areas owned by
  parallel workers. Any change to a shared interface needs an `ask` first.
- **Constraints.** The standing rules below, plus anything specific to the issue.
- **Observable acceptance.** The tests, output, and evidence that must appear in
  `worker_done`.

### Standing rules for every worker

Copy these into every spec. They restate `CLAUDE.md` for a worker that cannot reach the
developer.

- **Decision routing.** Send every design or scope question to the coordinator with the
  orchestration `ask` command and wait. Never open a local question prompt. Wherever
  `CLAUDE.md` says "ask the user" or "wait for user approval" (adding a `DESIGN_ISSUES.md`
  entry, creating a Linear issue, approving a PR-review revision plan), that means `ask`.
  If the developer types into the worker's terminal directly, that instruction wins; tell
  the coordinator with an escalation.
- **Plan first.** Follow `CLAUDE.md` and the Sensei skill chain (`work-start`, then
  `brainstorming` or `writing-plans`). Save the plan under `superpowers/` before changing
  code. For anything beyond a contained bug fix, send the plan summary and open questions
  through `ask` and wait for the coordinator's approval before implementing. Flag any
  point that needs the developer's decision explicitly (see "Approving plans" below).
- **Tests.** Write the failing regression test first. Run everything through `uv run`.
  The full `uv run pytest tests/` suite must pass before a PR, and pre-existing failures
  are reported, not hidden.
- **Out-of-scope findings.** Report a newly found bug or design problem to the coordinator
  rather than fixing it. Do not widen the issue.
- **Finishing.** Rebase onto current `origin/main` and rerun the suite if `main` moved.
  Push the branch, open a PR against `main` with `Fixes <ISSUE-ID>` in the body, and link
  it with `orca linear attach` and `orca worktree set --worktree current --pr <n>`. Put the
  PR URL in `worker_done`.
- **Never** merge, force-push, cut a release, mark the issue Done, or use bare `git stash`.
- **Report honestly.** `worker_done` is sent exactly once, with `--outcome failed` when the
  work is not done. Its body states what was done, what was found, and what is left.

## Supervise

The coordinator stays in the `check --wait` loop until every expected Dispatch has settled.

- **Process every message in a Delivery before acknowledging it.** Reply to questions,
  inspect escalations, and validate each `worker_done` against the Dispatch it claims.
- **Approving plans.** The coordinator approves a worker's plan or spec itself when it is
  an obvious reading of the issue. It raises a plan to the developer only when it carries
  a critical decision:
  - a change to public API or to documented behavior;
  - anything that changes `content_hash()`, `pipeline_hash()`, record IDs, or stored
    schemas, and so invalidates existing caches or databases;
  - a new dependency, or removal of a feature someone uses;
  - scope that grows beyond the issue, or a choice between materially different designs;
  - anything touching release, CI secrets, or data outside the repository.

  When raising one, state the decision, the options, and a recommendation, so the
  developer can answer in one line. Everything else keeps moving.
- **Relay owner decisions.** Answer a worker's `ask` directly when the answer follows from
  the issue, `CLAUDE.md`, or a decision the developer already made. Put critical decisions
  to the developer and relay the answer. Do not guess on the developer's behalf.
- **A timeout or empty wait is a checkpoint, not a failure.** After three consecutive empty
  waits, enumerate with `orca orchestration worker-list --json` and act on each row's
  stated next action.
- **Absence is not proof.** Contact loss, a stale handle, or a missing row does not show
  that a worker exited. Never start a second editor in a worktree, and never stop, abandon,
  or retry a worker, without positive proof of exit. Report the state as unverifiable
  instead.
- **A successful launch is not a successful task.** Neither is a heartbeat.

## Accept and settle

For each `worker_done`:

1. **Verify the claim.** Check that the PR exists, targets `main`, is linked to the issue,
   and that CI is green. An empty checks list is not passing CI. Read the diff against the
   spec's ownership boundary and acceptance list. Independent read-only review of the PR is
   encouraged.
2. **Route follow-up work back to a worker.** If review finds problems, dispatch a new Task
   to the same worker terminal; the coordinator does not patch the branch itself. PR review
   responses follow the `CLAUDE.md` rule: the worker drafts the revision plan, the
   coordinator gets the developer's approval, then the worker fixes and replies.
3. **Decide the terminal's next owner** before acknowledging the Delivery: reuse it for a
   follow-up Dispatch, retain it with `worker-retain` when the developer asked for that, or
   release it with `worker-release`. Release is cleanup after an accepted settlement, never
   a way to cancel.
4. **Leave merging to the developer.** The coordinator does not merge PRs, cut releases, or
   mark issues Done. Preserve the worktree until the PR is merged.

The coordinator's turn ends only when
`orca orchestration worker-list --run <run_id> --terminal-state reclaimable --json` returns
nothing and the developer has a report that names, per issue: the outcome, the PR, the
evidence behind it, and any unresolved blocker or follow-up issue.

## If the session restarts

Resume the same Run with `orca orchestration run-use --id <run_id>` and re-read the inbox.
Reconstruct ownership from Orca's records, not from memory or an old prompt.
