# Releasing Orcapod

This document describes how to cut a release of `orcapod`.

## Publish target and where things run

Orcapod is published on public PyPI as [`orcapod`](https://pypi.org/project/orcapod/), under
the MIT License (developer decision, 2026-11-10; the license is applied from `0.2.0`). Users
install it with `pip install orcapod` or `uv add orcapod`. `0.2.0` was cut as a tag only
(developer decision, 2026-11-10).

One repository does everything (developer decision, 2026-11-16, ITL-1193):
`metamorphic-labs/orcapod-python`, a public repository, holds development (branches, pull
requests, review, CI, the Linear integration, the design log) and cuts the releases.

The Release workflow (`.github/workflows/release.yml`) is the only publishing route. The
developer dispatches it by hand on `metamorphic-labs/orcapod-python`, from `main`. It uploads
to TestPyPI first and then to PyPI, and authenticates to both through OIDC trusted publishing:
the workflow uses no API token. That requires, on each index, a trusted publisher for the
`orcapod` project naming owner `metamorphic-labs`, repository `orcapod-python`, workflow
`release.yml` and environment `testpypi` (TestPyPI) or `pypi` (PyPI); see *One-time setup*
below. An agent never runs the Release workflow, creates or pushes a tag, or changes a
setting, secret or environment.

**History.** Up to and including `0.3.0` (2026-11-17), releases were cut from
`nauticalab/orcapod-python`: `main` was pushed there by fast-forward at a release point, the
Release workflow ran there, and the tag was fetched back into `metamorphic-labs`
(ITL-1007). Every release tag up to `v0.3.0` is also here, on the same commit; `v0.2.0` (a
tag only, never published) exists only here and stays as it is.

## Branching Model

- All development happens on feature branches off `main`.
- There are no long-lived branches (the historical `dev` branch has been retired).
- No back-merges are needed.

## Before You Cut

Do these on `main` before the Release workflow runs:

1. **Update `CHANGELOG.md`** in a release pull request, the only kind of pull request that
   edits it. Feature pull requests add `changelog.d/<ISSUE-ID>.md` fragments instead (see
   `changelog.d/README.md`).
   1. Fold the fragments into the section of the version being released:

      ```
      uv run python scripts/assemble_changelog.py --version X.Y.Z
      ```

      This appends each fragment's text to the matching category of `## [X.Y.Z]` (in order
      of issue id, without moving the lines already there), creates the section or a
      category if it is missing, and deletes the fragments. It stops without changing
      anything if a fragment is malformed; `--check` reports the same problems without
      folding. Commit the changed `CHANGELOG.md` and the deleted fragments together;
      `changelog.d/README.md` stays.
   2. Set the section's heading to the version being released and its date
      (`## [X.Y.Z] — YYYY-MM-DD`), and make sure breaking changes carry migration notes.

   The GitHub Release body is generated from PR titles; `CHANGELOG.md` is the curated
   record.
2. **Record the release in the golden history**, in the same release pull request, as its
   last change (see *Golden hash values* below):

   ```
   TZ=UTC uv run python tests/test_hashing/golden/generate_golden_hashes.py --release X.Y.Z
   ```

   Release dates are UTC days: the release revision records the date of the local clock,
   so run the script with `TZ=UTC` and check that the date it records equals the date in
   the `CHANGELOG.md` heading. It appends the release revision to the `golden` history of
   the current data context's manifest, writes its digest under `releases` in
   `src/orcapod/contexts/released_manifests.json`, and at the data context's first release
   sets `released_in` and the `manifest` pin. It computes no hash and refuses a version
   already released. Commit both files with the release pull request; the release is tagged
   on a commit that contains them. The Release workflow's build job checks this before it
   creates the tag (`scripts/check_golden_tags.py --release-version X.Y.Z`) and stops if the
   release is not recorded, or if a golden file was added after the release step (the
   release revision must be the last revision when the commit is tagged). It also runs the
   tag check (`scripts/check_golden_tags.py`) there, so a branch that changed what an earlier
   release of the data context shipped is not released. Tags of earlier data contexts are
   not checked, so the first release of a data context passes with nothing to check.
3. **Check that `main` is green** — the `Run Tests` workflow on the commit you are
   releasing. The Release workflow re-runs the unit tests on Python 3.11, 3.12 and 3.13
   and the license check, but not the PostgreSQL or SpiralDB integration jobs.
4. **Check the version is free** — the tag must not exist on `metamorphic-labs`, and the
   version must not exist on PyPI or TestPyPI; neither index allows a version to be uploaded twice, even
   after it is deleted.

Per-release checklists live in `docs/dev/plans/` (for example
`2026-11-09-v0.2-release-checklist.md`, `2026-11-15-v0.3-release-checklist.md`).

## Pre-release checklist

All of these hold before the developer runs the Release workflow:

1. **The full suite is green**: `uv run pytest tests/` passes on the commit to be released.
2. **The release pull request is merged** (`CHANGELOG.md` folded, section dated,
   `changelog.d/` fragments deleted).
3. **The golden release step is done**: `contexts/released_manifests.json` has the current
   data context's `releases` line for this version and a pinned `manifest`, and the golden
   hash tests and the data context tests pass unchanged.
4. **`main` is green**, and `git status` is clean.
5. **The version is free**: the tag does not exist on `metamorphic-labs`, and the version is
   on neither PyPI nor TestPyPI.
6. **The Linear release is named** (see *Linear Release System*): the open release of the
   Linear release pipeline has the version `vX.Y.Z`. Not for a hotfix run, which leaves the open
   release as it is.

## Golden hash values

Developer decisions, November 15, 2026 (ITL-946); the mechanism and what each check protects
against are in `docs/reference/golden-hashes.md`.

- A golden file that a release listed never changes, and is never removed or renamed.
- New golden values can be added in any release, a patch included.
- Any change to a released golden value, a logical type's own identifier change included,
  needs a new data context version. A new data context version ships only in a minor release
  (before 1.0) or a major one, never in a patch.
- When a new data context starts, the old context's golden files and code move to
  `orcapod-sanctuary`; this repository carries only the current context's golden folder.
- Release tags are protected on `metamorphic-labs/orcapod-python` (and on
  `nauticalab/orcapod-python`, the release repository up to `0.3.0`): the repository ruleset
  `protect-release-tags` (target `refs/tags/v*`; rules deletion, update and non-fast-forward;
  no bypass actors) is in place since 2026-11-15. A `v*` tag can be created, as the Release workflow does, but never moved
  or deleted. This is a precondition of the CI tag check (`scripts/check_golden_tags.py`),
  which compares HEAD with every release tag of the current data context. Agents never change
  repository settings.

## Cutting a Release

Releases are triggered from the GitHub Actions UI — no manual `git tag` step required.
**Do not create the release tag by hand:** the `build` job aborts when the tag already exists
on origin, and the ruleset `protect-release-tags` forbids deleting a `v*` tag, so a hand-made
`vX.Y.Z` tag means that version can no longer be published by the workflow.

1. **Run Release** — go to
   **[Actions → Release → Run workflow](https://github.com/metamorphic-labs/orcapod-python/actions/workflows/release.yml)**
   in the GitHub UI, keep **Use workflow from** on `main`, enter the version (e.g. `0.4.0`),
   optionally enter a branch name (defaults to `main`), and click **Run workflow**. A leading
   `v` is stripped automatically. Leave the `skip_tests` checkbox (labelled *Skip the test
   jobs …*) unchecked; it is an exception, described in *Skipping the release tests* below.
   The developer does this; no agent runs the workflow.

2. **Approve the PyPI deployment** — `publish-pypi` runs in the `pypi` environment, which
   requires a reviewer: approve it once `publish-testpypi` has succeeded.

3. **CI takes over** — the workflow runs the following jobs:

   ```
   validate-branch ─┬─ test ──────────┐
                    └─ license-check ──┤─ build ─┬─ publish-testpypi ─ publish-pypi ─┐
                                                 └─ linear-sync ──────────────────────┴─ linear-complete
   ```

   - Pre-flight: branch validation, then tests on Python 3.11, 3.12 and 3.13 and license check (parallel after validation); the tests are skipped only with `skip_tests`
   - Build: normalises version (strips a leading `v`), checks the golden release step and the release tags, creates the local tag, builds wheel + sdist with `uv build`, aborts if the tag already exists on origin, pushes the tag, uploads `dist/` as an artifact
   - TestPyPI: in the `testpypi` environment, `uv publish --publish-url https://test.pypi.org/legacy/ dist/*` with an OIDC token (`id-token: write`) — a staging upload
   - PyPI: in the `pypi` environment, after TestPyPI succeeds and a reviewer approves, `uv publish dist/*` with an OIDC token, then creates the GitHub Release for the tag with generated release notes and the dist files attached
   - Linear: `linear-sync` starts immediately after build (parallel to publish); `linear-complete` runs after both PyPI publish and linear-sync succeed (see *Linear Release System*)

The tag is created by the workflow in this repository; there is nothing to fetch back.

## Skipping the release tests

The Release workflow has a boolean input, `skip_tests` (a checkbox in the Run workflow form,
labelled *Skip the test jobs …*, default unchecked; developer, November 16, 2026, ITL-1188).
Checked, the `test` jobs (Python 3.11, 3.12 and 3.13) do not run; everything else does: branch validation,
the license check, the build with its golden checks, the tag, TestPyPI, PyPI and the GitHub
Release. Unchecked, the workflow is exactly what it was before the input existed.

This is an exception, for a pinch: the release tests take a long time and repeat what
`Run Tests` already ran on the same commit. Use it only when all of these hold:

- The exact commit being released (the tip of the **Branch** input) has green CI: the
  `Run Tests` workflow on that commit passed on Python 3.11, 3.12 and 3.13. Check the commit
  id, not the branch name.
- The pre-release checklist above holds, the full suite included.
- The developer chose it for this run. An agent never runs the Release workflow, with or
  without the option.

What the workflow guarantees:

- Only the input skips the tests. A test job that failed or was cancelled still stops the
  build and everything after it, as do a failed validation, license check or build.
- A run with the tests skipped says so: the build job writes a `Tests skipped` warning
  annotation and a run summary section naming who started the run (`github.actor`).

Every job after the tests carries its own condition. A GitHub job whose need was skipped is
itself skipped, and so is every job after it, unless its `if` names a status function; so
`build`, `publish-testpypi`, `publish-pypi`, `linear-sync` and `linear-complete` each check
the results of the jobs they need (`!cancelled() && needs.<job>.result == 'success'`), and
`build` accepts a skipped `test` only when `skip_tests` is set. A new job after the tests
needs the same.

## One-time setup (only the developer can do this)

On `metamorphic-labs/orcapod-python` and the package indexes; no agent changes settings,
secrets or environments:

1. The environments `testpypi` and `pypi` (`Settings → Environments`). `pypi` requires a
   reviewer. Give both a deployment branch policy that allows `main` only, so that no workflow
   on another branch can request a publishing token. A hotfix release still passes it: the
   workflow runs from `main` (**Use workflow from**) and only checks out the hotfix branch.
2. On pypi.org and test.pypi.org, an owner of the `orcapod` project opens
   *Manage → Publishing* and adds a GitHub trusted publisher with owner `metamorphic-labs`,
   repository `orcapod-python`, workflow `release.yml` and environment `pypi` (PyPI) or
   `testpypi` (TestPyPI). An upload from a repository that no trusted publisher names is
   rejected, which fails `publish-testpypi` after the tag has been pushed. Registered by the
   developer on 2026-11-16. Remove the entries that name `nauticalab` once
   `nauticalab/orcapod-python` is retired.
3. `main` is protected by organization rulesets: today their rules are pull requests only,
   no force push (non-fast-forward) and no deletion. The rulesets for public repositories
   apply once the repository is public; check that they keep these three rules.
4. Release tags are protected with the ruleset `protect-release-tags` (target
   `refs/tags/v*`; deletion, update, non-fast-forward; no bypass actors), so the golden tag
   check has a fixed anchor (ITL-946). Done on 2026-11-15.
5. The `LINEAR_ACCESS_KEY` repository secret, for the Linear jobs (see *Linear Release
   System*), and the organization secret `CODECOV_TOKEN`, for the coverage uploads.
6. Before the first release cut here: the latest GitHub Release in this repository is
   `v0.1.2` (the GitHub Release of `v0.3.0` is on `nauticalab`), and the generated
   release notes start at the latest earlier Release. Create a GitHub Release for `v0.3.0`
   here (its notes copied from `nauticalab`) so that the next notes start at `v0.3.0`, or edit
   the next Release's notes after the run.

### SpiralDB integration job

The `spiral-integration` job of `Run Tests` runs on every push to `main` and by hand
(`workflow_dispatch`) on `metamorphic-labs/orcapod-python`, never on pull requests. It runs
beside the test jobs, not after them, so its result comes in minutes. It authenticates to SpiralDB with a workload private key (ITL-1185): pyspiral reads
`SPIRAL_CLIENT_ID` (the workload id) and `SPIRAL_PRIVATE_KEY` (the private key as a JSON Web
Key) from the environment, signs a client assertion with the key, and requests a token from
`https://auth.spiraldb.com/oauth/token` with the `client_credentials` grant. It requests no
GitHub OIDC token: pyspiral's workload identity path posts a token exchange that the server
rejects. Client secrets are not used: the server no longer issues them. The job fails at its
first step, naming each missing setting, when one of the three below is not set or the key
is not a private JWK naming its `kid`. No agent creates, reads or sets any of them.

On `metamorphic-labs/orcapod-python` and in SpiralDB, the developer sets:

1. The SpiralDB workload the job runs as (today `orcapod-gha`) holds the **editor** role on
   the test project ("Orcapod testing"): the tests create, write, read and drop tables there.
2. A key pair registered for that workload. pyspiral's own CLI has no command for this; use
   Studio or the newer SpiralDB CLI, in an environment of its own and never installed beside
   pyspiral (both provide a top-level `spiral` module):
   `spiral workloads keys add <workload id> --kid <key id> --output -` prints the private key
   as a JWK that names its `kid`. A PEM key is not accepted.
3. Repository secrets (`Settings → Secrets and variables → Actions → Secrets`):
   `SPIRAL_CLIENT_ID`, the workload id (`work_...`), and `SPIRAL_PRIVATE_KEY`, the private JWK
   as one line of JSON. Keep the key file out of the repository and delete it once the secret
   is set.
4. Repository variable (`… → Variables`): `SPIRAL_PROJECT_ID`, the id of the test project.

The server URL is fixed in the workflow (`https://api.spiraldb.dev`); the repository variable
`SPIRAL_SERVER_URL` and the secret `SPIRAL_WORKLOAD_ID` are not read and can be deleted.

To rotate the key: add a new key to the workload with a new `kid`, replace the
`SPIRAL_PRIVATE_KEY` secret with the new JWK, run `Run Tests` by hand on `main` and check that
`spiral-integration` passes, then remove the old key (`spiral workloads keys rm`). A workload
can hold several keys, so the job keeps working during the switch.

If the key may have been exposed (for example printed in a log), remove it from the workload
first (`spiral workloads keys rm`), so it stops signing in at once, then add a new key and
replace the secret as above. The job fails in between; that is the price of containment.

## Pre-releases

Release candidates (e.g. `0.1.0rc1`) follow exactly the same path — enter the version in the
workflow input. PyPI handles the stable vs pre-release distinction natively:

- `pip install orcapod` — installs the latest **stable** release only
- `pip install --pre orcapod` — installs the latest release including pre-releases

## Cutting a Hotfix Release

To release from a branch other than `main` — for example, a maintenance branch carrying
a critical patch — enter the branch name in the **Branch** field when triggering the
workflow.

**Example:** cutting `v0.1.1` from `hotfix/0.1.x`:

0. **Make sure `hotfix/0.1.x` points at the commit to release.** The workflow checks that the
   **Branch** input exists in this repository and tags its tip.
1. **Trigger the release workflow** — go to
   **[Actions → Release → Run workflow](https://github.com/metamorphic-labs/orcapod-python/actions/workflows/release.yml)**
2. In the **Use workflow from** dropdown, keep `main` selected (so the workflow version
   includes the `Branch` field — do not select `hotfix/0.1.x` here)
3. Set **Version** to `0.1.1`
4. Set **Branch** to `hotfix/0.1.x`
5. Click **Run workflow**

The workflow will confirm the branch exists, run tests against it (unless `skip_tests` is
set; see *Skipping the release tests*), create the `v0.1.1` tag at its tip, and publish to TestPyPI and PyPI — identical to a normal release.

## If a Run Fails Part-Way

The `build` job pushes the `vX.Y.Z` tag to origin *before* anything is published, so a
failure in a later job leaves the tag behind.

| Failed job | State left behind | Recovery |
|-----------|-------------------|----------|
| `test` or `license-check` | Nothing | Fix on `main`, trigger a new run |
| `build`, before "Push release tag" | Nothing | Fix, trigger a new run |
| `build`, after "Push release tag" | Tag on origin, no artifact | The tag cannot be deleted (`protect-release-tags`), and a new run or a re-run stops at the duplicate-tag guard: release the next patch version |
| `publish-testpypi` | Tag on origin | Use **Re-run failed jobs** on the same run — do not start a new run, which would fail creating a tag that already exists |
| `publish-pypi` | Tag on origin, version on TestPyPI | Use **Re-run failed jobs**. If the upload to PyPI itself partly succeeded, the version is burned: release the next patch version instead |
| `linear-sync` | Unknown: it needs only `build` and runs in parallel with publishing, so it can fail before anything is published | Check `publish-testpypi` and `publish-pypi` first and recover them by their rows above. Then re-run `linear-sync`; it does not affect publishing |
| `linear-complete` | Release is published (it runs only after `publish-pypi` and `linear-sync` succeed) | Re-run the failed job; it does not affect publishing |

Never delete a tag whose version has been published to any package index.

## Release Lineage

`v0.1.3` and `v0.1.4` were cut from a maintenance branch and are **not** ancestors of
`main`; `git describe` on `main` therefore reports a distance from `v0.1.2`. This does not
affect a release from `main`, because the workflow creates the release tag on the commit
being built and `hatch-vcs` reads that tag. The fix those two releases carried is on
`main`. The commit tagged `v0.1.4` is reachable only through its tag.

## Tag Format

| Release type | Tag format | Example |
|-------------|------------|---------|
| Stable | `vMAJOR.MINOR.PATCH` | `v0.1.0` |
| Release candidate | `vMAJOR.MINOR.PATCHrcN` | `v0.1.0rc1` |
| Alpha | `vMAJOR.MINOR.PATCHaN` | `v0.1.0a1` |
| Beta | `vMAJOR.MINOR.PATCHbN` | `v0.1.0b1` |

The workflow input accepts any of the above (with or without a leading `v`). The version is
derived from the git tag by `hatch-vcs` (`dynamic = ["version"]` in `pyproject.toml`) — no
manual version bump is needed.

## Linear Release System

Three jobs call `linear/linear-release-action` (pinned; it runs the Linear Release CLI) with
the `LINEAR_ACCESS_KEY` repository secret, the access key of one Linear release pipeline. Each
runs only when `github.repository == 'metamorphic-labs/orcapod-python'`, so a fork's run skips
it, and none of them can block or undo a publish: no publish job depends on them. Up to
`0.3.0` the two release jobs never ran (the Release workflow ran on `nauticalab`, where they are
skipped) and the Linear release was completed by hand. From the first release cut here they
run.

| Trigger | Workflow / Job | Command | What it does |
|---------|---------------|---------|--------------|
| Push to `main` | `release-sync.yml / sync` | `sync` (no version) | Scans the commits since the last synced commit for Linear issue ids and pull request numbers, and adds them to the pipeline's open release (today one with no version set) |
| Release workflow `build` succeeds | `release.yml / linear-sync` | `sync --version vX.Y.Z` | Targets the release whose version is `vX.Y.Z`, or **creates** one, and adds the commits since the last synced commit |
| After `publish-pypi` succeeds | `release.yml / linear-complete` | `complete --version vX.Y.Z` | Completes the release whose version is `vX.Y.Z` |

The workflow only syncs and completes the Linear release; any change of issue status that
comes with a completed release is made by Linear, as the workspace configures it.

**Before the first run, and every run after it: name the open release.** Every push to `main`
adds its pull request to the open release, which has no version, and moves the scan start to
that commit. A release of `main`'s tip therefore finds no new commit: without a release named
`vX.Y.Z`, `linear-sync` creates an empty `vX.Y.Z` release (or none, if the CLI creates nothing
for an empty range), the open release holding the merged pull requests is left as it is, and
`linear-complete` completes the empty release or fails to find one. So, before running the
Release workflow, the developer sets the version of the pipeline's open release to `vX.Y.Z` in
Linear (pre-release checklist, item 6); `linear-sync` then targets it and `linear-complete`
completes it. If a Linear job fails anyway, the release is published; finish the Linear release
by hand (*If a Run Fails Part-Way*).

**A hotfix run is different: never rename the open release for it.** The open release holds
`main`'s unreleased pull requests; naming it after the hotfix would mark them shipped in the
hotfix. The Linear jobs check out the workflow's ref (`main`), not the **Branch** input, so they
never scan the hotfix branch: on a hotfix run `linear-sync` creates an empty `vX.Y.Z` release (or
none) and `linear-complete` completes it or fails, neither of which affects publishing. Attach
the hotfix's issues to that release by hand in Linear if it should list them. Syncing the hotfix
commits themselves would need the Linear jobs to check out the **Branch** input, a change to
`release.yml`.

Setup: the `LINEAR_ACCESS_KEY` repository secret, holding the access key of the pipeline meant
for this repository's releases.
