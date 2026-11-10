# Releasing Orcapod

This document describes how to cut a release of `orcapod`.

## Publish target — PyPI is superseded

> **OPEN ITEM — Publish target: pending the internal registry. Releasing is BLOCKED until
> the organization's internal Python package registry is available.** Its URL,
> authentication and install command will be filled in once the registry exists.

As of 2026-11-09, Orcapod is to be published to the organization's internal Python package
registry, not to public PyPI, until public development resumes in January 2027. The
Release workflow (`.github/workflows/release.yml`) still publishes to TestPyPI and PyPI;
switching it to the internal registry is not yet possible. Every step below that publishes to, checks or installs from
PyPI or TestPyPI is marked **(superseded)**; do not rely on it. These steps are rewritten
in one pass once the registry details are known.

> **Stop: do not trigger the Release workflow** until `release.yml` publishes to the
> internal registry. Its `publish-testpypi` and `publish-pypi` jobs are still active, so a
> run that gets past `build` publishes the package to TestPyPI and public PyPI, and the
> pushed tag and published version cannot be taken back.

## Branching Model

- All development happens on feature branches off `main`.
- There are no long-lived branches (the historical `dev` branch has been retired).
- No back-merges are needed.

## Before You Cut

Do these on `main` before triggering the workflow:

1. **Update `CHANGELOG.md`** — rename the top section to the version being released, set
   its date, and make sure breaking changes carry migration notes. The GitHub Release
   body is generated from PR titles; `CHANGELOG.md` is the curated record.
2. **Check that `main` is green** — the `Run Tests` workflow on the commit you are
   releasing. The Release workflow re-runs the unit tests on Python 3.11, 3.12 and 3.13
   and the license check, but not the PostgreSQL or SpiralDB integration jobs.
3. **Check the version is free** — the tag must not exist on origin. **(superseded)** The
   version must not exist on PyPI or TestPyPI; neither index allows a version to be
   uploaded twice. The equivalent check against the internal registry is part of the open
   item above.

Per-release checklists live in `superpowers/plans/` (for example
`2026-11-09-v0.2-release-checklist.md`).

## Cutting a Release

Releases are triggered from the GitHub Actions UI — no manual `git tag` step required.

1. **Merge your branch into `main`** — open a PR, get it reviewed, merge it.

2. **Trigger the release workflow** — **blocked: see "Stop" under *Publish target*
   above.** Once the workflow publishes to the internal registry, go to
   **[Actions → Release → Run workflow](https://github.com/metamorphic-labs/orcapod-python/actions/workflows/release.yml)**
   in the GitHub UI, enter the version (e.g. `0.1.0`), optionally enter a branch name
   (defaults to `main`), and click **Run workflow**.

   A leading `v` is stripped automatically — `v0.1.0` and `0.1.0` both work.

3. **CI takes over** — the workflow runs the following jobs automatically:

   ```
   validate-branch ─┬─ test ──────────┐
                    └─ license-check ──┤─ build ─┬─ publish-testpypi ─ publish-pypi ─┐
                                                 └─ linear-sync ──────────────────────┴─ linear-complete
   ```

   - Pre-flight: branch validation, then tests on Python 3.11, 3.12 and 3.13 and license check (parallel after validation)
   - Build: normalises version, creates local tag, builds wheel + sdist, pushes tag to origin
   - TestPyPI **(superseded)**: publishes to test.pypi.org first as a staging step
   - PyPI **(superseded)**: publishes to pypi.org and creates a GitHub Release with generated release notes
   - Linear: `linear-sync` starts immediately after build (parallel to publish); `linear-complete` runs after both PyPI publish and linear-sync succeed

## Pre-releases

Release candidates (e.g. `0.1.0rc1`) follow exactly the same path — enter the version in the
workflow input. **(superseded)** PyPI handles the stable vs pre-release distinction
natively:

- `pip install orcapod` — installs the latest **stable** release only
- `pip install --pre orcapod` — installs the latest release including pre-releases

## Cutting a Hotfix Release

To release from a branch other than `main` — for example, a maintenance branch carrying
a critical patch — enter the branch name in the **Branch** field when triggering the
workflow.

**Example:** cutting `v0.1.1` from `hotfix/0.1.x`:

1. **Trigger the release workflow** — go to
   **[Actions → Release → Run workflow](https://github.com/metamorphic-labs/orcapod-python/actions/workflows/release.yml)**
2. In the **Use workflow from** dropdown, keep `main` selected (so the workflow version
   includes the `Branch` field — do not select `hotfix/0.1.x` here)
3. Set **Version** to `0.1.1`
4. Set **Branch** to `hotfix/0.1.x`
5. Click **Run workflow**

The workflow will confirm the branch exists, run tests against it, create the `v0.1.1`
tag at its tip, and publish **(superseded: PyPI)** — identical to a normal release.

## If a Run Fails Part-Way

The `build` job pushes the `vX.Y.Z` tag to origin *before* anything is published, so a
failure in a later job leaves the tag behind.

| Failed job | State left behind | Recovery |
|-----------|-------------------|----------|
| `test` or `license-check` | Nothing | Fix on `main`, trigger a new run |
| `build`, before "Push release tag" | Nothing | Fix, trigger a new run |
| `build`, after "Push release tag" | Tag on origin, no artifact | Delete the tag (`git push origin :refs/tags/vX.Y.Z`), then trigger a new run |
| `publish-testpypi` **(superseded)** | Tag on origin | Use **Re-run failed jobs** on the same run — do not start a new run, which would fail creating a tag that already exists |
| `publish-pypi` **(superseded)** | Tag on origin, version on TestPyPI | Use **Re-run failed jobs**. If the upload to PyPI itself partly succeeded, the version is burned: release the next patch version instead |
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

When a release is triggered, the CI automatically transitions all Linear issues in
**"Merged"** status (whose PRs were included in the release) to **"Done"**. This is
handled by two jobs in `release.yml` that call `linear/linear-release-action`, plus a
separate `release-sync.yml` that runs on every push to `main`.

### How it works

| Trigger | Workflow / Job | Action | Effect |
|---------|---------------|--------|--------|
| Push to `main` | `release-sync.yml / sync` | `sync` (no version) | Associates the merged PR with the open Linear release draft |
| Release workflow `build` succeeds | `release.yml / linear-sync` | `sync --version <tag>` | Finalises the commit set for this release version in Linear |
| After `publish-pypi` succeeds | `release.yml / linear-complete` | `complete --version <tag>` | Marks the release done in Linear; triggers Merged → Done |

### Prerequisites

Before cutting the first release with this system active, a workspace admin must:

1. **Set the `LINEAR_ACCESS_KEY` repo secret** in `metamorphic-labs/orcapod-python` GitHub
   settings (`Settings → Secrets and variables → Actions → New repository secret`).
   Use the same Linear API key as `metamorphic-brain/axon`.

2. **Configure a Linear release** in the workspace for orcapod-python via the Linear UI:
   set the release name, which statuses count as "included" (at minimum: Merged), and
   the target state for the transition (Done).

3. **(superseded)** **Update PyPI/TestPyPI Trusted Publisher configs** — if the Trusted Publisher on
   pypi.org or test.pypi.org still references `publish.yml` as the workflow file, update
   it to `release.yml` before triggering the first release.

If `LINEAR_ACCESS_KEY` is not set: the `release-sync` workflow runs but the Linear action
may fail; the `linear-sync` and `linear-complete` jobs in `release.yml` fail but do not
block publishing (they have no dependents in the publish chain).

### Replicating for other orcapod repos

1. Copy `.github/workflows/release-sync.yml` verbatim.
2. Add `linear-sync` and `linear-complete` jobs to the repo's release workflow,
   adjusting `needs:` in `linear-complete` to point at whichever job creates the GitHub
   Release.
3. Set the `LINEAR_ACCESS_KEY` repo secret.
4. Configure a Linear release in the workspace for the new repo.
