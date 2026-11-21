# Accepted security advisory exceptions

This is the one list of security advisories that a CI check is told to accept. Each row
is an exception to a check that would otherwise fail, such as the `Dependency review` job
in `.github/workflows/run-tests.yml` (`allow-ghsas`).

Rules (developer, November 17, 2026):

- An exception is added only with the developer's approval.
- Each exception has a Linear issue with a due date, named in its row and in the comment
  next to the setting that allows it.
- The list is reviewed at every release (`RELEASING.md`, pre-release checklist). An
  exception whose advisory has a published fix is removed: upgrade the package, remove
  the setting, delete the row.

| Advisory | Package and versions | Where allowed | Why accepted | Accepted | Tracking issue | How to check | Removal |
|---|---|---|---|---|---|---|---|
| [GHSA-fc5q-39xc-vjpf](https://github.com/advisories/GHSA-fc5q-39xc-vjpf) (high: ReDoS through single-branch repeated extglobs) | npm `picomatch` <= 4.0.7: 2.3.2 and 4.0.7 in `site/pnpm-lock.yaml`, through `astro` (`unstorage` > `anymatch`, `@astrojs/internal-helpers`, `vite`, `fdir`) | `allow-ghsas` of the `Dependency review` step, `.github/workflows/run-tests.yml` | Build-time only: the documentation site's toolchain matches the repository's own file globs; the published site is static HTML and the Python package does not contain it. No patched version exists (4.0.7 is the latest release) | 2026-11-17 | ITL-1207 (due 2026-12-15) | `gh api /advisories/GHSA-fc5q-39xc-vjpf -q '.vulnerabilities[].first_patched_version'` is no longer `null`; or `cd site && pnpm audit` | Update `site/pnpm-lock.yaml` to a fixed `picomatch` (a newer `astro`, or a pnpm override), remove the GHSA from `allow-ghsas`, delete this row |
