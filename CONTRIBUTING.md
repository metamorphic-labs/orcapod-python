# Contributing to Orcapod

This page is for a person changing Orcapod: how to set up, what a pull request must pass, and
which project rules a change is most likely to meet. Each rule is stated once here and
links to the page that has the detail.

The repository is public. Do not put anything private into a commit, a branch name, a pull
request, an issue or a comment: no credential, no internal host name or path, and no content
of a private repository.

## Set up

The project uses [uv](https://docs.astral.sh/uv/) and supports Python 3.11, 3.12 and 3.13.
After cloning, install the library with every extra and the development tools:

```bash
uv sync --all-extras --dev
```

Run every Python command through `uv run`, so that it uses this environment and the tool
versions pinned in `uv.lock`.

## The checks a pull request must pass

These four commands are the local form of what CI runs:

```bash
uv run --with ruff ruff check .
uv run --with ruff ruff format --check .
uv run --with basedpyright basedpyright
uv run pytest tests/
```

- **Ruff.** `uv run ruff format .` formats the code. Do not add a repository-wide or
  per-directory ignore to make a check pass; a `# noqa: <rule>` on one line needs a reason.
- **basedpyright.** Read its exit code, not its output: it prints many warnings, and only an
  error fails the check. The existing errors are recorded in `.basedpyright/baseline.json`.
  Fix a new error, and do not regenerate the baseline to hide it.
- **Tests.** The full suite takes a while; while you work, run the folder or file you
  changed, for example `uv run pytest tests/test_docs/`. CI runs the main, PostgreSQL and
  specification test suites in separate jobs with coverage. All three run on Python 3.13
  on pull requests and on Python 3.11, 3.12 and 3.13 on pushes to `main`. A failure specific
  to Python 3.11 or 3.12 is first seen on `main` for any of these suites. The main test job
  runs in parallel.

Two more commands were not run while writing this page:

- **Parallel tests.** CI runs the tests in parallel (pytest-xdist), and locally they run
  serially. `uv run pytest -n 4 tests/` runs them on four workers. Each worker holds its own
  copy of the suite's memory, so use it only on a machine with cores and memory to spare,
  and not beside another heavy process.
- **PostgreSQL integration tests.** They are skipped unless asked for:
  `uv run pytest -m postgres --postgres`. That command needs two things. Most of the tests
  need a running PostgreSQL server, reached through the standard `PG*` environment variables
  (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`);
  [the workflow that runs them](.github/workflows/run-postgres-tests.yml) shows a working
  setup. The `PostgresHashCacher` tests (`tests/test_hashing/test_postgres_hash_cacher.py`)
  start a PostgreSQL container of their own, so they need Docker.

Tests drive the library through its public interface: call an operator on a stream and read
the result through the stream's public methods.

## Changelog

A pull request does not edit `CHANGELOG.md`. If the change has something to tell users, add
one file, `changelog.d/<ISSUE-ID>.md`, with the text that would go under the release
heading. A change with nothing to tell users (tests, internal tooling) adds no file. Check
the fragments with:

```bash
uv run python scripts/assemble_changelog.py --check
```

The format and an example are in [changelog.d/README.md](changelog.d/README.md).

## Documentation

The documentation is the Markdown under `docs/` (outside `docs/dev/`) and `README.md`. A
test executes every `python` block of `README.md`, `docs/index.md`,
`docs/getting-started.md`, `docs/troubleshooting.md` and the pages in `docs/concepts/`,
`docs/reference/` and `docs/tutorial/`; blocks on other pages are not run. Every line a block
prints must appear in the block as a `# ...` comment. A block that needs infrastructure the
test run lacks (a PostgreSQL server, for example) carries `<!-- docs-test: skip -->` on the
line above its fence; the marker is not for hiding a wrong sample. If a sample fails because
the library changed, fix the page to match the code. Run the documentation tests with:

```bash
uv run pytest tests/test_docs/
```

- Docstrings are
  [Google style](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings),
  with no ReST markup. The API reference is generated from them.
- How to write a page is in
  [docs/dev/documentation-style.md](docs/dev/documentation-style.md).
- The published site is built from `docs/`, and its build fails on a broken internal link
  (a link to another page, file or anchor of the documentation). Links to other sites are
  not checked. How to build and preview it is in [site/README.md](site/README.md).

## Golden hashes

Orcapod's hashes name stored tables and find stored results again, so a hash value must stay
the same from one release to the next. The values are pinned in golden files under
`tests/test_hashing/golden/data/`, and the golden test computes each of them again on every
run. This command reports any difference without writing anything:

```bash
uv run python tests/test_hashing/golden/generate_golden_hashes.py --check
```

A failing golden entry means your change moved a hash. Treat that as a bug in the change.
Never edit or regenerate a golden file to make the test pass. Adding new golden values is
always allowed.

If the hash is meant to change, stop and raise it with the maintainers. What it takes
depends on the value:

- A value no release has frozen changes only with their approval of each entry that moves.
- A value frozen in a release does not change within its data context: the change needs a
  new data context version.
- The values of provisional operators, sources and library value types, and of everything
  computed downstream of them, are outside the freeze. They are pinned and checked like the
  others, and still change only with approval, but no release freezes them and a change
  needs no new data context.

The whole mechanism is described in
[docs/reference/golden-hashes.md](docs/reference/golden-hashes.md).

## Design documents

- The design specification is
  [docs/dev/orcapod-design.md](docs/dev/orcapod-design.md).
- Known design problems and bugs are logged in
  [docs/dev/design_issues/](docs/dev/design_issues/README.md), one file per entry. When you
  fix one, set its status to `resolved` and add a **Fix:** note. List the log and check its
  files with:

  ```bash
  uv run python scripts/design_issues.py
  uv run python scripts/design_issues.py --check
  ```

- Specs and plans written during development go under `docs/dev/specs/` and
  `docs/dev/plans/`. Nothing under `docs/dev/` is published on the site.

Orcapod is a greenfield project: do not add backward-compatibility shims, re-exports,
aliases or deprecation wrappers. Change the code and update every reference directly.

## Branches, commits and pull requests

- The maintainers track work in Linear, and a change belongs to an issue there (an id such
  as `ITL-123`). The branch name comes from the issue, and the pull request body names it:
  `Fixes ITL-123` for an issue the pull request resolves, a plain mention for a related
  one. If you have no access to Linear, open a GitHub issue first and describe the change.
- Create the branch from `main` and open the pull request against `main`.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/):
  `<type>(<optional scope>): <short description>`, for example
  `fix(data_function): reject variadic parameters at construction`. The usual types are
  `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `perf` and `ci`.

## Releases

Only the maintainers cut a release; the procedure is in [RELEASING.md](RELEASING.md).

## Coding agents

[CLAUDE.md](CLAUDE.md) is the full rule book, written for coding agents. A person does not
need to read it to contribute, but it is the place to look when a rule here seems incomplete.
