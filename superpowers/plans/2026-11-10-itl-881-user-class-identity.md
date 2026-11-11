# ITL-881 User Class Identity — Investigation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the developer a decision document on what makes two user-defined classes
(dataclass, pydantic model, and other supported kinds) "the same" for Orcapod, backed by
runnable experiments. No product code changes.

**Architecture:** One spec document
(`superpowers/specs/2026-11-10-user-class-identity.md`) and a folder of experiment scripts
(`superpowers/specs/user-class-identity-experiments/`). Each script drives Orcapod through its
public interface (`DictSource`, `@function_pod`, `PipelineJob`, `DeltaTableDatabase`,
`stream.as_table()`, `content_hash()`, `pipeline_hash()`), runs any cross-process scenario in
fresh subprocesses against a temporary directory, and prints a plain-text table. The spec
cites the script and the printed result for every claim.

**Tech Stack:** Python 3.12 via `uv run`, orcapod, pyarrow, pydantic, Ray (installed),
IPython (installed).

**Constraints:** Nothing under `src/` or `tests/` changes. Hash values are only observed,
never changed. The developer decides; the document recommends but does not decide.

---

## File structure

| File | Responsibility |
|---|---|
| `experiments/_harness.py` | Shared helpers: write throw-away packages into a temp dir, run a snippet in a fresh subprocess and return its JSON, collect an "identity report" for a class (schema, hashes, Arrow extension name and metadata, system-tag column names, node identity path, DB table paths). |
| `experiments/e01_where_identity_enters.py` | Same fields, different definition sites (`__main__`, module A, module B, other class name, other fields): which identity components differ. |
| `experiments/e02_notebook_redefine.py` | Same-kernel re-definition (cell re-run with identical fields, then changed fields) through a real IPython shell. |
| `experiments/e03_cross_process_cache.py` | Delta DB shared across processes: script `__main__` vs `python -m` vs import; two notebooks sharing a DB; class moved from notebook to module. Reports cache hits (function call counts) and read-back. |
| `experiments/e04_read_back.py` | Stored data read in a new process when the class is not importable, renamed, moved, or has changed fields. |
| `experiments/e05_locals_and_executors.py` | Class nested in a function; Ray executor and `multiprocessing` with a `__main__` class. |
| `experiments/e06_other_kinds.py` | Pydantic, Enum, NamedTuple, TypedDict, nested dataclass, `list[Point]`, `Point | None`. |
| `experiments/e07_registration_today.py` | What registration exists: implicit factory registration, `register_python_class`, a hand-built logical type with a custom name; which hashes follow the logical name and which follow the Python type name. |
| `experiments/e08_golden_impact.py` | Extract PR #284's golden package, recompute it with the fixture classes' identity changed, list the pinned entries that move. |
| `experiments/README.md` | How to run; what each script shows. |
| `specs/2026-11-10-user-class-identity.md` | Findings, scenarios, options (a)–(d)+, consequences, smallest safe workshop step, recommendation, decisions needed. |
| `DESIGN_ISSUES.md` | One appended entry for ITL-881 (pre-approved). |

(`experiments/` = `superpowers/specs/user-class-identity-experiments/`.)

## Tasks

### Task 1: Harness
- [ ] Write `_harness.py` with `run_snippet(code, cwd, extra_path) -> dict` (subprocess with
      `sys.executable`, prints one JSON line), `write_package(root, name, files)`, and
      `identity_report(cls, value)` returning the components listed above.
- [ ] Smoke-run `uv run python -c "import _harness"` from the experiments dir.

### Task 2: e01 where identity enters
- [ ] Each variant runs in its own subprocess so registries are clean.
- [ ] Print a table: component × variant, with "=" when equal to the `__main__` baseline.
- [ ] Run: `uv run python superpowers/specs/user-class-identity-experiments/e01_where_identity_enters.py`.
      Expected: module-qualified name appears in schema hash, data hash, pipeline hash,
      system-tag column names, node identity path, extension name.

### Task 3: e02 notebook re-definition
- [ ] Drive `IPython.core.interactiveshell.InteractiveShell` cells: define class, run job,
      re-run the definition cell, run job again; then change fields and run again.
- [ ] Record errors verbatim and function-call counts.

### Task 4: e03 cross-process caching
- [ ] Temp project with `pkg/models.py`, `pkg/run.py`, and a script `nb.py` defining the
      class in `__main__`. Shared `DeltaTableDatabase`. Count function calls per run.
- [ ] Scenarios: script twice; script then `python -m pkg.run`; `-m` then import-based run;
      two `__main__` processes with identical classes; two with different fields under the
      same name; class moved from notebook to module.

### Task 5: e04 read-back
- [ ] Store from `pkg.models.Point`; read in new processes with module missing, class
      renamed, moved, fields added / removed / retyped. Read through `job.nodes[...]`
      `as_table()`, `iter_data()`, `get_all_records()`.

### Task 6: e05 locals and executors
- [ ] Local class: record the error. Ray executor (`orcapod.core.executors.ray`) with a
      `__main__` class; `multiprocessing` spawn shows the `__mp_main__` module name.

### Task 7: e06 other kinds
- [ ] For each kind: does a source accept it, what Arrow type / extension name, does a pod
      accept it as input/output.

### Task 8: e07 registration today
- [ ] Show the implicit registration path and whether a user can set a stable name today
      (hand-built `DataclassLogicalType` with a custom `logical_name`), and which hashes
      follow it.

### Task 9: e08 golden impact
- [ ] `git show <PR-284 ref>:tests/test_hashing/golden/...` into a temp package; recompute
      with (i) unchanged, (ii) `__module__="__main__"`, (iii) module-less name; list moved keys.

### Task 10: Spec document, README, DESIGN_ISSUES entry
- [ ] Write the spec from the recorded outputs. Every claim cites a script or says
      "not verified". Tables over prose.
- [ ] Self-review for placeholders, contradictions, unverified claims.

### Task 11: Verify and ship
- [ ] Run every script once more; ruff check / format on the experiment folder.
- [ ] `uv run pytest tests/` (no code changed, but the rules require a green suite); note
      pre-existing failures and skips by name.
- [ ] Merge `origin/main`, push, open PR "Refs ITL-881", `orca linear attach`,
      `orca worktree set --pr`, worker_done.
