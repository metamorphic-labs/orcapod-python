# ITL-881 experiments: user class identity

Evidence for `superpowers/specs/2026-11-10-user-class-identity.md`. Every script drives
Orcapod through its public interface, runs each scenario in a fresh process inside a
temporary directory, and prints plain text. Nothing is written to the repository.

Run from the repository root:

```
uv run python superpowers/specs/user-class-identity-experiments/<script>.py
```

| Script | Shows | Runtime |
|---|---|---|
| `e01_where_identity_enters.py` | Which hashes, names and stored types change when the same class is defined in `__main__`, in another module, under another name, nested, or with other fields. Control: the function's own module does not enter the pod hash. | ~10 s |
| `e02_notebook_redefine.py` | Re-running a class-definition cell in a real IPython shell (identical fields, changed fields, pydantic, `importlib.reload`). | ~15 s |
| `e03_cross_process_cache.py` | Cache hits and misses over a shared Delta database: script vs `python -m` vs import, two notebooks, class moved to a module. | ~40 s |
| `e04_read_back.py` | Reading stored values in a new process after the class was deleted, renamed, moved or changed. | ~40 s |
| `e05_locals_and_executors.py` | A class defined inside a function; a `__main__` class through `RayExecutor`; `multiprocessing` spawn. | ~30 s |
| `e06_other_kinds.py` | Pydantic, nested dataclass, `list[Point]`, `Point \| None`, Enum, NamedTuple, TypedDict. | ~30 s |
| `e07_registration_today.py` | Implicit registration; a declared name built with internal API; reading data stored under a declared name. | ~20 s |
| `e08_golden_impact.py` | Which values pinned by PR #284 (golden hashes) move if a class's identity string changes. Needs `git fetch origin eywalker/itl-626-guard-hash-stability-with-golden-regression`. | ~30 s |

`_harness.py` holds the shared helpers. A child process reports one `JSON:` line and
exits with `os._exit(0)`: processes that read Delta tables sometimes hang at shutdown in
Arrow's thread-pool destructor (out of scope here, reported separately).

Recorded on macOS, Python 3.12.15, at `origin/main` `c438d4d1`. Hash values are
specific to that commit; the equal / different pattern is what the spec relies on.
