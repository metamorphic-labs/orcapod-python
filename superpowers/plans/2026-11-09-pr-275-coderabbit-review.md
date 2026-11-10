# PR #275 — CodeRabbit review log

PR: https://github.com/metamorphic-labs/orcapod-python/pull/275 (ITL-825)

Every CodeRabbit finding is verified against the code before acting. Verdicts:
FIX (correct), PARTIAL (right problem, different remedy), DISMISS (wrong, out of
scope, or contradicts a project rule).

## Findings

| Round | Finding | Location | Verdict | Reason | Commit / reply |
|---|---|---|---|---|---|

## Coordinator items (not CodeRabbit)

| Item | Verdict | Notes | Commit |
|---|---|---|---|
| README line 14 still says Python 3.11 and 3.12 | Deferred | PR #269 owns README version text; not edited here. | — |
| 72 new basedpyright warnings in files this PR touches (org typecheck fails on warnings) | FIX | Typing-only changes; canonical strings and hashes byte-identical (438-value probe on 3.11/3.12/3.13). See counts below. | (this commit) |

### basedpyright per-file counts (errors + warnings)

New files must be 0; modified files must not exceed `main`.

| File | main | before | after |
|---|---|---|---|
| src/orcapod/utils/canonical_type_names.py (new) | — | 10 | 0 |
| tests/test_hashing/test_canonical_type_names.py (new) | — | 41 | 0 |
| src/orcapod/core/data_function.py | 183 | 183 | 181 |
| src/orcapod/core/data_function_proxy.py | 79 | 79 | 79 |
| src/orcapod/core/sources/list_source.py | 49 | 53 | 49 |
| src/orcapod/hashing/hash_utils.py | 49 | 53 | 20 |
| src/orcapod/hashing/semantic_hashing/builtin_handlers.py | 131 | 131 | 131 |
| src/orcapod/hashing/semantic_hashing/semantic_hasher.py | 78 | 78 | 76 |
| src/orcapod/utils/function_info.py | 48 | 50 | 33 |
| tests/test_core/data_function/test_data_function_proxy.py | 21 | 26 | 21 |
| tests/test_core/sources/test_sources_comprehensive.py | 163 | 165 | 163 |
| tests/test_hashing/test_hash_utils.py | 187 | 187 | 116 |

Remedies used:

- `typing.cast(object, ...)` around `Any`-typed stdlib members (`Parameter.annotation`,
  `Signature.return_annotation`, `__annotations__` items) before passing them to
  `stabilize_type_names`. A declared `object` type does not silence `reportAny` on a member
  access, while `cast` does.
- `Callable` → `Callable[..., object]` and `-> list` → `-> list[str]` on the hashing helpers
  this PR already touches.
- Test parameters annotated; the implicit string concatenation is parenthesised.
- Narrow ignores, each with a stated reason: `reportDeprecated` on the four `typing.Optional`/
  `List`/`Sequence`/`Union` entries (the legacy forms are the inputs under test);
  `reportPrivateUsage` on `ListSource._hash_tag_function` (the pre-hash string is the subject);
  `reportUnusedParameter` on the `stem` fixture function. In
  `test_canonical_type_names.py` the rule `reportUnusedParameter` is disabled file-wide,
  because the golden functions' source text is hashed: a line-level comment inside them
  changed `function_content_hash` (verified).
