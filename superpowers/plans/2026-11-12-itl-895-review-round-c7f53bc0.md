# ITL-895 / PR #299: review round on c7f53bc0 (successor worker)

State on takeover: head c7f53bc0 pushed, CI green, CodeRabbit review 5876929622 on c7f53bc0
with six inline findings and no body-only findings. Scratch and repro scripts: `.untracked/r2/`.

## Findings (all reproduced through pods / PipelineJob before any change)

| # | Thread (comment id) | Verdict | Evidence | Action |
|---|---|---|---|---|
| 1 | changelog.d/ITL-895.md:19 (4552331351) | accept | "downstream values agree with stored values" contradicts the subclass sentence before it | reword: numeric normalization gives agreement; subclass / datetime-for-date keep more in memory than storage |
| 2 | design_issues/ITL-895-FO2.md:4 (4552331374) | accept | `**Status:** open (deferred)`; checker passes it by prefix, convention lists `open` | `open`, deferral stays in body |
| 3 | user_class_logical_type.py:213 (4552331387) | real, design decision | `.untracked/r2/repro3b.py`: run 1 (async) x=20 stores z=20; run 2 x=20,99: r99 served z=20 with zero calls. First-run sync gives z=-1, async z=20 | no code change without the developer; document + design issue entry, pending coordinator answer |
| 4 | output_conformance.py:204 (4552331409) | accept | `MappingProxyType`, `UserDict` accepted for `dict[str,int]` and rebuilt as dict | `isinstance(value, dict)` in check and normalize |
| 5 | output_conformance.py:213 (4552331439) | accept | `Literal[1]` returning 1.5 stores 1; `Literal["yes"]` returning "no" stored; `Literal["yes", None]` returning None rejected | Literal branch: member by type and value; None allowed when None is a member |
| 6 | FINDINGS.md:227 (4552331469) | accept | text only | add the reproducibility caveat |

Extra observation (not from CodeRabbit): `normalize_output_numbers` rebuilds list and dict
subclass instances as plain list / dict in memory (`.untracked/r2/repro7.py`).

## Steps after coordinator approval

1. Failing tests in `tests/test_core/function_pod/test_output_conformance.py` (public pods):
   non-dict mappings rejected, dict subclass accepted; Literal membership (standalone and stored).
2. Fix `output_conformance.py`; doc edits (1, 2, 6, and whatever is approved for 3).
3. Merge origin/main if moved; full suite 3.12, 3.11, 3.13; ruff, format, basedpyright; golden
   check (no movement expected: conforming values are untouched).
4. One commit, one push, signed replies on all six threads; wait for CI and CodeRabbit on the new head.
