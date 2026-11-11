# ITL-897 plan and progress notes

Branch: `eywalker/itl-897-pandas-dataframe-hash-ignores-column-order` (from `origin/main` `b197544e`).
Coordinator task `task_25c74f4198f0`, dispatch `ctx_4af6270358e5`.

## Step 1: investigation and proposal (no library code)

- [x] Scripts in `superpowers/scripts/itl_897/`, one per claim; prototypes in `prototype_v1.py`
      (patched into the script's process only).
- [x] Spec: `superpowers/specs/2026-11-11-itl-897-pandas-hash-identity.md`, sections (a) to (h).
- [x] Scope addition from the coordinator (Nov 11): `pa.Table` / `RecordBatch` values, claims (f), (g), (h).
- [ ] Send the proposal with `orca orchestration ask`; wait for decisions 1 to 4 in the spec.

## Step 2: implementation (only after approval)

To be written from the approved decisions with `sensei:writing-plans`. Expected outline:
failing tests first (reordered columns give a different hash; a cached pod returning
`df.columns[0]` recomputes, in-memory and Delta, sync and async; round trip keeps order), shared
labelled-hash helper, handlers, docs §1c, CHANGELOG `[0.3.0]`, DESIGN_ISSUES entry, golden
changes limited to the approved pandas entries plus additions.
