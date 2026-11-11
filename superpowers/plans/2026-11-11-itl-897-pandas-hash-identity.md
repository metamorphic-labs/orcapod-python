# ITL-897 plan and progress notes

Branch: `eywalker/itl-897-pandas-dataframe-hash-ignores-column-order` (from `origin/main` `b197544e`).
Coordinator task `task_25c74f4198f0`, dispatch `ctx_4af6270358e5`.

## Step 1: investigation and proposal (no library code)

- [x] Scripts in `superpowers/scripts/itl_897/`, one per claim; prototypes in `prototype_v1.py`
      (patched into the script's process only).
- [x] Spec: `superpowers/specs/2026-11-11-itl-897-pandas-hash-identity.md`, sections (a) to (h).
- [x] Scope addition from the coordinator (Nov 11): `pa.Table` / `RecordBatch` values, claims (f), (g), (h).
- [x] Proposal sent (ask). Coordinator: HOLD library code; decisions 1, 2, 4 go to the developer
      (also deciding ITL-898, a starfix order option, which may replace the descriptor for orders).
      Decision 3 (coordinator): wait for #286 to merge, then build from `main` on a shared helper.
- [x] (i) Spec section: full `v1` descriptor per property (`claim_i_v1_descriptor.py`,
      `prototype_v1.install_full_descriptor()`); it agrees with `assert_frame_equal` defaults except
      `flags` (storage drops it) and `-0.0`. Same 3 golden entries move.
- [x] (ii) Failing public-interface tests committed (14 red, 24 green), no PR:
      `tests/test_hashing/test_pandas_value_identity.py`,
      `tests/test_core/function_pod/test_pandas_dataframe_order.py`,
      `tests/test_hashing/test_arrow_table_value_identity.py`.
- [x] Grammar extended to every label/dtype that hashes and stores today (f31ab59c).
- [x] Developer decision (Nov 11): orders come from a starfix ordered mode (ITL-898). Spec section
      "Mechanism" says what the option replaces and what stays in the descriptor.
- [ ] Wait for: developer decisions 1, 2, 4; #286 merged; the starfix ordered-mode release.

## Step 2: implementation (only after approval)

To be written from the approved decisions with `sensei:writing-plans`. Expected outline:
failing tests first (reordered columns give a different hash; a cached pod returning
`df.columns[0]` recomputes, in-memory and Delta, sync and async; round trip keeps order), shared
labelled-hash helper, handlers, docs §1c, CHANGELOG `[0.3.0]`, DESIGN_ISSUES entry, golden
changes limited to the approved pandas entries plus additions.
