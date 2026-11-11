# ITL-874 plan: array and tensor storage review and design

Issue: ITL-874. Branch: `eywalker/itl-874-design-array-and-tensor-storage-numpy`.
Review and design only. No change under `src/` or `tests/`.

## Deliverables

1. `superpowers/specs/2026-11-10-array-tensor-storage.md`: the design document.
2. `superpowers/specs/array-tensor-storage-experiments/*.py`: experiment scripts. Each one runs
   with `uv run python <script>` and prints its result. Every claim in the document cites one,
   or says "not verified".
3. A `DESIGN_ISSUES.md` entry for ITL-874, appended at the end.
4. A PR against `main` with only those files. The body says "Refs ITL-874".

## Steps

1. Part A1, hashing and storage (`a1_*.py`). Find the stored bytes and the hashed bytes.
   Check hash stability across processes, memory layout, byte order, dtype, `.npy` header
   version and NumPy 1.26 against 2.x. Cover 0-d, empty and large arrays.
2. Part A2, dtype coverage (`a2_*.py`). Cover every dtype kind, structured field order,
   object, masked, `np.generic` and subclasses.
3. Part A3, pipeline behaviour through the public interface (`a3_*.py`). Cover function pods,
   every operator, the sync and async orchestrators, the in-memory, Delta and SQLite
   databases, and a cached re-run in a fresh process.
4. Part A4, annotations and what foreign tools see (`a4_*.py`).
5. Part B, candidates (`b*_*.py`). Compare `.npy`, `fixed_shape_tensor`,
   `variable_shape_tensor`, a flattened logical type and out-of-table storage on the brief's
   criteria. Run torch through `uv run --with torch`.
6. Write the document. Part A findings and defects with repros, the Part B table, migration
   paths, and Part C options with my pick.
7. Verify that every script runs. Run ruff on the scripts. Merge `origin/main`, run the full
   suite, push, open the PR and link it.

Steps 3–4 and step 5 run as parallel forks. Steps 1–2 and 6–7 run in the main session.
