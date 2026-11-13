# Raw output of the ITL-1061 scripts

`*.txt` is gitignored in this repository; these files are force-added as the evidence the
report cites. Builds:

- no suffix: `main` at `97263e5b`, except `node_state.txt` and `profile_runs.txt`, taken after
  merging `main` at `89160c47`.
- `*.main-89160c47.txt`: `main` at `89160c47` (PR #355 merged).
- `*-pr355-963310fb.txt`: PR #355's head `963310fb`, run with that build's `src` first on
  `PYTHONPATH`.
- `delta-process1.txt` / `delta-process2.txt`: two processes, one after the other, on one
  `DeltaTableDatabase` (`97263e5b`).
