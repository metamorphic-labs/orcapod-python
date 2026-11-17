# ITL-1173 measurement scripts

These scripts measured why a `PipelineJob` over 10,000 rows into a local `DeltaTableDatabase`
is slow. The findings are in `docs/dev/design_issues/ITL-1173-DS1.md`. Each run starts a fresh
interpreter. Each measurement was repeated at least 3 times, and the minimum and spread are
reported. Run them from the repository root on an otherwise idle machine: Arrow is
multithreaded, and a loaded machine moves the numbers.

| Script | What it measures |
|---|---|
| `bench_one.py <delta\|memory\|noop\|nojob> <N> <commit_every> <dbdir>` | Wall time of one job run over N rows of `double(value)`. `commit_every` 0 means one commit at node end. Prints the run time, the same job run again in the same process, and for Delta the versions, files and bytes of each table. `--profile out.prof` writes a cProfile. |
| `instrument.py <delta\|memory\|noop> <N> <commit_every> <dbdir>` | Inclusive time and call counts of the named code paths (lookups, inserts, commits, hashing, deltalake calls), plus mean commit time per decile of the commits. In `memory` mode it also times a second run by a fresh job object over the same database. |
| `commit_curve.py <empty dir>` | Raw deltalake cost per commit against the table version: load, append by URI, dataset build, `update_incremental`, and append through a kept handle. |
| `warn_test.py all <dir>` | Counts deltalake's "Skipping column ... binary field" log lines on Orcapod's create, append, MERGE and schema-evolution paths, with and without a statistics table property, and for an existing table after the property is set. |

The matrix used for the report:

```
for rep in 1 2 3; do
  for n in 1000 10000; do
    uv run python docs/dev/scripts/itl_1173/bench_one.py nojob $n 0 x
    for every in 100 1000 0; do
      rm -rf /tmp/itl1173_db
      uv run python docs/dev/scripts/itl_1173/bench_one.py delta $n $every /tmp/itl1173_db
      uv run python docs/dev/scripts/itl_1173/bench_one.py delta $n $every /tmp/itl1173_db  # rerun, all stored
    done
  done
done
```

The in-memory rows of the report came from `instrument.py memory` at N = 1,000, 3,000 and
10,000 (commit every 100), and at 10,000 with 1,000 and node end. The run at 10,000 with node
end takes about 37 minutes.

A raw `deltalake` dataset read in a script can hang the interpreter at exit (ITL-892).
`commit_curve.py` and `warn_test.py` end with `os._exit(0)` for that reason.
