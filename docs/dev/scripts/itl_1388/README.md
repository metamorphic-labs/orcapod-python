# ITL-1388 save/load audit scripts

Synthetic data only. Every claim in `docs/dev/specs/itl-1388-save-load-audit.md` is
reproduced by these scripts; outputs go to a directory you pass (the audit used a scratch
folder outside the repository).

```
ROOT=<some scratch folder>
uv run python docs/dev/scripts/itl_1388/writer.py $ROOT/delta delta    # build, run 3x, save
uv run python docs/dev/scripts/itl_1388/writer.py $ROOT/sqlite sqlite  # operator-only job
uv run python docs/dev/scripts/itl_1388/run_audit.py $ROOT             # readers, fresh processes
uv run python docs/dev/scripts/itl_1388/analyze.py $ROOT/delta $ROOT/delta/out/main.b.json
uv run python docs/dev/scripts/itl_1388/roundtrip.py $ROOT/delta main b
uv run python docs/dev/scripts/itl_1388/scenarios.py $ROOT             # changed/moved/missing, blueprint
uv run python docs/dev/scripts/itl_1388/secrets_check.py $ROOT/secrets
```

- `pods_template.py`: the user module `itl1388_pods` in two variants (`v1`, `changed`).
- `writer.py`: the jobs (`main` run three times over changing CSV generations; `other`, same
  name and database, other source; `separate`, another name; `downstream` over a
  `DerivedSource`; `polling` over a `PollingSource`) and `expected.json` (the live rows).
- `reader.py`: one fresh process per job and situation (a: pods importable, a_imported:
  imported before load, b: absent, c: changed); records every read.
- `analyze.py`: rows shown vs the live job's rows, as multisets.
- `scenario_child.py`: one situation of `scenarios.py` per fresh process.

`scenarios.py` renames the database and data folders temporarily and restores them; run it
alone.
