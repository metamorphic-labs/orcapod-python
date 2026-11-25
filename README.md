# Orcapod

Orcapod is a Python library for building reproducible scientific data pipelines. Data enters
a pipeline through **sources** and flows through typed, immutable streams, in which every row
is a tag (the keys that identify the row) and its data. **Function pods** compute new values
from each row's data with plain Python functions; **operators** join, filter, group and
reshape streams without computing new values. A **job** runs these steps against a database:
every function result is stored, and a later run computes only what is not stored yet. Every
row records which source and which source row it came from, and every value carries a
provenance token.

## Installation

Orcapod supports Python 3.11, 3.12 and 3.13 and is published on PyPI as `orcapod`.
0.1.4 is the latest stable release there (0.3.0 was withdrawn); a plain `uv add orcapod`
installs it. The workshop uses the latest 0.4.0 release
candidate, which the commands below install.

The recommended way to install it is [uv](https://docs.astral.sh/uv/): `uv add` adds
Orcapod to a uv project (start one with `uv init` in a new directory), and `uv run` runs
your code in that project's environment:

```bash
uv add "orcapod>=0.4.0rc7"
```

In an environment not managed by uv, use `pip install "orcapod>=0.4.0rc7"` instead. For the
development version, install from the repository:
`uv add git+https://github.com/metamorphic-labs/orcapod-python`.

Tables written by 0.2.0 or earlier are not read by this version: see
[Upgrading from 0.2.0](https://github.com/metamorphic-labs/orcapod-python/blob/main/CHANGELOG.md#upgrading-from-020)
(it also covers 0.1.4).

Optional features are installed as extras, for example `uv add "orcapod[spiraldb]"`:

| Extra | Adds |
|-------|------|
| `orcapod[ray]` | Distributed execution with `RayExecutor` (Ray 2.60 or later, which has no wheel for Intel macOS) |
| `orcapod[redis]` | `RedisCacher`, a Redis-backed hash cache |
| `orcapod[postgresql]` | The PostgreSQL connector, table source and file hash cache (psycopg 3.1.17 or later) |
| `orcapod[spiraldb]` | The SpiralDB connector and table source, read only (spiral-sdk) |
| `orcapod[torch]` | PyTorch, for `torch.Tensor` values (any installed `torch` works) |
| `orcapod[all]` | All of the above |

## A quick example

A source of two rows, one function pod, and a job that stores its results in a database:

```python
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource


@function_pod(output_keys="temp_fahrenheit")
def to_fahrenheit(temperature: float) -> float:
    return temperature * 9.0 / 5.0 + 32.0


rows = [
    {"experiment": "exp_001", "temperature": 20.5},
    {"experiment": "exp_002", "temperature": 22.3},
]
database = InMemoryArrowDatabase()


def run_job(rows):
    source = DictSource(data=rows, tag_columns=["experiment"], source_id="lab_results")
    job = PipelineJob(name="lab", database=database)
    with job:  # record the steps; nothing runs yet
        to_fahrenheit.pod(source, label="fahrenheit")
    print(job.run())
    return job


job = run_job(rows)
# Run of job 'lab': all rows succeeded.
#   fahrenheit: 2 rows: 2 computed, 0 from store, 0 failed
print(job.nodes["fahrenheit"].as_table().sort_by("experiment").to_pylist())
# [{'experiment': 'exp_001', 'temp_fahrenheit': 68.9}, {'experiment': 'exp_002', 'temp_fahrenheit': 72.14}]
```

Add a row and run a new job over the same database. The two stored results are found again
by their input values, and only the new row is computed:

```python
rows.append({"experiment": "exp_003", "temperature": 19.8})
job = run_job(rows)
# Run of job 'lab': all rows succeeded.
#   fahrenheit: 3 rows: 1 computed, 2 from store, 0 failed
```

`InMemoryArrowDatabase` keeps results for the life of the Python process;
`DeltaTableDatabase("some/directory")` writes them to disk, so they are found again in a new
session.

## Documentation

- [Getting Started](https://github.com/metamorphic-labs/orcapod-python/blob/main/docs/getting-started.md):
  sources, streams and function pods, step by step.
- [Tutorial](https://github.com/metamorphic-labs/orcapod-python/blob/main/docs/tutorial/index.md):
  a self-paced course in eleven short parts, from a first stream to a stored, re-runnable
  pipeline over files, with exercises.
- [Concepts](https://github.com/metamorphic-labs/orcapod-python/tree/main/docs/concepts): sources,
  streams, function pods, operators, pipelines and jobs, identity and hashing, file types,
  arrays and your own data types.
- Reference: [Hashing](https://github.com/metamorphic-labs/orcapod-python/blob/main/docs/reference/hashing.md)
  and [Golden hashes](https://github.com/metamorphic-labs/orcapod-python/blob/main/docs/reference/golden-hashes.md).
- [Troubleshooting](https://github.com/metamorphic-labs/orcapod-python/blob/main/docs/troubleshooting.md):
  common errors, their causes and fixes.
- [CHANGELOG](https://github.com/metamorphic-labs/orcapod-python/blob/main/CHANGELOG.md): what
  changed in each release, with upgrade notes.

## Status

Orcapod is before version 1.0. In the development version (0.4.0 is next):

- **Hash values of data context v1 freeze at 0.4.0.** Hashes name stored tables and find
  stored results again, so a hash value released in 0.4.0 will not change in a later release
  of the same data context; CI checks this. Until 0.4.0 a v1 hash may still change, only by
  an approved and explained change (0.3.0, which first shipped v1, was withdrawn). This
  does not cover a provisional operator (such as `PolarsFilter` and `Filter`): its identity, and every hash
  downstream of it, may still change in a later release of v1. See
  [Golden hashes](https://github.com/metamorphic-labs/orcapod-python/blob/main/docs/reference/golden-hashes.md)
  and
  [Provisional operators](https://github.com/metamorphic-labs/orcapod-python/blob/main/docs/concepts/operators.md#provisional-operators).
- **Library value types are provisional.** The hashes of `numpy.ndarray`, `torch.Tensor`, and
  pandas and polars `DataFrame` and `Series` values are provisional until the library value
  type audit (after 0.4.0rc1) and may change then; results computed from them may then be
  computed again. Using one issues `ProvisionalHashWarning` once per
  type per process.
- **Stores from earlier versions are not read as current.** Tables written by 0.1.x or 0.2.0
  are neither read as results nor modified, and results stored by a development build before
  v1 was released may not be found again by a later build, which then computes them again.
  Read
  [Upgrading from 0.2.0](https://github.com/metamorphic-labs/orcapod-python/blob/main/CHANGELOG.md#upgrading-from-020)
  before running a job over an old store, in particular one with side-effect nodes.
- **Connector databases cannot hold function nodes yet.** A job over a SQLite, PostgreSQL or
  SpiralDB database refuses every function node when it is built. Use `InMemoryArrowDatabase`
  or `DeltaTableDatabase` for jobs.

## Related packages

SpikeInterface support lives in a separate package,
[`orcapod-extension-spikeinterface`](https://github.com/nauticalab/orcapod-extension-spikeinterface).
It is not published on PyPI; install it from its repository.

## Development

The project uses [uv](https://docs.astral.sh/uv/):

```bash
uv sync --all-extras --dev
```

Every pull request must pass these checks:

```bash
uv run --with ruff ruff check .
uv run --with ruff ruff format --check .
uv run --with basedpyright basedpyright
uv run pytest tests/
```

CI runs the tests in parallel (pytest-xdist). Locally they run serially; `uv run pytest -n 4 tests/`
runs them on four workers, each holding its own copy of the suite's memory, so use it only on a
machine with cores and memory to spare.

The PostgreSQL integration tests are skipped unless asked for
(`uv run pytest -m postgres --postgres`) and need a running PostgreSQL server, reached
through the standard `PG*` environment variables; see
[the workflow that runs them](https://github.com/metamorphic-labs/orcapod-python/blob/main/.github/workflows/run-postgres-tests.yml).
Releases are cut by the GitHub Actions Release workflow; see
[RELEASING.md](https://github.com/metamorphic-labs/orcapod-python/blob/main/RELEASING.md).

## License

Orcapod is released under the
[MIT License](https://github.com/metamorphic-labs/orcapod-python/blob/main/LICENSE).
