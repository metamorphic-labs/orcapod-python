# Orcapod

Orcapod is an intuitive and powerful Python library for building highly reproducible
scientific data pipelines. It provides a structured way to load, transform, and track data
through typed, immutable streams -- ensuring that every step of your analysis is traceable,
content-addressable, and reproducible by design.

## Installation

Orcapod supports Python 3.11, 3.12 and 3.13 and is published on PyPI as `orcapod`, under the
MIT License.
`0.1.4` is the latest stable release there (`0.3.0` was withdrawn); a plain `uv add orcapod`
installs it. The workshop uses the latest `0.4.0` release
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

Tables written by `0.2.0` or earlier are not read by this version: see
`CHANGELOG.md` in the repository, *Upgrading from 0.2.0*
(it also covers `0.1.4`).

## Quick example

Create a data source, apply a transformation, and inspect the results -- all in a few lines:

```python
from orcapod import function_pod
from orcapod.sources import DictSource

# 1. Load data into a source
source = DictSource(
    data=[
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
    ],
    tag_columns=["name"], tag_only_columns=True,
    source_id="people",
)

# 2. Define a transform with the function_pod decorator
@function_pod(output_keys="birth_year")
def compute_birth_year(age: int) -> int:
    return 2026 - age

# 3. Apply the function pod and inspect the output
result = compute_birth_year.pod(source)
for tag, data in result:
    print(f"{tag.as_dict()} -> {data.as_dict()}")
# {'name': 'Alice'} -> {'birth_year': 1996}
# {'name': 'Bob'} -> {'birth_year': 2001}
# {'name': 'Charlie'} -> {'birth_year': 1991}
```

## Next steps

- [Getting Started](getting-started.md) -- a hands-on walkthrough of sources, streams,
  function pods, and operators.
- [Tutorials](tutorial/index.md) -- a self-paced core course in seven parts, from a first
  stream to a stored, re-runnable pipeline, and short tutorials on files, inspecting results,
  tracing a value, chaining pipelines and handling errors, with exercises and solutions.
- [Concepts](concepts/sources.md) -- deeper explanations of Orcapod's core abstractions.
- [Troubleshooting](troubleshooting.md) -- common errors, their causes and fixes.
- [API Reference](api/index.md) -- complete reference for all public classes and functions.
