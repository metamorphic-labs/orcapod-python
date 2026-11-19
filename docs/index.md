# Orcapod

Orcapod is an intuitive and powerful Python library for building highly reproducible
scientific data pipelines. It provides a structured way to load, transform, and track data
through typed, immutable streams -- ensuring that every step of your analysis is traceable,
content-addressable, and reproducible by design.

## Installation

Orcapod supports Python 3.11, 3.12 and 3.13 and is published on PyPI as `orcapod`, under the
MIT License.
No release after `0.1.4` is available there yet: `0.2.0` was never published, and `0.3.0` was
withdrawn on 2026-11-17. Until `0.4.0` is released, install the development version from the
repository.

The recommended way to install it is [uv](https://docs.astral.sh/uv/): `uv add` adds
Orcapod to a uv project (start one with `uv init` in a new directory), and `uv run` runs
your code in that project's environment:

```bash
uv add git+https://github.com/metamorphic-labs/orcapod-python
```

In an environment not managed by uv, use
`pip install git+https://github.com/metamorphic-labs/orcapod-python` instead. Once `0.4.0` is
out, `uv add orcapod` (or `pip install orcapod`) installs the latest stable release; to try a
release candidate, name it, for example `uv add "orcapod==0.4.0rc1"`.

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
- [Tutorial](tutorial/index.md) -- a self-paced course in eleven short parts, from a first
  stream to a stored, re-runnable pipeline over files, with exercises and solutions.
- [Concepts](concepts/sources.md) -- deeper explanations of Orcapod's core abstractions.
- [Troubleshooting](troubleshooting.md) -- common errors, their causes and fixes.
- [API Reference](api/index.md) -- complete reference for all public classes and functions.
