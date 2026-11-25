# Getting Started

This guide walks you through the core workflow in Orcapod: creating a data source,
inspecting its stream, applying a transformation with a function pod, and examining
the results.

## Installing

Install Orcapod with [uv](https://docs.astral.sh/uv/), the recommended way: start a project
with `uv init` (in a new directory) and add Orcapod to it:

```bash
uv init
uv add "orcapod>=0.4.0rc7"
```

In an environment not managed by uv, `pip install "orcapod>=0.4.0rc7"` installs it instead.

The workshop uses the latest 0.4.0 release candidate, which these commands install. The latest
stable release on PyPI is 0.1.4 (0.3.0 was withdrawn), which a plain `uv add orcapod` installs
and this guide does not cover. For the development version, install from the repository: `uv
add git+https://github.com/metamorphic-labs/orcapod-python`.

## Creating a source

A **source** is the entry point for data in an Orcapod pipeline. The simplest way to
get started is with `DictSource`, which accepts a list of dictionaries:

```python
from orcapod.sources import DictSource

source = DictSource(
    data=[
        {"experiment": "exp_001", "temperature": 20.5, "pressure": 1.01},
        {"experiment": "exp_002", "temperature": 22.3, "pressure": 0.98},
        {"experiment": "exp_003", "temperature": 19.8, "pressure": 1.05},
    ],
    tag_columns=["experiment"], tag_only_columns=True,
    source_id="lab_results",
)
```

- **Tag columns** (`tag_columns`) are the keys that identify each row, like primary keys in a
  database or independent variables in an experiment. Here, `experiment` identifies each
  measurement.
- **Data columns** hold the values. Here they are `temperature` and `pressure`.

Here a column is either a tag or data. `tag_only_columns=True` sets that; see
[Data-tags](concepts/data-tags.md) for columns with both roles.

The `source_id` names the source. Orcapod uses it when it records where a value came from.

!!! note
    Orcapod provides several source types beyond `DictSource`: `ListSource`, `CSVSource`,
    `ArrowTableSource`, `DataFrameSource`, and `DeltaTableSource`. They all produce the
    same kind of immutable stream. See [Sources](concepts/sources.md) for details.

## Inspecting the stream

Every step in Orcapod takes streams and returns a stream. A source is the stream that brings
data in, so you can inspect it right away. In a notebook, put `source` on the last line of a
cell to see a table of its first rows; [Working in notebooks](concepts/notebooks.md) shows
what that table and Orcapod's other notebook displays show.

### Schema

Use `output_schema()` to see the tag and data column types:

```python
tag_schema, data_schema = source.output_schema()
print(tag_schema)
# Schema({'experiment': str})
print(data_schema)
# Schema({'temperature': float, 'pressure': float})
```

### Column names

Use `keys()` to get just the column names:

```python
tag_keys, data_keys = source.keys()
print(tag_keys)
# ('experiment',)
print(data_keys)
# ('temperature', 'pressure')
```

### Iterating over rows

A `for` loop over a stream gives one (tag, data) pair per row:

```python
for tag, data in source:
    print(f"Tag: {tag.as_dict()}, Data: {data.as_dict()}")
# Tag: {'experiment': 'exp_001'}, Data: {'temperature': 20.5, 'pressure': 1.01}
# Tag: {'experiment': 'exp_002'}, Data: {'temperature': 22.3, 'pressure': 0.98}
# Tag: {'experiment': 'exp_003'}, Data: {'temperature': 19.8, 'pressure': 1.05}
```

### Getting the full table

Use `as_table()` to get a PyArrow Table, which you can convert to pandas:

```python
table = source.as_table()
print(table.to_pandas())
#   experiment  temperature  pressure
# 0    exp_001         20.5      1.01
# 1    exp_002         22.3      0.98
# 2    exp_003         19.8      1.05
```

## Applying a function pod

A **function pod** transforms data row by row. Use the `@function_pod` decorator
to turn a plain Python function into a reusable, trackable transformation:

```python
from orcapod import function_pod

@function_pod(output_keys=["temp_fahrenheit", "is_high_pressure"])
def analyze_conditions(temperature: float, pressure: float) -> tuple[float, bool]:
    temp_fahrenheit = temperature * 9.0 / 5.0 + 32.0
    is_high_pressure = pressure > 1.0
    return temp_fahrenheit, is_high_pressure
```

- **Parameter names match data column names.** Each parameter takes the data column with
  exactly its name, here `temperature` and `pressure`.
- **Data columns the function does not take are dropped** when you call the pod.
  `pod(stream, strict=True)` and `pod.process(stream)` reject them instead (see
  [Function Pods](concepts/function-pods.md#input-columns)).
- **Type annotations define the schemas.** Orcapod uses them to build the output schema and
  to validate compatibility when the pod is applied. A parameter without an annotation needs
  its type in the decorator's `input_schema` mapping instead.
- **`output_keys` names the output columns.** The function returns a tuple of two
  values, so `output_keys` is a list of two names, in the order of the tuple. A function that
  returns one value takes a single string, such as `output_keys="result"`.
- **The decorator creates a `.pod` attribute.** You call `.pod(stream)` to apply
  the function to a stream.

Now apply it:

```python
result = analyze_conditions.pod(source)
```

`result` is a new stream. It has the tags of the input, and the function's outputs as its
data columns.

## Inspecting the result

The result stream supports the same inspection methods as the source:

```python
tag_schema, data_schema = result.output_schema()
print(tag_schema)
# Schema({'experiment': str})
print(data_schema)
# Schema({'temp_fahrenheit': float, 'is_high_pressure': bool})
```

The tag schema is unchanged: a function pod passes tags through. The data schema holds the
function's outputs.

Iterate over the results:

```python
for tag, data in result:
    print(f"Tag: {tag.as_dict()}, Data: {data.as_dict()}")
# Tag: {'experiment': 'exp_001'}, Data: {'temp_fahrenheit': 68.9, 'is_high_pressure': True}
# Tag: {'experiment': 'exp_002'}, Data: {'temp_fahrenheit': 72.14, 'is_high_pressure': False}
# Tag: {'experiment': 'exp_003'}, Data: {'temp_fahrenheit': 67.64, 'is_high_pressure': True}
```

Or view it as a table:

```python
print(result.as_table().to_pandas())
#   experiment  temp_fahrenheit  is_high_pressure
# 0    exp_001            68.90              True
# 1    exp_002            72.14             False
# 2    exp_003            67.64              True
```

## Putting it all together

Here is the complete example in one block. Save it as `my_pipeline.py`:

```python
from orcapod import function_pod
from orcapod.sources import DictSource

# Create a source from raw data
source = DictSource(
    data=[
        {"experiment": "exp_001", "temperature": 20.5, "pressure": 1.01},
        {"experiment": "exp_002", "temperature": 22.3, "pressure": 0.98},
        {"experiment": "exp_003", "temperature": 19.8, "pressure": 1.05},
    ],
    tag_columns=["experiment"], tag_only_columns=True,
    source_id="lab_results",
)

# Define a transformation
@function_pod(output_keys=["temp_fahrenheit", "is_high_pressure"])
def analyze_conditions(temperature: float, pressure: float) -> tuple[float, bool]:
    temp_fahrenheit = temperature * 9.0 / 5.0 + 32.0
    is_high_pressure = pressure > 1.0
    return temp_fahrenheit, is_high_pressure

# Apply and view results
result = analyze_conditions.pod(source)
print(result.as_table().to_pandas())
#   experiment  temp_fahrenheit  is_high_pressure
# 0    exp_001            68.90              True
# 1    exp_002            72.14             False
# 2    exp_003            67.64              True
```

Then run it:

```bash
uv run python my_pipeline.py
```

With `pip`, in the activated environment: `python my_pipeline.py`.

## Keeping results

The results above are held in memory, so each new run computes everything again. To save
results in a database and have later runs compute only what is new, record the same steps
inside a `PipelineJob`. See [Pipelines and Jobs](concepts/pipeline-job.md), or work through
the [Tutorial](tutorial/index.md), which builds up to it step by step.
