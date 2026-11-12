# Your Own Data Types

Function pods can take and return your own classes: Python
[dataclasses](https://docs.python.org/3/library/dataclasses.html) and
[pydantic](https://docs.pydantic.dev/) models. Orcapod stores their values in Arrow
columns and caches results computed from them like any other value.

Caching needs to know when two classes are **the same type**. This page explains how
Orcapod decides, and what that means for notebooks, scripts and modules.

## What makes two classes the same type

Orcapod identifies a dataclass or pydantic model by three things:

| Component | What it is |
|---|---|
| **Name** | The class name for a class defined in a notebook or a script, the module path and class name for a class defined in a module, or a name you declare with `op.register_type` |
| **Kind** | Dataclass or pydantic model. A dataclass and a pydantic model are never the same type, even with the same name and fields |
| **Fields** | The name and type of every stored field, in the order the class declares them |

Two classes that agree on all three are the same type: results stored with one are found
again with the other.

Field order counts because your code can see it: a dataclass compares, unpacks and
prints its fields in declared order, and a pydantic model dumps and serializes in
declared order. Two classes with the same fields in another order are two types.

Known boundary: defaults, methods, properties, pydantic computed fields and validators,
and docstrings are **not** part of the identity, although a pod can observe them.
Changing one of them keeps the type and its stored results. If such a change alters what
a pod computes, bump the pod's version, as you would for any change to its code.

Adding, removing, renaming, retyping or reordering fields makes a **new type**. Results computed with the old
definition stay in the database, and the new type computes its results into new tables,
so a stored value is never read back with fields it was not written with.

## Defining and redefining a class in a notebook

Define a class, use it in a pod and run a job:

```python
from dataclasses import dataclass

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource


@dataclass
class Point:
    x: float
    y: float


calls = []


@function_pod(output_keys="norm")
def norm(p: Point) -> float:
    calls.append(p)  # only here to show when the function really runs
    return (p.x**2 + p.y**2) ** 0.5


db = InMemoryArrowDatabase()


def run(point_class):
    rows = [
        {"k": "a", "p": point_class(x=3.0, y=4.0)},
        {"k": "b", "p": point_class(x=6.0, y=8.0)},
    ]
    job = PipelineJob(name="points", store=db)
    with job:
        source = DictSource(data=rows, tag_columns=["k"], source_id="pts")
        norm.pod(source, label="norm")
    job.run()
    return job.nodes["norm"]


print(run(Point).as_table().to_pandas())
#    k  norm
# 0  a   5.0
# 1  b  10.0
print("function calls:", len(calls))
# function calls: 2
```

Running the cell that defines `Point` again creates a new class object with the same
name and fields. It is the same type, so the next run finds the stored results:

```python
@dataclass
class Point:
    x: float
    y: float


run(Point)
print("function calls:", len(calls))
# function calls: 2
```

Adding, removing, retyping or reordering fields makes a new type. Re-run the pod cell too, so that `norm` takes
the new `Point`; the run then computes into new tables:

```python
@dataclass
class Point:
    x: float
    y: float
    label: str = ""


@function_pod(output_keys="norm")
def norm(p: Point) -> float:
    calls.append(p)
    return (p.x**2 + p.y**2) ** 0.5


run(Point)
print("function calls:", len(calls))
# function calls: 4
```

## How a class is named

The name is part of the identity, so it decides whether results survive moving code
around:

- A class defined in a **notebook or a script** run directly (`__main__`) is named by its
  class name: `Point`.
- A class defined in a **module** is named by its module path and class name:
  `pkg.models.Point`.

So results computed with `Point` in a notebook are **not** found by
`pkg.models.Point` after you move the class into a module, unless you give the module
class the same name. Declare it with `op.register_type`:

```python
import orcapod as op


@op.register_type(name="Point")
@dataclass
class ModulePoint:
    x: float
    y: float
    label: str = ""


run(ModulePoint)
print("function calls:", len(calls))
# function calls: 4
```

`op.register_type` accepts:

- `name="..."`: the name to use, the same wherever the class is defined. It must not be
  empty, contain `@` or spaces, or start with `orcapod.`.
- `qualified_name=False`: name a module class by its class name alone, without retyping it.
- no arguments: keep the default name.

It never changes your class; it records the name in Orcapod's registry and returns the
class. Declare the name before the class is first used in a source or a pod. Declaring
the same class again with the same name does nothing; declaring it under a different
name raises `ValueError`.

You can see a class's identity on any column that holds it. The part before `@` is the
name; the rest is a hash of the name, the kind and the fields:

```python
source = DictSource(data=[{"k": "a", "p": ModulePoint(1.0, 2.0)}], tag_columns=["k"])
print(source.as_table().schema.field("p").type.extension_name)
# Point@...
```

The first time a class defined in a notebook or a script is used, Orcapod prints a
one-time warning saying that it is identified by its class name and fields.

## Classes inside classes

A field whose type is another dataclass or pydantic model carries that class's own
identity: its name (however that class is named or registered), its kind and its fields.
Renaming the inner class, registering it under another name, or changing its fields
therefore makes the outer class a new type too. The order in which you declare the inner
and outer classes does not matter, as long as both are declared before first use.

A class that refers back to itself, directly (`next: Node | None`) or through other
classes, cannot be stored: Arrow has no recursive struct type. Orcapod raises
`RecursiveTypeError` naming the path, for example `Node.next -> Node`.

## Reading stored values back

Reading stored values needs a class with the stored name, kind and fields. Orcapod
looks for one among the classes registered or used in the current process, then in the
script or notebook (`__main__`), then by importing the name as a module path.

- If no class with that name exists, reading raises `ImportError` naming the column, the
  type and its fields, and showing the `op.register_type` call to declare it.
- If a class with that name exists but its fields or their order differ, reading raises
  `TypeIdentityError` (from `orcapod.errors`) naming the column and both sets of fields.

Neither case is reported as missing data, and nothing stored is changed.

## Rules and limits

- Classes defined inside a function cannot be registered: define them at module level or
  at the top of the notebook.
- Each subclass is its own type with its own fields; it is never stored as its parent.
- Two different classes registered under the same name with the same fields are the same
  type. Orcapod warns once when that happens across two modules, because stored values are
  then read back as the most recently registered class.
- Results stored by Orcapod versions before 0.3 identified classes by module and class
  name only. Version 0.3 does not read those columns; pipelines compute their results again
  into new tables and leave the old ones untouched.

## How it connects to other concepts

- [Function Pods](function-pods.md): the pods that take and return these classes.
- [Identity & Hashing](identity.md): how a type's identity enters pipeline and content
  hashes.
