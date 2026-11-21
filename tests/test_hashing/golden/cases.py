"""Golden hash cases (ITL-626): every hash value Orcapod persists or embeds in a name.

Each case computes a flat ``{entry: value}`` map. ``compute_group`` prefixes each entry with
its case name, so a golden file entry reads ``<case>.<entry>``. The values are pinned in
the golden folder of the current data context, ``data/<folder>/<group>-NN.json``, by
``generate_golden_hashes.py`` and checked by ``test_golden_hashes.py`` (ITL-946; see
``README.md``).

DO NOT MOVE OR RENAME anything defined in this module. Function pods, dataclasses and
pydantic models hash together with their defining module and qualified name, so moving them
changes pinned values even though no hashing code changed.

Imports of names that older releases lack are made inside the case that needs them, so the
generator can be run against an older checkout (``--allow-errors``) to list the values that
changed between releases. A case the old API cannot express records an ``ERROR:`` entry.

Values that are volatile by design are deliberately not pinned:

* the record ids of a function pod's result table, and the per-row token inside a function
  pod's output source info, are time-based UUIDv7 values;
* ``__pod_ts``, ``__pf_var_git_hash`` and ``__pf_exec_python_version`` record when, where and
  on which interpreter a result was computed.
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import collections
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pa_csv
import pydantic

import orcapod as op
from orcapod import dataclass as structural_dataclass
from orcapod import function_pod

CaseFn = Callable[[], dict[str, Any]]

GROUPS = ("semantic", "schema", "arrow", "datagram", "pipeline", "torch")

OPTIONAL_GROUPS: dict[str, str] = {"torch": "torch"}
"""Groups that need an optional dependency, mapped to the module they need (ITL-871).

Such a group is computed, added to and checked only where the module is installed; a
golden file is never rewritten, with the module or without it (ITL-946).
"""


def group_available(group: str) -> bool:
    """Whether ``group`` can be computed in this environment.

    Args:
        group: One of ``GROUPS``.

    Returns:
        False when the group needs an optional module that is not installed.
    """
    import importlib.util

    module = OPTIONAL_GROUPS.get(group)
    return module is None or importlib.util.find_spec(module) is not None


def available_groups() -> tuple[str, ...]:
    """The groups that can be computed in this environment, in ``GROUPS`` order."""
    return tuple(g for g in GROUPS if group_available(g))


_CASES: dict[str, list[tuple[str, CaseFn, tuple[str, ...]]]] = {
    group: [] for group in GROUPS
}


def case(
    group: str, name: str, *, provisional: tuple[str, ...] = ()
) -> Callable[[CaseFn], CaseFn]:
    """Register a golden case under ``group``.

    Args:
        group: One of ``GROUPS``; selects the golden files (``<group>-NN.json``) that
            pin the entries.
        name: Case name, used as the prefix of every entry the case returns.
        provisional: The provisional features the case depends on
            (``declarable_provisional_names``; ITL-1266). A case that declares one is
            pinned in the provisional file of its group, which no release freezes. A case
            that uses a provisional feature must declare it (``compute_group_kinds``).

    Returns:
        A decorator that registers the case function and returns it unchanged.
    """

    def register(fn: CaseFn) -> CaseFn:
        _CASES[group].append((name, fn, tuple(provisional)))
        return fn

    return register


def declarable_provisional_names() -> frozenset[str]:
    """The names a case may declare provisional: the provisional operators (ITL-1266)
    and the provisional library value types, by extension name (the ``provisional``
    list of ``contexts/logical_types.json``; ITL-1324)."""
    import importlib.resources

    from orcapod.core.operators.provisional import PROVISIONAL_OPERATORS

    registration = json.loads(
        importlib.resources.files("orcapod.contexts")
        .joinpath("logical_types.json")
        .read_text()
    )
    return frozenset(PROVISIONAL_OPERATORS) | frozenset(
        registration.get("provisional", [])
    )


# The provisional library value types, by the name a case declares (ITL-1324).
NUMPY_NDARRAY = "orcapod.numpy.ndarray"
PANDAS_DATAFRAME = "orcapod.pandas.dataframe"
PANDAS_SERIES = "orcapod.pandas.series"
POLARS_DATAFRAME = "orcapod.polars.dataframe"
POLARS_SERIES = "orcapod.polars.series"
TORCH_TENSOR = "orcapod.torch.tensor"
POLARS_TYPES = (POLARS_DATAFRAME, POLARS_SERIES)
NUMPY_PANDAS_TYPES = (NUMPY_NDARRAY, PANDAS_DATAFRAME, PANDAS_SERIES)


def _library_type_name(tp: type) -> str | None:
    """The provisional library value type a class is (or derives from), by name."""
    kinds: list[tuple[str, str, str]] = [
        ("numpy", "ndarray", NUMPY_NDARRAY),
        ("pandas", "DataFrame", PANDAS_DATAFRAME),
        ("pandas", "Series", PANDAS_SERIES),
        ("polars", "DataFrame", POLARS_DATAFRAME),
        ("polars", "Series", POLARS_SERIES),
        ("torch", "Tensor", TORCH_TENSOR),
    ]
    import sys

    for module, name, declared in kinds:
        # Only a library already imported can have made the class.
        cls = getattr(sys.modules.get(module), name, None)
        if isinstance(cls, type) and issubclass(tp, cls):
            return declared
    return None


def provisional_types_of(
    item: Any, _seen: frozenset[int] = frozenset()
) -> frozenset[str]:
    """The provisional library value types a value or a type is, or embeds (ITL-1324).

    A type embeds one through its generic arguments (``list[np.ndarray]``) and a user
    class through its fields, at any depth: a user class's identity record holds the
    identifier of each field's logical type, so a class with a field of a provisional
    type has provisional hashes too. A value embeds one through its type and, for a
    container, its elements. This is how a golden case splits its items into frozen
    and provisional entries: the tool's use record cannot see a dependence hidden
    behind an identity built earlier and cached (a registered user class), so the
    split is declared from the types (``test_golden_provisional_types.py`` checks it
    against each fixture class's identity record).

    Args:
        item: A value or a type.

    Returns:
        The declared names (``NUMPY_NDARRAY``, ...) of the types found.
    """
    import typing

    from orcapod.logical_types.user_class_identity import type_hints

    if id(item) in _seen:
        return frozenset()
    seen = _seen | {id(item)}
    names: set[str] = set()
    if isinstance(item, type) or typing.get_origin(item) is not None:
        tp: Any = item
        for arg in typing.get_args(tp):
            names |= provisional_types_of(arg, seen)
        origin = typing.get_origin(tp)
        if origin is not None:
            tp = origin
        if isinstance(tp, type):
            declared = _library_type_name(tp)
            if declared is not None:
                names.add(declared)
            elif dataclasses.is_dataclass(tp) or issubclass(tp, pydantic.BaseModel):
                for hint in type_hints(tp).values():
                    names |= provisional_types_of(hint, seen)
        return frozenset(names)
    if isinstance(item, dict):
        for key, value in item.items():
            names |= provisional_types_of(key, seen) | provisional_types_of(value, seen)
    elif isinstance(item, (list, tuple, set, frozenset)):
        for element in item:
            names |= provisional_types_of(element, seen)
    return frozenset(names | provisional_types_of(type(item), seen))


def _recording() -> Any:
    """``recording_provisional_uses`` of the library, or an empty record for an older one.

    The generator is also run against an older checkout's library (``--stdout``), which
    has no record of provisional uses.
    """
    try:
        from orcapod.utils.provisional import recording_provisional_uses
    except ImportError:
        return contextlib.nullcontext(set())
    return recording_provisional_uses()


def compute_group_kinds(
    group: str, *, allow_errors: bool = False
) -> tuple[dict[str, Any], frozenset[str]]:
    """Compute every entry of one golden group, and say which entries are provisional.

    Each case runs while the library records the provisional features it uses
    (``orcapod.utils.provisional.recording_provisional_uses``): a provisional operator
    when it is built, a provisional library value type when it is resolved. A case that
    uses one it does not declare, or declares a name ``declarable_provisional_names`` does
    not hold, is refused, so a value that depends directly on a provisional feature can
    never be pinned in a frozen file. The fixtures are registered first, outside every
    record, so registering one is not a case's use.

    Limit: the record sees a direct use only. A user class with a field of a provisional
    library type embeds that type's identifier in its identity, which is built once, at
    registration, and cached; a later use of the class records nothing. Such items are
    split out by ``provisional_types_of`` instead (``test_golden_provisional_types.py``).

    Args:
        group: One of ``GROUPS``.
        allow_errors: When true, a case that raises records a single
            ``"ERROR: <type>: <message>"`` entry instead of propagating, and the
            declarations are not checked. Used only when running the generator against an
            older release.

    Returns:
        A dict of ``<case>.<entry>`` names to JSON-compatible values, sorted by name, and
        the names of the entries of cases that declare a provisional feature.

    Raises:
        ValueError: A case declares an unknown provisional name, uses a provisional
            feature it does not declare, or two cases produce one entry name.
    """
    out: dict[str, Any] = {}
    provisional_entries: set[str] = set()
    # The fixtures are registered before any case records its uses (ITL-1324).
    register_logical_classes()
    for name, fn, declared in _CASES[group]:
        if not allow_errors:
            unknown = sorted(set(declared) - declarable_provisional_names())
            if unknown:
                raise ValueError(
                    f"golden case {group}:{name} declares {unknown} provisional, but "
                    "they are not provisional (orcapod.operators.PROVISIONAL_OPERATORS, "
                    "or 'provisional' of contexts/logical_types.json)"
                )
        try:
            with _recording() as used:
                values = fn()
        except Exception as exc:
            if not allow_errors:
                raise
            out[name] = f"ERROR: {type(exc).__name__}: {str(exc).splitlines()[0][:200]}"
            continue
        undeclared = sorted(set(used) - set(declared))
        if undeclared and not allow_errors:
            raise ValueError(
                f"golden case {group}:{name} uses the provisional {undeclared} without "
                f"declaring it: register it with @case({group!r}, {name!r}, "
                f"provisional={tuple(sorted(used))!r}), so that its values go to the "
                "provisional file, never to a frozen one"
            )
        for key, value in values.items():
            full = f"{name}.{key}" if key else name
            if full in out:
                raise ValueError(f"duplicate golden entry {full!r}")
            out[full] = value
            if declared:
                provisional_entries.add(full)
    return dict(sorted(out.items())), frozenset(provisional_entries)


def compute_group(group: str, *, allow_errors: bool = False) -> dict[str, Any]:
    """Compute every entry of one golden group (``compute_group_kinds``, values only).

    Args:
        group: One of ``GROUPS``.
        allow_errors: Passed to ``compute_group_kinds``.

    Returns:
        A dict of ``<case>.<entry>`` names to JSON-compatible values, sorted by name.
    """
    values, _ = compute_group_kinds(group, allow_errors=allow_errors)
    return values


def compute_all(*, allow_errors: bool = False) -> dict[str, dict[str, Any]]:
    """Compute every golden group this environment can compute (``available_groups``).

    Args:
        allow_errors: Passed to ``compute_group``.

    Returns:
        A dict mapping each group name to its computed entries.
    """
    return {g: compute_group(g, allow_errors=allow_errors) for g in available_groups()}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _ctx() -> Any:
    from orcapod.contexts import get_default_context

    return get_default_context()


def _bytes_text(value: bytes) -> str:
    """Hex-encode bytes, keeping a stored hash's ASCII method prefix readable.

    Stored content hashes are ``b"<method>:<digest>"`` (e.g. ``b"arrow_v0.1:..."``); they are
    rendered as ``"<method>:<digest hex>"``. Any other bytes value is plain hex.
    """
    prefix, sep, digest = value.partition(b":")
    if sep and prefix and all(0x21 <= c <= 0x7E for c in prefix):
        return f"{prefix.decode('ascii')}:{digest.hex()}"
    return value.hex()


def _jsonable(value: Any) -> Any:
    """Convert a value to a JSON-compatible form (bytes become hex)."""
    if isinstance(value, (bytes, bytearray)):
        return _bytes_text(bytes(value))
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (Path, uuid.UUID)):
        return str(value)
    return value


def _row_key(tag: dict[str, Any]) -> str:
    return json.dumps(_jsonable(tag), sort_keys=True, separators=(",", ":"))


def stream_summary(stream: Any) -> dict[str, Any]:
    """Summarize a stream's identity through its public interface.

    Rows are keyed by their user tag values, so a failure names the row and the pinned
    values do not depend on emission order.

    Args:
        stream: Any Orcapod stream (source, operator output, function pod output, node).

    Returns:
        Entries for the stream's content hash, pipeline hash, system-tag column names, and
        per-row system-tag values (record ids as hex) and data content hashes.
    """
    tag_keys, _ = stream.keys()
    all_tag_keys, _ = stream.keys(columns={"system_tags": True})
    sys_cols = [k for k in all_tag_keys if k not in tag_keys]
    out: dict[str, Any] = {
        "content_hash": stream.content_hash().to_string(),
        "pipeline_hash": stream.pipeline_hash().to_string(),
        "system_tag_columns": sys_cols,
    }
    for tag, data in stream.iter_data():
        row = _row_key(tag.as_dict())
        tag_all = tag.as_dict(all_info=True)
        for col in sys_cols:
            out[f"row[{row}].{col}"] = _jsonable(tag_all[col])
        out[f"row[{row}].data_hash"] = data.content_hash().to_string()
    return out


def _prefixed(prefix: str, values: dict[str, Any]) -> dict[str, Any]:
    return {f"{prefix}.{k}": v for k, v in values.items()}


# ---------------------------------------------------------------------------
# Logical-type fixtures — DO NOT MOVE (module path is part of their identity)
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class GoldenPoint:
    """Dataclass whose logical type and values are pinned."""

    x: int
    y: float


class GoldenModel(pydantic.BaseModel):
    """Pydantic model whose logical type and values are pinned."""

    name: str
    count: int


@structural_dataclass(name="golden.Declared")
class GoldenDeclared:
    """Structural class (ITL-972) with an optional field."""

    label: str
    score: float | None


class GoldenDeclaredModel(pydantic.BaseModel):
    """Pydantic model with the same fields as ``GoldenDeclared``: another type."""

    label: str
    score: float | None


@structural_dataclass(name="golden.Inner")
class GoldenInner:
    """Structural class nested in ``GoldenOuter``; its record enters the outer one."""

    value: int


@structural_dataclass(name="golden.Outer")
class GoldenOuter:
    """Structural class with a nested structural field and a container field."""

    inner: GoldenInner
    tags: list[str]


@structural_dataclass(name="golden.WithArray")
class GoldenWithArray:
    """Structural class with a NumPy array field; the array's identifier enters its identity."""

    label: str
    values: np.ndarray  # pyright: ignore[reportMissingTypeArgument]  # the bare class is the annotation Orcapod stores


@structural_dataclass(name="golden.Twin")
class GoldenTwinAB:
    """Structural ``golden.Twin``; same fields as ``GoldenTwinBA`` in another order.

    Declared field order is part of the identity (ITL-881), so the twins are two types
    and must pin different values.
    """

    a: int
    b: str


@structural_dataclass(name="golden.Twin")
class GoldenTwinBA:
    """Structural ``golden.Twin``; must pin values different from ``GoldenTwinAB``'s."""

    b: str
    a: int


@structural_dataclass(name="golden.Declared")
class GoldenDeclaredAgain:
    """A second class of the type ``golden.Declared``: must pin ``GoldenDeclared``'s values."""

    label: str
    score: float | None


@dataclasses.dataclass
class GoldenChild(GoldenPoint):
    """Ordinary subclass of ``GoldenPoint``: its own type, with its own field."""

    z: int = 0


@dataclasses.dataclass
class GoldenHolder:
    """Ordinary dataclass with a structural field; the inner record enters its identity."""

    inner: GoldenInner
    note: str


_REGISTERED_WITH: set[int] = set()


def register_logical_classes() -> None:
    """Register the dataclass and pydantic fixtures with the default type converter.

    Once per type converter: ``compute_group_kinds`` registers them before the recorded
    cases, so registering (which resolves a field's provisional type, ``GoldenWithArray``)
    is not counted as a case's use of that type (ITL-1324).
    """
    tc = _ctx().type_converter
    if id(tc) in _REGISTERED_WITH:
        return
    tc.register_python_class(GoldenPoint)
    tc.register_python_class(GoldenModel)
    for cls in (
        GoldenDeclared,
        GoldenDeclaredModel,
        GoldenOuter,
        GoldenWithArray,
        GoldenTwinAB,
        GoldenTwinBA,
        GoldenDeclaredAgain,
        GoldenChild,
        GoldenHolder,
    ):
        tc.register_python_class(cls)
    _REGISTERED_WITH.add(id(tc))


PATH_A = Path("/golden/data/a.txt")
PATH_B = Path("/golden/data/b.txt")
UUID_A = uuid.UUID("12345678-1234-5678-1234-567812345678")
UUID_B = uuid.UUID("87654321-4321-8765-4321-876543218765")


# ---------------------------------------------------------------------------
# Group: semantic — BaseSemanticHasher on Python values and type objects
# ---------------------------------------------------------------------------

# Added by ITL-942 (finding T): field order is part of a namedtuple's hash.
_POINT_XY = collections.namedtuple("Point", ["x", "y"])
_POINT_YX = collections.namedtuple("Point", ["y", "x"])

SEMANTIC_VALUES: dict[str, Any] = {
    "int.zero": 0,
    "int.one": 1,
    "int.negative": -1,
    "int.large": 2**62,
    "int.huge": 2**100,
    "float.zero": 0.0,
    "float.one_half": 1.5,
    "float.negative": -2.25,
    "float.tiny": 1e-300,
    "str.empty": "",
    "str.ascii": "orcapod",
    "str.unicode": "üñíçødé ✓",
    "bytes.empty": b"",
    "bytes.binary": b"\x00\x01\xfe\xff",
    "bytearray.binary": bytearray(b"\x00\x01\xfe\xff"),
    "bool.true": True,
    "bool.false": False,
    "none": None,
    "list.empty": [],
    "list.ints": [1, 2, 3],
    "list.mixed": [1, "a", None, 2.5],
    "tuple.ints": (1, 2, 3),
    "dict.empty": {},
    "dict.ab": {"a": 1, "b": 2},
    "dict.ba": {"b": 2, "a": 1},
    "set.ints": {3, 1, 2},
    "frozenset.ints": frozenset({3, 1, 2}),
    "set.strs": {"x", "y", "z"},
    "nested": {"a": [1, {"b": (2, 3)}], "c": {"d": None}},
    "uuid.a": UUID_A,
    "uuid.nil": uuid.UUID(int=0),
    "ndarray.int32": np.arange(6, dtype=np.int32).reshape(2, 3),
    "ndarray.int64": np.arange(6, dtype=np.int64).reshape(2, 3),
    "ndarray.float64": np.array([[0.5, 1.5], [2.5, 3.5]], dtype=np.float64),
    "ndarray.empty": np.zeros((0,), dtype=np.float32),
    # Added by ITL-981: the properties the strict array hash counts or never counts.
    "ndarray.fortran": np.asfortranarray(np.arange(6, dtype=np.int32).reshape(2, 3)),
    "ndarray.big_endian": np.arange(6, dtype=">i4").reshape(2, 3),
    "ndarray.aligned_struct": np.array(
        [(1, 0.5), (-2, 1.5)], dtype=np.dtype([("a", "i1"), ("b", "<f8")], align=True)
    ),
    "ndarray.strided_view": np.arange(12, dtype=np.int32).reshape(2, 6)[:, ::2],
    "dataframe.basic": pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}),
    # Added by ITL-942: set order, dict keys and '__type__' dicts.
    "set.mixed_str_equal": {1, "1", 2, "2"},
    "dict.int_keys": {1: "x", 2: "y"},
    "dict.str_keys_like_ints": {"1": "x", "2": "y"},
    "dict.type_key": {"__type__": "tuple", "items": [1, 2]},
    "namedtuple.xy": _POINT_XY(x=1, y=2),
    "namedtuple.yx": _POINT_YX(y=2, x=1),
    # Added by ITL-897: pandas value hashes (pandas_dataframe_v1 / pandas_series_v1).
    "dataframe.reordered": pd.DataFrame({"b": ["x", "y"], "a": [1, 2]}),
    "dataframe.categorical": pd.DataFrame(
        {"c": pd.Categorical(["x", "y", "x"], categories=["y", "x", "z"])}
    ),
    "dataframe.multiindex": pd.DataFrame(
        {"a": [1, 2]},
        index=pd.MultiIndex.from_tuples([(0, "p"), (1, "q")], names=["i", "j"]),
    ),
    "series.named": pd.Series([1.5, 2.5], name="metric"),
    "series.unnamed": pd.Series([1, 2, 3]),
    # Added by ITL-897 (review round 6): columns whose restored Arrow type is null hash as an
    # all-null large_string column; a decimal and an object column of None hash equal.
    "series.null_decimal": pd.Series(
        pd.array([None, None], dtype=pd.ArrowDtype(pa.decimal128(5, 2)))
    ),
    "dataframe.null_object": pd.DataFrame({"c": pd.Series([None, None], dtype=object)}),
    "series.null_arrow_null": pd.Series(
        pd.array([None, None], dtype=pd.ArrowDtype(pa.null()))
    ),
    "series.null_arrow_large_string": pd.Series(
        pd.array([None, None], dtype=pd.ArrowDtype(pa.large_string()))
    ),
}


@case("semantic", "value")
def semantic_values() -> dict[str, Any]:
    """The values of every type but the provisional library value types."""
    hasher = _ctx().semantic_hasher
    return {
        k: hasher.hash_object(v).to_string()
        for k, v in SEMANTIC_VALUES.items()
        if not provisional_types_of(v)
    }


@case("semantic", "value", provisional=NUMPY_PANDAS_TYPES)
def semantic_library_values() -> dict[str, Any]:
    """The NumPy and pandas values of ``semantic:value``: provisional (ITL-1324)."""
    hasher = _ctx().semantic_hasher
    return {
        k: hasher.hash_object(v).to_string()
        for k, v in SEMANTIC_VALUES.items()
        if provisional_types_of(v)
    }


def golden_fn_plain(a: int, b: str) -> float:
    """Function hashed as a value by the semantic group (ITL-953). DO NOT MOVE."""
    return float(a)


def golden_fn_defaults(
    a: int,
    b: str = "x",
    c: int | None = None,
    d: tuple[int, ...] = (1, 2),
    e: frozenset[str] = frozenset({"p", "q", "r"}),
    f: dict[str, list[int]] = {"k": [1, 2], "j": []},  # noqa: B006  (never mutated)
) -> float:
    """Function with default values, hashed as a value (ITL-953). DO NOT MOVE."""
    return float(a)


def golden_fn_kinds(a: int, /, b: int, *, c: int = 0) -> int:
    """Function with every parameter kind, hashed as a value (ITL-953). DO NOT MOVE."""
    return a + b + c


def golden_scaled(age: int, factor: float = 2.0) -> float:
    """Data function with a default, pinned by its stored signature hash. DO NOT MOVE."""
    return age * factor


def golden_stats2(a: int, b: int) -> tuple[int, int]:
    """Two outputs of one type: the order of the output keys is in the stored signature."""
    return a + b, a * b


def golden_stats3(a: int, b: int) -> tuple[int, str, float]:
    """Three outputs of three types."""
    return a + b, f"{a}-{b}", a / 4


@case("semantic", "function")
def semantic_functions() -> dict[str, Any]:
    """Pin the hash of a function used as a value and the stored signature hash.

    A default value enters as the semantic hasher's hash of the value and each
    parameter carries its kind (ITL-953).

    Returns:
        The function-value hashes, and the ``function_signature_hash`` a data
        function stores with every result row.
    """
    from orcapod.core.data_function import PythonDataFunction

    hasher = _ctx().semantic_hasher
    out: dict[str, Any] = {
        "value.plain": hasher.hash_object(golden_fn_plain).to_string(),
        "value.defaults": hasher.hash_object(golden_fn_defaults).to_string(),
        "value.kinds": hasher.hash_object(golden_fn_kinds).to_string(),
    }
    data_functions = {
        "ratio": golden_ratio.pod.data_function,
        "scaled": PythonDataFunction(golden_scaled, output_keys="scaled"),
    }
    for name, data_function in data_functions.items():
        variation = data_function.get_function_variation_data()
        out[f"stored_signature.{name}"] = _bytes_text(
            variation["function_signature_hash"]
        )
        # ITL-1256: the function as written, from its def line on.
        out[f"stored_content.{name}"] = _bytes_text(variation["function_content_hash"])
    out.update(_stored_record_cases())
    return out


def _stored_record_cases() -> dict[str, Any]:
    """The record of python.function.v1 rev1 (ITL-1256) in more declarations.

    Output keys in two orders (two and three keys), an ``input_schema=`` override, the
    three ways of making a pod, and a function whose source cannot be read (its content
    hash is absent: ``None``).
    """
    from orcapod import function_pod
    from orcapod.core.data_function import PythonDataFunction
    from orcapod.core.function_pod import FunctionPod

    def signature(data_function: Any) -> str:
        return _bytes_text(
            data_function.get_function_variation_data()["function_signature_hash"]
        )

    out: dict[str, Any] = {
        "stored_signature.stats2.forward": signature(
            PythonDataFunction(golden_stats2, output_keys=["sum", "product"])
        ),
        "stored_signature.stats2.backward": signature(
            PythonDataFunction(golden_stats2, output_keys=["product", "sum"])
        ),
        "stored_signature.stats3.forward": signature(
            PythonDataFunction(golden_stats3, output_keys=["total", "label", "quarter"])
        ),
        "stored_signature.stats3.rotated": signature(
            PythonDataFunction(golden_stats3, output_keys=["label", "quarter", "total"])
        ),
        "stored_signature.scaled.input_schema": signature(
            PythonDataFunction(
                golden_scaled, output_keys="scaled", input_schema={"age": float}
            )
        ),
        "stored_signature.stats2.from_fn": signature(
            FunctionPod.from_fn(
                golden_stats2, output_keys=["sum", "product"]
            ).data_function
        ),
        "stored_signature.stats2.decorator": signature(
            function_pod(output_keys=["sum", "product"])(
                golden_stats2
            ).pod.data_function
        ),
    }
    namespace: dict[str, Any] = {"__name__": "golden_repl"}
    exec("def typed_in(x: int) -> int:\n    return x + 1\n", namespace)
    without_source = PythonDataFunction(namespace["typed_in"], output_keys="y")
    variation = without_source.get_function_variation_data()
    out["stored_signature.without_source"] = _bytes_text(
        variation["function_signature_hash"]
    )
    out["stored_content.without_source"] = variation["function_content_hash"]
    return out


def _semantic_types() -> dict[str, Any]:
    return {
        "int": int,
        "float": float,
        "str": str,
        "bool": bool,
        "bytes": bytes,
        "optional_int": int | None,
        "list_int": list[int],
        "list_list_int": list[list[int]],
        "dict_str_int": dict[str, int],
        "tuple_int_str": tuple[int, str],
        "path": Path,
        "uuid": uuid.UUID,
        "ndarray": np.ndarray,
        "dataframe": pd.DataFrame,
        "list_path": list[Path],
        "set_uuid": set[uuid.UUID],
        "dataclass": GoldenPoint,
        "pydantic": GoldenModel,
        "structural": GoldenDeclared,
        "pydantic_optional": GoldenDeclaredModel,
        "structural_nested": GoldenOuter,
        "structural_with_array": GoldenWithArray,
        "structural_twin_ab": GoldenTwinAB,
        "structural_twin_ba": GoldenTwinBA,
        "structural_same_identity": GoldenDeclaredAgain,
        "dataclass_subclass": GoldenChild,
        "dataclass_with_structural": GoldenHolder,
        "list_dataclass": list[GoldenPoint],
        # Added by ITL-611: a set of a primitive is the native set container.
        "set_int": set[int],
        "set_str": set[str],
        "set_bytes": set[bytes],
    }


@case("semantic", "type")
def semantic_types() -> dict[str, Any]:
    """Every type but the provisional library value types."""
    register_logical_classes()
    hasher = _ctx().semantic_hasher
    return {
        k: hasher.hash_object(t).to_string()
        for k, t in _semantic_types().items()
        if not provisional_types_of(t)
    }


@case("semantic", "type", provisional=NUMPY_PANDAS_TYPES)
def semantic_library_types() -> dict[str, Any]:
    """The NumPy and pandas types of ``semantic:type``: provisional (ITL-1324)."""
    register_logical_classes()
    hasher = _ctx().semantic_hasher
    return {
        k: hasher.hash_object(t).to_string()
        for k, t in _semantic_types().items()
        if provisional_types_of(t)
    }


# ---------------------------------------------------------------------------
# Group: schema — Schema hashes, Arrow types, schema-hash helpers, SourceStreamBuilder
# ---------------------------------------------------------------------------


def _arrow_type_text(arrow_type: pa.DataType) -> str:
    """Render an Arrow type as stored: extension name plus storage type, not pyarrow's repr."""
    if isinstance(arrow_type, pa.ExtensionType):
        return (
            f"extension<{arrow_type.extension_name}>"
            f"[{_arrow_type_text(arrow_type.storage_type)}]"
        )
    return str(arrow_type)


def _schema_single_field(library: bool) -> dict[str, Any]:
    """``schema:single_field`` of the provisional library value types (``library``) or of
    every other type."""
    from orcapod.types import Schema

    register_logical_classes()
    ctx = _ctx()
    out: dict[str, Any] = {}
    for name, tp in _semantic_types().items():
        if bool(provisional_types_of(tp)) != library:
            continue
        out[f"{name}.arrow_type"] = _arrow_type_text(
            ctx.type_converter.python_type_to_arrow_type(tp)
        )
        out[f"{name}.schema_hash"] = ctx.semantic_hasher.hash_object(
            Schema({"c": tp})
        ).to_string()
    return out


@case("schema", "single_field")
def schema_single_field() -> dict[str, Any]:
    """Every type but the provisional library value types."""
    return _schema_single_field(library=False)


@case("schema", "single_field", provisional=NUMPY_PANDAS_TYPES)
def schema_library_single_field() -> dict[str, Any]:
    """The NumPy and pandas types of ``schema:single_field``: provisional (ITL-1324)."""
    return _schema_single_field(library=True)


@case("schema", "multi_field")
def schema_multi_field() -> dict[str, Any]:
    from orcapod.types import Schema

    register_logical_classes()
    hasher = _ctx().semantic_hasher
    schemas = {
        "two_scalars": Schema({"a": int, "b": str}),
        "two_scalars_reordered": Schema({"b": str, "a": int}),
        "optional_field": Schema({"a": int, "b": str}, optional_fields=["b"]),
        "nullable_type": Schema({"a": int, "b": str | None}),
        "logical_mix": Schema(
            {"p": Path, "u": uuid.UUID, "pt": GoldenPoint, "m": GoldenModel}
        ),
        "empty": Schema({}),
    }
    return {k: hasher.hash_object(s).to_string() for k, s in schemas.items()}


def _tag_data_schemas() -> tuple[Any, Any]:
    from orcapod.types import Schema

    return Schema({"subject": str, "session": int}), Schema(
        {"weight": float, "image": Path}
    )


@case("schema", "compute_schema_hash")
def schema_compute_schema_hash() -> dict[str, Any]:
    from orcapod.utils.schema_utils import compute_schema_hash

    tag_schema, data_schema = _tag_data_schemas()
    hasher = _ctx().semantic_hasher
    return {
        "full": compute_schema_hash(tag_schema, data_schema, hasher, None),
        "chars_16": compute_schema_hash(tag_schema, data_schema, hasher, 16),
    }


@case("schema", "source_schema_hash")
def schema_source_schema_hash() -> dict[str, Any]:
    from orcapod import DEFAULT_CONFIG
    from orcapod.utils.arrow_utils import system_tag_column_names
    from orcapod.utils.schema_utils import compute_source_schema_hash

    tag_schema, data_schema = _tag_data_schemas()
    schema_hash = compute_source_schema_hash(
        tag_schema, data_schema, _ctx(), DEFAULT_CONFIG
    )
    source_id_col, record_id_col = system_tag_column_names(schema_hash)
    return {
        "schema_hash": schema_hash,
        "source_id_column": source_id_col,
        "record_id_column": record_id_col,
    }


def _builder_table() -> pa.Table:
    return pa.table(
        {
            "subject": pa.array(["s1", "s2", "s3"], pa.large_string()),
            "weight": pa.array([20.5, 22.25, 19.0], pa.float64()),
            "count": pa.array([1, 2, 3], pa.int32()),
        },
        schema=pa.schema(
            [
                pa.field("subject", pa.large_string(), nullable=False),
                pa.field("weight", pa.float64(), nullable=False),
                pa.field("count", pa.int32(), nullable=True),
            ]
        ),
    )


@case("schema", "source_stream_builder")
def schema_source_stream_builder() -> dict[str, Any]:
    # SourceStreamBuilder is not public API; the issue names it as a pinned call site.
    from orcapod import DEFAULT_CONFIG
    from orcapod.core.sources.stream_builder import SourceStreamBuilder

    builder = SourceStreamBuilder(_ctx(), DEFAULT_CONFIG)
    out: dict[str, Any] = {}
    variants: dict[str, dict[str, Any]] = {
        "default_source_id": {"source_kind": "arrow_table"},
        "named_source_id": {"source_id": "golden_source"},
        "record_id_column": {
            "source_id": "golden_source",
            "record_id_columns": "subject",
        },
    }
    for variant, kwargs in variants.items():
        result = builder.build(_builder_table(), tag_columns=["subject"], **kwargs)
        out[f"{variant}.schema_hash"] = result.schema_hash
        out[f"{variant}.table_hash"] = result.table_hash.to_string()
        out[f"{variant}.source_id"] = result.source_id
        out[f"{variant}.system_tag_columns"] = list(result.system_tag_columns)
        out.update(_prefixed(variant, stream_summary(result.stream)))
    return out


@case("schema", "row_identifiers")
def schema_row_identifiers() -> dict[str, Any]:
    """Pin the row identifier forms of the record id (ITL-1154).

    Position (``row_<n>``), and the canonical JSON of the key columns: one string key,
    a composite key (string, then integer, in key order), and timestamp, date and
    boolean keys. Each variant's stream summary pins the record ids and the
    source-info tokens built on them.

    Returns:
        Each variant's stream summary.
    """
    import datetime as dt

    from orcapod.core.sources import ArrowTableSource

    table = pa.table(
        {
            "subject": pa.array(["s2", "s1"], pa.large_string()),
            "trial": pa.array([7, 3], pa.int64()),
            "start": pa.array(
                [dt.datetime(2026, 1, 2, 3, 4, 5, 6), dt.datetime(1969, 12, 31)],
                pa.timestamp("us"),
            ),
            "day": pa.array([dt.date(2026, 1, 2), dt.date(2000, 2, 29)], pa.date32()),
            "ok": pa.array([True, False], pa.bool_()),
            "value": pa.array([1.5, 2.5], pa.float64()),
        }
    )
    out: dict[str, Any] = {}
    for variant, key in {
        "position": None,
        "string_key": "subject",
        "composite_key": ["subject", "trial"],
        "timestamp_key": "start",
        "date_key": "day",
        "bool_key": "ok",
    }.items():
        source = ArrowTableSource(
            table,
            tag_columns=["subject"],
            record_id_columns=key,
            source_id="golden_rows",
            data_context=_ctx(),
            tag_only_columns=True,
        )
        out.update(_prefixed(variant, stream_summary(source)))
    return out


# Rows of the hash_in_key_order case: two tag columns, no single one a key, given out
# of key order for both key orders, which sort them differently.
KEY_ORDER_ROWS = [
    {"subject": "s2", "trial": 1, "weight": 19.0},
    {"subject": "s1", "trial": 2, "weight": 22.25},
    {"subject": "s2", "trial": 2, "weight": 21.75},
    {"subject": "s1", "trial": 1, "weight": 20.5},
]
# Rows whose two key columns rise together: both key orders sort them into one table.
ALIGNED_KEY_ROWS = [
    {"subject": "s2", "trial": 2, "weight": 22.25},
    {"subject": "s3", "trial": 3, "weight": 19.0},
    {"subject": "s1", "trial": 1, "weight": 20.5},
]


@case("schema", "hash_in_key_order")
def schema_hash_in_key_order() -> dict[str, Any]:
    """Pin default source ids hashed with ``hash_in_key_order`` (ITL-1272).

    For each in-memory source kind, over rows given out of key order: with
    ``hash_in_key_order=True``, both orders of the two key columns (two values), each in
    two row orders (``given`` and ``reversed``, one value); without it, the two row
    orders (two values). Pinned: the default source id and the content hash, and the
    builder's table hash for the same variants.

    Returns:
        Each variant's source id, content hash and table hash.
    """
    from orcapod import DEFAULT_CONFIG
    from orcapod.core.sources import (
        ArrowTableSource,
        DataFrameSource,
        DictSource,
        ListSource,
    )
    from orcapod.core.sources.stream_builder import SourceStreamBuilder

    tags = ["subject", "trial"]
    row_orders = {"given": KEY_ORDER_ROWS, "reversed": KEY_ORDER_ROWS[::-1]}
    schema = pa.schema(
        [
            pa.field("subject", pa.large_string(), nullable=False),
            pa.field("trial", pa.int64(), nullable=False),
            pa.field("weight", pa.float64(), nullable=False),
        ]
    )

    def build(kind: str, rows: list[dict[str, Any]], **kw: Any) -> Any:
        if kind == "arrow_table":
            return ArrowTableSource(
                pa.Table.from_pylist(rows, schema=schema),
                tag_columns=tags,
                tag_only_columns=True,
                data_context=_ctx(),
                **kw,
            )
        if kind == "dict":
            return DictSource(
                rows, tag_columns=tags, tag_only_columns=True, data_context=_ctx(), **kw
            )
        if kind == "data_frame":
            import polars as pl

            return DataFrameSource(
                pl.DataFrame(rows),
                tag_columns=tags,
                tag_only_columns=True,
                data_context=_ctx(),
                **kw,
            )
        by_weight = {row["weight"]: row for row in rows}
        return ListSource(
            "weight",
            [row["weight"] for row in rows],
            tag_function=lambda w, _i: {t: by_weight[w][t] for t in tags},
            expected_tag_keys=tags,
            data_context=_ctx(),
            **kw,
        )

    variants: dict[str, dict[str, Any]] = {
        "key_order.subject_trial": {
            "record_id_columns": ["subject", "trial"],
            "hash_in_key_order": True,
        },
        "key_order.trial_subject": {
            "record_id_columns": ["trial", "subject"],
            "hash_in_key_order": True,
        },
        "as_given.subject_trial": {"record_id_columns": ["subject", "trial"]},
    }
    out: dict[str, Any] = {}
    for kind in ("arrow_table", "dict", "data_frame", "list"):
        for variant, kwargs in variants.items():
            for order, rows in row_orders.items():
                source = build(kind, rows, **kwargs)
                prefix = f"{kind}.{variant}.{order}"
                out[f"{prefix}.source_id"] = source.source_id
                out[f"{prefix}.content_hash"] = source.content_hash().to_string()
        for key_name, key in (
            ("subject_trial", ["subject", "trial"]),
            ("trial_subject", ["trial", "subject"]),
        ):
            source = build(
                kind, ALIGNED_KEY_ROWS, record_id_columns=key, hash_in_key_order=True
            )
            prefix = f"{kind}.aligned.{key_name}"
            out[f"{prefix}.source_id"] = source.source_id
            out[f"{prefix}.content_hash"] = source.content_hash().to_string()
    builder = SourceStreamBuilder(_ctx(), DEFAULT_CONFIG)
    for variant, kwargs in variants.items():
        for order, rows in row_orders.items():
            result = builder.build(
                pa.Table.from_pylist(rows, schema=schema),
                tag_columns=tags,
                source_kind="arrow_table",
                **kwargs,
            )
            out[f"builder.{variant}.{order}.table_hash"] = result.table_hash.to_string()
    return out


class GoldenPollingImpl:
    """A polling source implementation whose identity is fixed (default source id).

    Only ``identity`` is used by the golden case; the source is never polled.
    """

    def identity(self) -> Any:
        return ("golden_polling", 1)

    def to_config(self) -> dict[str, Any] | None:
        return None

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> GoldenPollingImpl:
        raise NotImplementedError

    def schema(self) -> Any:
        return None

    async def poll(self, cursor: Any = None) -> bool:
        return False

    async def fetch(self, cursor: Any = None) -> Any:
        raise NotImplementedError

    async def close(self) -> None:
        return None


@case("schema", "location_default_source_ids")
def schema_location_default_source_ids() -> dict[str, Any]:
    """Pin the default source id of every source built from a location (ITL-1154).

    Each source reads ``_builder_table()``'s rows from a fixed relative location in a
    temporary directory (``rows.csv``, a Delta folder ``golden_delta``, a SQLite table
    ``golden_trials``), so no value depends on the machine. Pinned: the default
    ``<kind>:<location>`` and the stream summary (record ids are ``row_<n>``, and the
    primary key's JSON for SQLite). The polling source's default is the semantic hash of its
    implementation's identity.

    Returns:
        Each source's default source id and stream summary.
    """
    import os
    import sqlite3
    import tempfile

    from deltalake import write_deltalake

    from orcapod.core.sources import (
        CSVSource,
        DeltaTableSource,
        PollingSource,
        SQLiteTableSource,
    )

    table = _builder_table()
    out: dict[str, Any] = {}
    previous = os.getcwd()
    with tempfile.TemporaryDirectory() as tmp:
        os.chdir(tmp)
        try:
            pa_csv.write_csv(table, "rows.csv")
            write_deltalake("golden_delta", table)
            # ``with conn`` commits; ``closing`` closes the file before the
            # temporary directory is removed (Windows refuses an open file).
            with contextlib.closing(sqlite3.connect("golden.sqlite")) as conn, conn:
                conn.execute(
                    "CREATE TABLE golden_trials "
                    "(subject TEXT PRIMARY KEY, weight REAL NOT NULL, count INTEGER)"
                )
                conn.executemany(
                    "INSERT INTO golden_trials VALUES (?, ?, ?)",
                    list(zip(*table.to_pydict().values())),
                )
            sources = {
                "csv": CSVSource(
                    "rows.csv", tag_columns="subject", tag_only_columns=True
                ),
                "delta_table": DeltaTableSource(
                    "golden_delta", tag_columns="subject", tag_only_columns=True
                ),
                "sqlite_table": SQLiteTableSource(
                    "golden.sqlite", "golden_trials", tag_only_columns=True
                ),
            }
            for kind, source in sources.items():
                out[f"{kind}.source_id"] = source.source_id
                out.update(_prefixed(kind, stream_summary(source)))
        finally:
            os.chdir(previous)
    polling = PollingSource(
        GoldenPollingImpl(), tag_columns="subject", record_id_columns="subject"
    )
    out["polling_source.source_id"] = polling.source_id
    return out


@case("schema", "derived_source_tags_as_data")
def schema_derived_source_tags_as_data() -> dict[str, Any]:
    """Pin a source derived from another source with a tag also offered as data (ITL-939).

    Three variants: the default source id (``tags_as_data:<label>:<hex>``, the derivation
    hash), an explicit source id, and the default source id with a ``record_id_column``
    (which enters the derivation hash).

    Returns:
        Each variant's source id and stream summary.
    """
    from orcapod.core.sources import ArrowTableSource, DerivedSource

    origin = ArrowTableSource(
        _builder_table(),
        tag_columns=["subject"],
        tag_only_columns=True,
        source_id="golden_source",
        data_context=_ctx(),
    )
    out: dict[str, Any] = {}
    for variant, kwargs in {
        "default_source_id": {},
        "named_source_id": {"source_id": "golden_derived"},
        "record_id_column": {"record_id_columns": "subject"},
    }.items():
        derived = DerivedSource(
            origin, tags_as_data={"subject": "subject_value"}, **kwargs
        )
        out[f"{variant}.source_id"] = derived.source_id
        out.update(_prefixed(variant, stream_summary(derived)))
    return out


@case("schema", "key_policy_identity")
def schema_key_policy_identity() -> dict[str, Any]:
    """Pin sources that differ only in key policy (developer, November 16, 2026, ITL-1154).

    A root source's identity holds its resolved key columns (``()`` for the
    position), so two sources with one ``source_id`` and schema that name their rows
    differently have different content hashes and the same pipeline hash. Pinned per
    source kind, for two key policies each: the content hash, and the pipeline hash
    where the source has a schema without polling.

    Returns:
        Each variant's content hash and pipeline hash.
    """
    import sqlite3
    import tempfile

    from orcapod.core.sources import (
        ArrowTableSource,
        DerivedSource,
        ListSource,
        PollingSource,
        SQLiteTableSource,
    )

    table = _builder_table()
    out: dict[str, Any] = {}

    def pin(variant: str, source: Any, *, pipeline: bool = True) -> None:
        out[f"{variant}.content_hash"] = source.content_hash().to_string()
        if pipeline:
            out[f"{variant}.pipeline_hash"] = source.pipeline_hash().to_string()

    for variant, key in {"position": None, "key": "subject"}.items():
        arrow = ArrowTableSource(
            table,
            tag_columns=["subject"],
            record_id_columns=key,
            source_id="golden_keys",
            data_context=_ctx(),
            tag_only_columns=True,
        )
        pin(f"arrow_table.{variant}", arrow)
        derived = DerivedSource(
            arrow,
            tags_as_data={"subject": "subject_value"},
            source_id="golden_keys_derived",
            record_id_columns=key,
        )
        pin(f"derived_tags_as_data.{variant}", derived)
    for variant, key in {"position": None, "key": "element_index"}.items():
        listed = ListSource(
            "value",
            [10, 20, 30],
            source_id="golden_keys_list",
            record_id_columns=key,
            data_context=_ctx(),
        )
        pin(f"list.{variant}", listed)
    for variant, key in {
        "subject": "subject",
        "subject_count": ["subject", "count"],
    }.items():
        polling = PollingSource(
            GoldenPollingImpl(),
            tag_columns="subject",
            record_id_columns=key,
            source_id="golden_keys_polling",
        )
        pin(f"polling_source.{variant}", polling, pipeline=False)
    with tempfile.TemporaryDirectory() as tmp:
        path = f"{tmp}/golden.sqlite"
        with contextlib.closing(sqlite3.connect(path)) as conn, conn:
            conn.execute(
                "CREATE TABLE golden_trials "
                "(subject TEXT PRIMARY KEY, weight REAL NOT NULL, count INTEGER)"
            )
            conn.executemany(
                "INSERT INTO golden_trials VALUES (?, ?, ?)",
                list(zip(*table.to_pydict().values())),
            )
        for variant, key in {"primary_key": None, "position": ()}.items():
            sqlite = SQLiteTableSource(
                path,
                "golden_trials",
                record_id_columns=key,
                source_id="golden_keys_sqlite",
                tag_only_columns=True,
            )
            pin(f"sqlite_table.{variant}", sqlite)
    return out


# ---------------------------------------------------------------------------
# Group: arrow — StarfixArrowHasher table and schema hashes
# ---------------------------------------------------------------------------


class GoldenOpaqueType(pa.ExtensionType):
    """Extension type with no orcapod handler: exercises the passthrough leg."""

    def __init__(self) -> None:
        super().__init__(pa.int64(), "golden.opaque")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> GoldenOpaqueType:
        return cls()


def _arrow_tables() -> dict[str, pa.Table]:
    ints = [1, 2, 3]
    tables: dict[str, pa.Table] = {}
    for width in ("int8", "int16", "int32", "int64", "uint8"):
        tables[f"width.{width}"] = pa.table({"a": pa.array(ints, getattr(pa, width)())})
    tables["float.float32"] = pa.table({"a": pa.array([0.5, 1.5], pa.float32())})
    tables["float.float64"] = pa.table({"a": pa.array([0.5, 1.5], pa.float64())})
    tables["bool"] = pa.table({"a": pa.array([True, False, True], pa.bool_())})
    tables["layout.string"] = pa.table({"a": pa.array(["x", "yz"], pa.string())})
    tables["layout.large_string"] = pa.table(
        {"a": pa.array(["x", "yz"], pa.large_string())}
    )
    tables["layout.binary"] = pa.table({"a": pa.array([b"\x00", b"ab"], pa.binary())})
    tables["layout.large_binary"] = pa.table(
        {"a": pa.array([b"\x00", b"ab"], pa.large_binary())}
    )
    tables["layout.list"] = pa.table(
        {"a": pa.array([[1, 2], [3]], pa.list_(pa.int64()))}
    )
    tables["layout.large_list"] = pa.table(
        {"a": pa.array([[1, 2], [3]], pa.large_list(pa.int64()))}
    )
    tables["nulls.int64"] = pa.table({"a": pa.array([1, None, 3], pa.int64())})
    tables["nulls.string"] = pa.table(
        {"a": pa.array(["x", None, "z"], pa.large_string())}
    )
    tables["nullability.nullable"] = pa.table(
        {"a": pa.array(ints, pa.int64())},
        schema=pa.schema([pa.field("a", pa.int64(), nullable=True)]),
    )
    tables["nullability.non_nullable"] = pa.table(
        {"a": pa.array(ints, pa.int64())},
        schema=pa.schema([pa.field("a", pa.int64(), nullable=False)]),
    )
    tables["nested.list_list"] = pa.table(
        {"a": pa.array([[[1], [2, 3]], []], pa.large_list(pa.large_list(pa.int64())))}
    )
    struct_type = pa.struct([("x", pa.int64()), ("y", pa.large_string())])
    tables["nested.struct"] = pa.table(
        {"a": pa.array([{"x": 1, "y": "p"}, {"x": 2, "y": "q"}], struct_type)}
    )
    tables["nested.list_struct"] = pa.table(
        {"a": pa.array([[{"x": 1, "y": "p"}], []], pa.large_list(struct_type))}
    )
    col_a = pa.array(ints, pa.int64())
    col_b = pa.array(["x", "y", "z"], pa.large_string())
    tables["order.ab"] = pa.Table.from_arrays([col_a, col_b], names=["a", "b"])
    tables["order.ba"] = pa.Table.from_arrays([col_b, col_a], names=["b", "a"])
    tables["order.rows_reversed"] = pa.table(
        {
            "a": pa.array(ints[::-1], pa.int64()),
            "b": pa.array(["z", "y", "x"], pa.large_string()),
        }
    )
    tables["empty.zero_rows"] = pa.table({"a": pa.array([], pa.int64())})
    # Added with starfix hash format 2 (ITL-910): inputs format 1 could not hash or
    # hashed alike (ITL-848, ITL-927).
    tables["fixed_size_list.float32_2"] = pa.table(
        {"a": pa.array([[0.5, 1.5], [2.5, 3.5]], pa.list_(pa.float32(), 2))}
    )
    tables["fixed_size_list.null_slot"] = pa.table(
        {"a": pa.array([[1, 2], None], pa.list_(pa.int64(), 2))}
    )
    list3 = pa.large_list(pa.large_list(pa.large_list(pa.int64())))
    tables["collision.list_lengths_a"] = pa.table(
        {"a": pa.array([[[[1]], [[2], [3]]]], list3)}
    )
    tables["collision.list_lengths_b"] = pa.table(
        {"a": pa.array([[[[1], [2]], [[3]]]], list3)}
    )
    tables["collision.null_struct"] = pa.table(
        {"a": pa.array([None], pa.struct([("x", pa.int64())]))}
    )
    tables["collision.struct_of_nulls"] = pa.table(
        {"a": pa.array([{"x": None}], pa.struct([("x", pa.int64())]))}
    )
    tables["bool.chunked"] = pa.Table.from_batches(
        tables["bool"].to_batches(max_chunksize=1)
    )
    # Added by ITL-942 (finding I): nested nullability is part of the hash whether or
    # not the column holds data.
    non_null_struct = pa.struct([pa.field("x", pa.int64(), nullable=False)])
    nullable_struct = pa.struct([pa.field("x", pa.int64(), nullable=True)])
    non_null_list = pa.large_list(pa.field("item", pa.int64(), nullable=False))
    for label, struct_t, list_t in (
        ("non_null", non_null_struct, non_null_list),
        ("nullable", nullable_struct, pa.large_list(pa.int64())),
    ):
        tables[f"nested_nullability.struct_{label}.data"] = pa.table(
            {"a": pa.array([{"x": 1}], struct_t)}
        )
        tables[f"nested_nullability.struct_{label}.zero_rows"] = pa.table(
            {"a": pa.array([], struct_t)}
        )
        tables[f"nested_nullability.struct_{label}.all_null"] = pa.table(
            {"a": pa.array([None], struct_t)}
        )
        tables[f"nested_nullability.list_{label}.data"] = pa.table(
            {"a": pa.array([[1]], list_t)}
        )
        tables[f"nested_nullability.list_{label}.zero_rows"] = pa.table(
            {"a": pa.array([], list_t)}
        )
    return tables


def _extension_tables() -> dict[str, pa.Table]:
    from orcapod.types import Schema

    tc = _ctx().type_converter
    rows = {
        "path": [{"c": PATH_A}, {"c": PATH_B}],
        "uuid": [{"c": UUID_A}, {"c": UUID_B}],
    }
    types = {"path": Path, "uuid": uuid.UUID}
    tables = {
        f"extension.{name}": tc.python_dicts_to_arrow_table(
            rows[name], python_schema=Schema({"c": types[name]})
        )
        for name in rows
    }
    try:
        # The pyarrow stubs name PyExtensionType, but ExtensionType subclasses are the
        # documented way to register a custom extension type.
        pa.register_extension_type(GoldenOpaqueType())  # pyright: ignore[reportArgumentType]
    except pa.ArrowKeyError:
        pass  # already registered in this process
    storage = pa.array([1, 2, 3], pa.int64())
    tables["extension.unregistered_passthrough"] = pa.table(
        {"c": pa.ExtensionArray.from_storage(GoldenOpaqueType(), storage)}
    )
    # pyarrow's canonical extension types, hashable with their identity since ITL-910.
    tables["extension.canonical_uuid"] = pa.table(
        {
            "c": pa.ExtensionArray.from_storage(
                pa.uuid(), pa.array([UUID_A.bytes, UUID_B.bytes], pa.binary(16))
            )
        }
    )
    tables["extension.canonical_fixed_shape_tensor"] = pa.table(
        {
            "c": pa.ExtensionArray.from_storage(
                pa.fixed_shape_tensor(pa.float32(), [2]),
                pa.array([[0.5, 1.5], [2.5, 3.5]], pa.list_(pa.float32(), 2)),
            )
        }
    )
    return tables


@case("arrow", "table")
def arrow_table_hashes() -> dict[str, Any]:
    hasher = _ctx().arrow_hasher
    return {k: hasher.hash_table(t).to_string() for k, t in _arrow_tables().items()}


@case("arrow", "extension_table")
def arrow_extension_table_hashes() -> dict[str, Any]:
    hasher = _ctx().arrow_hasher
    return {k: hasher.hash_table(t).to_string() for k, t in _extension_tables().items()}


@case("arrow", "schema")
def arrow_schema_hashes() -> dict[str, Any]:
    hasher = _ctx().arrow_hasher
    return {
        k: hasher.hash_schema(t.schema).to_string() for k, t in _arrow_tables().items()
    }


@case("arrow", "dict_order")
def arrow_dict_order_hashes() -> dict[str, Any]:
    """Pin that an Arrow table is hashed exactly as stored (ITL-905).

    A key/value list that arrives in Arrow form keeps its order as identity (sorted and
    unsorted pin different values), like any other list of structs.
    """
    hasher = _ctx().arrow_hasher
    kv = pa.large_list(
        pa.struct([pa.field("key", pa.large_string()), pa.field("value", pa.int64())])
    )
    pairs = pa.large_list(
        pa.struct([pa.field("f0", pa.large_string()), pa.field("f1", pa.int64())])
    )
    ab = [{"key": "a", "value": 1}, {"key": "b", "value": 2}]
    tables = {
        "kv.sorted": pa.table({"m": pa.array([ab], kv)}),
        "kv.unsorted": pa.table({"m": pa.array([ab[::-1]], kv)}),
        "pairs.ab": pa.table(
            {"m": pa.array([[{"f0": "a", "f1": 1}, {"f0": "b", "f1": 2}]], pairs)}
        ),
        "pairs.ba": pa.table(
            {"m": pa.array([[{"f0": "b", "f1": 2}, {"f0": "a", "f1": 1}]], pairs)}
        ),
    }
    return {k: hasher.hash_table(t).to_string() for k, t in tables.items()}


@case("arrow", "extension_schema")
def arrow_extension_schema_hashes() -> dict[str, Any]:
    """Schema hashes of extension columns, hashed with their identity (ITL-910, ITL-846)."""
    hasher = _ctx().arrow_hasher
    return {
        k: hasher.hash_schema(t.schema).to_string()
        for k, t in _extension_tables().items()
    }


# ---------------------------------------------------------------------------
# Group: datagram — public sources carrying each logical type
# ---------------------------------------------------------------------------


def _datagram_rows() -> dict[str, list[dict[str, Any]]]:
    return {
        "scalars": [
            {"k": "r1", "i": 1, "f": 0.5, "s": "x", "b": True, "raw": b"\x00"},
            {"k": "r2", "i": -2, "f": 1.5, "s": "yz", "b": False, "raw": b"\xff"},
        ],
        "optional": [{"k": "r1", "v": 1}, {"k": "r2", "v": None}],
        "nested": [
            {"k": "r1", "li": [1, 2], "lli": [[1], [2, 3]], "d": {"a": 1}},
            {"k": "r2", "li": [], "lli": [[]], "d": {"b": 2, "c": 3}},
        ],
        "path": [{"k": "r1", "c": PATH_A}, {"k": "r2", "c": PATH_B}],
        "uuid": [{"k": "r1", "c": UUID_A}, {"k": "r2", "c": UUID_B}],
        "ndarray": [
            {"k": "r1", "c": np.arange(4, dtype=np.int64)},
            {"k": "r2", "c": np.array([[0.5, 1.5]], dtype=np.float64)},
        ],
        "dataframe": [
            {"k": "r1", "c": pd.DataFrame({"a": [1, 2]})},
            {"k": "r2", "c": pd.DataFrame({"a": [3], "b": ["x"]})},
        ],
        "dataclass": [
            {"k": "r1", "c": GoldenPoint(1, 0.5)},
            {"k": "r2", "c": GoldenPoint(2, 1.5)},
        ],
        "pydantic": [
            {"k": "r1", "c": GoldenModel(name="a", count=1)},
            {"k": "r2", "c": GoldenModel(name="b", count=2)},
        ],
        "list_path": [{"k": "r1", "c": [PATH_A, PATH_B]}, {"k": "r2", "c": []}],
        "set_uuid": [{"k": "r1", "c": {UUID_A, UUID_B}}, {"k": "r2", "c": {UUID_A}}],
        "structural": [
            {"k": "r1", "c": GoldenDeclared("a", 0.5)},
            {"k": "r2", "c": GoldenDeclared("b", None)},
        ],
        "pydantic_optional": [
            {"k": "r1", "c": GoldenDeclaredModel(label="a", score=0.5)},
            {"k": "r2", "c": GoldenDeclaredModel(label="b", score=None)},
        ],
        "structural_nested": [
            {"k": "r1", "c": GoldenOuter(GoldenInner(1), ["x", "y"])},
            {"k": "r2", "c": GoldenOuter(GoldenInner(2), [])},
        ],
        "structural_with_array": [
            {"k": "r1", "c": GoldenWithArray("a", np.arange(3, dtype=np.float32))},
            {"k": "r2", "c": GoldenWithArray("b", np.zeros((2, 2), dtype=np.int64))},
        ],
        "structural_twin_ab": [
            {"k": "r1", "c": GoldenTwinAB(1, "x")},
            {"k": "r2", "c": GoldenTwinAB(2, "y")},
        ],
        "structural_twin_ba": [
            {"k": "r1", "c": GoldenTwinBA("x", 1)},
            {"k": "r2", "c": GoldenTwinBA("y", 2)},
        ],
        "structural_same_identity": [
            {"k": "r1", "c": GoldenDeclaredAgain("a", 0.5)},
            {"k": "r2", "c": GoldenDeclaredAgain("b", None)},
        ],
        "dataclass_subclass": [
            {"k": "r1", "c": GoldenChild(1, 0.5, 7)},
            {"k": "r2", "c": GoldenChild(2, 1.5)},
        ],
        "dataclass_with_structural": [
            {"k": "r1", "c": GoldenHolder(GoldenInner(1), "a")},
            {"k": "r2", "c": GoldenHolder(GoldenInner(2), "b")},
        ],
        # Added by ITL-611: a set of a primitive is the native set container.
        "set_int": [{"k": "r1", "c": {10, 2, 1}}, {"k": "r2", "c": {-1}}],
        "set_str": [{"k": "r1", "c": {"b", "a", "10"}}, {"k": "r2", "c": {"x"}}],
        "set_bytes": [{"k": "r1", "c": {b"y", b"\x00"}}, {"k": "r2", "c": {b"x"}}],
    }


def _make_datagram_case(name: str) -> CaseFn:
    def run() -> dict[str, Any]:
        from orcapod.sources import DictSource

        source = DictSource(
            data=_datagram_rows()[name],
            tag_columns=["k"],
            tag_only_columns=True,
            source_id=f"golden_{name}",
        )
        return stream_summary(source)

    return run


# A datagram case whose rows hold a provisional library value type, or a user class
# embedding one, declares it (ITL-1324).
for _name, _rows in _datagram_rows().items():
    case("datagram", _name, provisional=tuple(sorted(provisional_types_of(_rows))))(
        _make_datagram_case(_name)
    )


def _arrow_source(column: pa.Array[Any], nullable: bool = False) -> Any:
    from orcapod.sources import ArrowTableSource

    table = pa.table(
        {"k": pa.array(["r1", "r2", "r3"], pa.large_string()), "c": column},
        schema=pa.schema(
            [
                pa.field("k", pa.large_string(), nullable=False),
                pa.field("c", column.type, nullable=nullable),
            ]
        ),
    )
    return ArrowTableSource(
        table, tag_columns=["k"], tag_only_columns=True, source_id="golden_arrow"
    )


@case("datagram", "dict_order")
def datagram_dict_order() -> dict[str, Any]:
    """Pin equal hashes for rows holding equal dicts in different insertion orders (ITL-905)."""
    from orcapod.sources import DictSource

    rows: dict[str, tuple[Any, list[Any]]] = {
        "str_keys": (dict[str, int], [{"b": 2, "a": 1}, {"a": 1, "b": 2}]),
        "int_keys": (dict[int, str], [{10: "x", 9: "y"}, {9: "y", 10: "x"}]),
        "nested": (
            dict[str, dict[str, int]],
            [
                {"y": {"d": 4, "c": 3}, "x": {"a": 1}},
                {"x": {"a": 1}, "y": {"c": 3, "d": 4}},
            ],
        ),
    }
    out: dict[str, Any] = {}
    for name, (value_type, values) in rows.items():
        source = DictSource(
            data=[{"k": f"r{i}", "m": v} for i, v in enumerate(values)],
            tag_columns=["k"],
            tag_only_columns=True,
            data_schema={"k": str, "m": value_type},
            source_id=f"golden_dict_{name}",
        )
        out.update(_prefixed(name, stream_summary(source)))
    return out


@case("datagram", "arrow_source")
def datagram_arrow_source() -> dict[str, Any]:
    ints = [1, 2, 3]
    columns = {
        "int8": pa.array(ints, pa.int8()),
        "int32": pa.array(ints, pa.int32()),
        "int64": pa.array(ints, pa.int64()),
        "string": pa.array(["x", "y", "z"], pa.string()),
        "large_string": pa.array(["x", "y", "z"], pa.large_string()),
    }
    out: dict[str, Any] = {}
    for name, column in columns.items():
        out.update(_prefixed(name, stream_summary(_arrow_source(column))))
    out.update(
        _prefixed(
            "int64_nullable",
            stream_summary(_arrow_source(pa.array(ints, pa.int64()), nullable=True)),
        )
    )
    return out


# ---------------------------------------------------------------------------
# Group: pipeline — end-to-end identity through the public interface
# ---------------------------------------------------------------------------


@function_pod(output_keys="ratio")
def golden_ratio(age: int, weight: float) -> float:
    """Function pod pinned by the pipeline group. DO NOT MOVE."""
    return weight / age


@function_pod(output_keys="adjusted")
def golden_adjust(age: int, weight: float, score: float) -> float:
    """Function pod pinned by the pipeline group. DO NOT MOVE."""
    return weight * score + age


SUBJECT_ROWS = [
    {"subject": "s1", "age": 3, "weight": 20.5},
    {"subject": "s2", "age": 4, "weight": 22.25},
    {"subject": "s3", "age": 5, "weight": 19.0},
]
SCAN_ROWS = [
    {"subject": "s1", "score": 0.5},
    {"subject": "s2", "score": 1.5},
]
GROUPED_ROWS = [
    {"cohort": "a", "subject": "s1", "weight": 20.5},
    {"cohort": "a", "subject": "s2", "weight": 22.25},
    {"cohort": "b", "subject": "s3", "weight": 19.0},
]


def _subjects() -> Any:
    from orcapod.sources import DictSource

    return DictSource(
        data=SUBJECT_ROWS,
        tag_columns=["subject"],
        tag_only_columns=True,
        source_id="subjects",
    )


def _scans() -> Any:
    from orcapod.sources import DictSource

    return DictSource(
        data=SCAN_ROWS,
        tag_columns=["subject"],
        tag_only_columns=True,
        source_id="scans",
    )


def _grouped() -> Any:
    from orcapod.sources import DictSource

    return DictSource(
        data=GROUPED_ROWS,
        tag_columns=["cohort", "subject"],
        tag_only_columns=True,
        source_id="cohorts",
    )


@case("pipeline", "sources")
def pipeline_sources() -> dict[str, Any]:
    out = _prefixed("subjects", stream_summary(_subjects()))
    out.update(_prefixed("scans", stream_summary(_scans())))
    out.update(_prefixed("cohorts", stream_summary(_grouped())))
    return out


@case("pipeline", "pod")
def pipeline_pod() -> dict[str, Any]:
    return stream_summary(golden_ratio.pod(_subjects()))


@case("pipeline", "pod_default_version")
def pipeline_pod_default_version() -> dict[str, Any]:
    """Pin one uri for one function whatever the entry point (ITL-942 finding N).

    ``FunctionPod.from_fn`` and ``@function_pod`` both default to version ``v0.0``,
    so the same function gets the same uri, and therefore the same table paths.

    Returns:
        The uri of the pod built each way.
    """
    from orcapod import FunctionPod

    built = FunctionPod.from_fn(
        golden_ratio,
        output_keys="ratio",
        name="golden_ratio",
    )
    return {"from_fn.uri": list(built.uri), "decorator.uri": list(golden_ratio.pod.uri)}


@case("pipeline", "join_pod")
def pipeline_join_pod() -> dict[str, Any]:
    from orcapod.operators import Join

    joined = Join()(_subjects(), _scans())
    out = _prefixed("join", stream_summary(joined))
    out.update(_prefixed("pod", stream_summary(golden_adjust.pod(joined))))
    return out


@case("pipeline", "merge_join", provisional=("MergeJoin",))
def pipeline_merge_join() -> dict[str, Any]:
    from orcapod.operators import MergeJoin, SelectDataColumns

    left = SelectDataColumns(columns=["weight"])(_subjects())
    from orcapod.sources import DictSource

    right = DictSource(
        data=[{"subject": "s1", "weight": 30.0}, {"subject": "s3", "weight": 31.5}],
        tag_columns=["subject"],
        tag_only_columns=True,
        source_id="reweigh",
    )
    return stream_summary(MergeJoin()(left, right))


@case("pipeline", "semi_join")
def pipeline_semi_join() -> dict[str, Any]:
    from orcapod.operators import SemiJoin

    return stream_summary(SemiJoin()(_subjects(), _scans()))


def _keyed(source_id: str, value: str, nullable_key: bool) -> Any:
    """A source on tag ``subject`` (``large_string``), nullable with a null if asked."""
    from orcapod.sources import ArrowTableSource

    subjects = ["s1", None, "s3"] if nullable_key else ["s1", "s2", "s3"]
    table = pa.table(
        {
            "subject": pa.array(subjects, pa.large_string()),
            value: pa.array([1, 2, 3], pa.int64()),
        },
        schema=pa.schema(
            [
                pa.field("subject", pa.large_string(), nullable=nullable_key),
                pa.field(value, pa.int64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(
        table, tag_columns=["subject"], tag_only_columns=True, source_id=source_id
    )


def _nullability_inputs() -> tuple[Callable[[], Any], Callable[[], Any]]:
    """The two inputs of ``join_key_nullability``: a nullable and a plain key."""

    def nullable() -> Any:
        return _keyed("golden_nullable_key", "x", nullable_key=True)

    def plain() -> Any:
        return _keyed("golden_plain_key", "y", nullable_key=False)

    return nullable, plain


@case("pipeline", "join_key_nullability", provisional=("MergeJoin",))
def pipeline_join_key_nullability_merge_join() -> dict[str, Any]:
    """The ``MergeJoin`` entries of ``join_key_nullability``: provisional (ITL-1279)."""
    from orcapod.operators import MergeJoin

    nullable, plain = _nullability_inputs()
    return _prefixed("merge_join", stream_summary(MergeJoin()(nullable(), plain())))


@case("pipeline", "join_key_nullability")
def pipeline_join_key_nullability() -> dict[str, Any]:
    """Pin the joins over a tag key that is ``T`` in one input and ``T | None`` in the other.

    Accepted since ITL-829 (it raised before, so nothing was stored): ``Join`` and
    ``MergeJoin`` give the key ``T``; ``SemiJoin`` keeps the left input's schema.
    """
    from orcapod.operators import Join, SemiJoin

    nullable, plain = _nullability_inputs()
    out = _prefixed("join", stream_summary(Join()(nullable(), plain())))
    out.update(
        _prefixed(
            "semi_join_left_nullable", stream_summary(SemiJoin()(nullable(), plain()))
        )
    )
    out.update(
        _prefixed(
            "semi_join_right_nullable", stream_summary(SemiJoin()(plain(), nullable()))
        )
    )
    return out


@case("pipeline", "batch", provisional=("Batch",))
def pipeline_batch() -> dict[str, Any]:
    from orcapod.operators import Batch

    return stream_summary(Batch(batch_size=2)(_subjects()))


@case("pipeline", "group_by", provisional=("GroupBy",))
def pipeline_group_by() -> dict[str, Any]:
    from orcapod.operators import GroupBy

    return stream_summary(GroupBy(by=["cohort"])(_grouped()))


@case("pipeline", "index_pick", provisional=("Index", "Pick"))
def pipeline_index_pick() -> dict[str, Any]:
    from orcapod.operators import Index, Pick
    from orcapod.sources import DictSource

    source = DictSource(
        data=[
            {"subject": "s1", "values": [1, 2, 3], "meta": {"k": 10, "j": 11}},
            {"subject": "s2", "values": [4, 5], "meta": {"k": 20, "j": 21}},
        ],
        tag_columns=["subject"],
        tag_only_columns=True,
        source_id="lists",
    )
    out = _prefixed("index", stream_summary(Index(column="values", i=0)(source)))
    out.update(_prefixed("pick", stream_summary(Pick(column="meta", key="k")(source))))
    return out


@case("pipeline", "map_data")
def pipeline_map_data() -> dict[str, Any]:
    from orcapod.operators import MapData

    return stream_summary(MapData(name_map={"age": "age_years"})(_subjects()))


@case("pipeline", "column_selection")
def pipeline_column_selection() -> dict[str, Any]:
    from orcapod.operators import (
        DropDataColumns,
        DropTagColumns,
        SelectDataColumns,
        SelectTagColumns,
    )

    out = _prefixed(
        "select_data", stream_summary(SelectDataColumns(columns=["age"])(_subjects()))
    )
    out.update(
        _prefixed(
            "drop_data", stream_summary(DropDataColumns(columns=["age"])(_subjects()))
        )
    )
    out.update(
        _prefixed(
            "select_tag",
            stream_summary(SelectTagColumns(columns=["cohort"])(_grouped())),
        )
    )
    out.update(
        _prefixed(
            "drop_tag", stream_summary(DropTagColumns(columns=["subject"])(_grouped()))
        )
    )
    # Added by ITL-942 (finding F): every spelling of one set of names has one
    # identity; these equal the sorted-list entries.
    for spelling, columns in {
        "tuple": ("weight", "age"),
        "set": {"weight", "age"},
        "unsorted": ["weight", "age"],
    }.items():
        stream = SelectDataColumns(columns=columns)(_subjects())
        out[f"select_data_spelling.{spelling}.pipeline_hash"] = (
            stream.pipeline_hash().to_string()
        )
    out["select_data_spelling.sorted.pipeline_hash"] = (
        SelectDataColumns(columns=["age", "weight"])(_subjects())
        .pipeline_hash()
        .to_string()
    )
    return out


@case("pipeline", "tag_and_data_columns")
def pipeline_tag_and_data_columns() -> dict[str, Any]:
    """``DropColumns`` and ``SelectColumns`` across tag and data columns (ITL-1084).

    ``stream_summary`` keys rows by user tags, so every stream pinned here keeps a
    tag whose values are unique per row.
    """
    from orcapod.operators import DropColumns, SelectColumns
    from orcapod.sources import DictSource

    cohort_subjects = DictSource(
        data=[
            {"cohort": "a", "subject": "s1", "age": 3, "weight": 20.5},
            {"cohort": "a", "subject": "s2", "age": 4, "weight": 22.25},
            {"cohort": "b", "subject": "s3", "age": 5, "weight": 19.0},
        ],
        tag_columns=["cohort", "subject"],
        tag_only_columns=True,
        source_id="cohort_subjects",
    )
    streams = {
        "drop": DropColumns(columns=["cohort", "age"])(cohort_subjects),
        "select": SelectColumns(columns=["subject", "weight"])(cohort_subjects),
        "select_tag_and_data": SelectColumns(columns=["subject", "age"])(_subjects()),
    }
    out: dict[str, Any] = {}
    for name, stream in streams.items():
        row_keys = [_row_key(tag.as_dict()) for tag, _ in stream.iter_data()]
        assert len(set(row_keys)) == len(row_keys) == 3, (name, row_keys)
        out.update(_prefixed(name, stream_summary(stream)))
    return out


@case("pipeline", "map_columns", provisional=("MapColumns",))
def pipeline_map_columns() -> dict[str, Any]:
    """``MapColumns``: tag and data columns renamed in one step (ITL-1160).

    ``stream_summary`` keys rows by user tags, so every stream pinned here keeps a
    tag whose values are unique per row.
    """
    from orcapod.operators import MapColumns
    from orcapod.sources import DictSource

    cohort_subjects = DictSource(
        data=[
            {"cohort": "a", "subject": "s1", "age": 3, "weight": 20.5},
            {"cohort": "a", "subject": "s2", "age": 4, "weight": 22.25},
            {"cohort": "b", "subject": "s3", "age": 5, "weight": 19.0},
        ],
        tag_columns=["cohort", "subject"],
        tag_only_columns=True,
        source_id="cohort_subjects",
    )
    streams = {
        "rename": MapColumns(name_map={"subject": "animal", "age": "age_years"})(
            cohort_subjects
        ),
        "drop_unmapped": MapColumns(
            name_map={"subject": "animal", "weight": "kg"}, drop_unmapped=True
        )(cohort_subjects),
    }
    out: dict[str, Any] = {}
    for name, stream in streams.items():
        row_keys = [_row_key(tag.as_dict()) for tag, _ in stream.iter_data()]
        assert len(set(row_keys)) == len(row_keys) == 3, (name, row_keys)
        out.update(_prefixed(name, stream_summary(stream)))
    return out


@case("pipeline", "polars_filter", provisional=("PolarsFilter",))
def pipeline_polars_filter() -> dict[str, Any]:
    from orcapod.operators import PolarsFilter

    return stream_summary(PolarsFilter(constraints={"cohort": "a"})(_grouped()))


@case("pipeline", "job")
def pipeline_job() -> dict[str, Any]:
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.operators import Join

    db = InMemoryArrowDatabase()
    job = PipelineJob(name="golden_job", database=db)
    with job:
        joined = Join(label="joined")(_subjects(), _scans())
        golden_adjust.pod(joined, label="adjust")
    job.run()

    out: dict[str, Any] = {}
    for label in sorted(job.nodes):
        out.update(_prefixed(f"node[{label}]", stream_summary(job.nodes[label])))

    paths = sorted(db.list_sources())
    out["table_paths"] = ["/".join(p) for p in paths]
    for path in paths:
        name = "/".join(path)
        records = db.get_all_records(path, record_id_column="__golden_record_id")
        assert records is not None
        if "__input_data_hash" in records.column_names and "_result" in path:
            # Result-table record ids are UUIDv7 (volatile); pin the lookup key instead.
            out[f"table[{name}].input_data_hashes"] = sorted(
                _jsonable(v) for v in records.column("__input_data_hash").to_pylist()
            )
        else:
            out[f"table[{name}].record_ids"] = sorted(
                _jsonable(v) for v in records.column("__golden_record_id").to_pylist()
            )
    return out


# ---------------------------------------------------------------------------
# Polars values (ITL-578) — semantic, schema and datagram entries
# ---------------------------------------------------------------------------
#
# Each Polars case is separate from the groups' general cases, so running the generator
# against a release without the Polars logical types (``--allow-errors``) loses only these
# entries. The hash of a Polars value is a cache key carrying the label
# ``polars_dataframe_v1`` / ``polars_series_v1`` (``orcapod.logical_types.polars_type``); an
# intended change to it is a new definition under new labels, never new bytes under these.


def _polars_values() -> dict[str, Any]:
    """Polars values whose hashes are pinned: one per dtype family the grammar names."""
    import datetime as dt
    import decimal

    import polars as pl

    return {
        "simple": pl.DataFrame(
            {"x": pl.Series([1, 2, 3], dtype=pl.Int64), "y": ["a", "b", "c"]}
        ),
        "nulls_and_temporal": pl.DataFrame(
            {
                "i": pl.Series([1, None, -3], dtype=pl.Int8),
                "t": pl.Series(
                    [dt.datetime(2020, 1, 1, 12), None, dt.datetime(1999, 12, 31)],
                    dtype=pl.Datetime("ms", "America/New_York"),
                ),
                "d": pl.Series(
                    [dt.timedelta(seconds=1), dt.timedelta(0), None],
                    dtype=pl.Duration("us"),
                ),
                "m": pl.Series(
                    [decimal.Decimal("1.25"), None, decimal.Decimal("-0.50")],
                    dtype=pl.Decimal(10, 2),
                ),
            }
        ),
        "nested_and_categorical": pl.DataFrame(
            {
                "e": pl.Series(["x", "y", None], dtype=pl.Enum(["y", "x", "z"])),
                "c": pl.Series(["p", None, "q"], dtype=pl.Categorical),
                "a": pl.Series([[1, 2], [3, 4], None], dtype=pl.Array(pl.Int16, 2)),
                "s": pl.Series(
                    [{"u": 1, "l": [1.5]}, None, {"u": None, "l": []}],
                    dtype=pl.Struct({"u": pl.UInt16, "l": pl.List(pl.Float32)}),
                ),
                "n": pl.Series([None, None, None], dtype=pl.Null),
            }
        ),
        "empty_with_schema": pl.DataFrame(schema={"x": pl.Int32, "b": pl.Binary}),
        "series": pl.Series("metric", [1.5, None, 3.0], dtype=pl.Float32),
    }


def _polars_types() -> dict[str, Any]:
    """Polars classes whose type, stored Arrow type and ``Schema`` hashes are pinned."""
    import polars as pl

    return {"dataframe": pl.DataFrame, "series": pl.Series}


@case("semantic", "polars_value", provisional=POLARS_TYPES)
def semantic_polars_values() -> dict[str, Any]:
    """Semantic hash of each Polars value (the ``PolarsDataFrameHandler`` digest)."""
    hasher = _ctx().semantic_hasher
    return {k: hasher.hash_object(v).to_string() for k, v in _polars_values().items()}


@case("semantic", "polars_type", provisional=POLARS_TYPES)
def semantic_polars_types() -> dict[str, Any]:
    """Semantic hash of the ``pl.DataFrame`` and ``pl.Series`` type objects."""
    hasher = _ctx().semantic_hasher
    return {k: hasher.hash_object(t).to_string() for k, t in _polars_types().items()}


@case("schema", "polars_single_field", provisional=POLARS_TYPES)
def schema_polars_single_field() -> dict[str, Any]:
    """Stored Arrow type and single-field ``Schema`` hash of each Polars class."""
    from orcapod.types import Schema

    ctx = _ctx()
    out: dict[str, Any] = {}
    for name, tp in _polars_types().items():
        out[f"{name}.arrow_type"] = _arrow_type_text(
            ctx.type_converter.python_type_to_arrow_type(tp)
        )
        out[f"{name}.schema_hash"] = ctx.semantic_hasher.hash_object(
            Schema({"c": tp})
        ).to_string()
    return out


@case("datagram", "polars_data_hash", provisional=POLARS_TYPES)
def datagram_polars_data_hash() -> dict[str, Any]:
    """Content hash of the ``Data`` holding each Polars value in its only data column."""
    from orcapod.sources import DictSource

    out: dict[str, Any] = {}
    for name, value in _polars_values().items():
        source = DictSource(
            [{"k": "a", "v": value}],
            tag_columns=["k"],
            tag_only_columns=True,
            data_schema={"k": str, "v": type(value)},
        )
        ((_, data),) = list(source.iter_data())
        out[name] = data.content_hash().to_string()
    return out


def _polars_rows() -> dict[str, list[dict[str, Any]]]:
    """Rows of the Polars-valued sources whose identities are pinned."""
    import polars as pl

    return {
        "polars_dataframe": [
            {"k": "r1", "c": pl.DataFrame({"a": [1, 2]})},
            {"k": "r2", "c": pl.DataFrame({"a": [3], "b": ["x"]})},
        ],
        "polars_series": [
            {"k": "r1", "c": pl.Series("a", [1, 2])},
            {"k": "r2", "c": pl.Series("b", ["x"])},
        ],
    }


def _make_polars_datagram_case(name: str) -> CaseFn:
    """Build the case summarizing the Polars-valued ``DictSource`` named ``name``.

    Args:
        name: A key of ``_polars_rows()``.

    Returns:
        A case function returning the source's ``stream_summary``.
    """

    def run() -> dict[str, Any]:
        """Return the ``stream_summary`` of the Polars-valued source."""
        import polars as pl

        from orcapod.sources import DictSource

        column_type = pl.DataFrame if name == "polars_dataframe" else pl.Series
        source = DictSource(
            data=_polars_rows()[name],
            tag_columns=["k"],
            tag_only_columns=True,
            data_schema={"k": str, "c": column_type},
            source_id=f"golden_{name}",
        )
        return stream_summary(source)

    return run


for _name, _declared in (
    ("polars_dataframe", (POLARS_DATAFRAME,)),
    ("polars_series", (POLARS_SERIES,)),
):
    case("datagram", _name, provisional=_declared)(_make_polars_datagram_case(_name))


@case("arrow", "polars_extension_no_values", provisional=POLARS_TYPES)
def arrow_polars_extension_columns_without_values() -> dict[str, Any]:
    """Table hashes of Polars-typed columns with zero rows or nulls only.

    The Polars counterpart of ``arrow:extension_no_values``: such a column has no cell
    token, so its logical type enters the hash through the hashed field alone (ITL-967).
    """
    ctx = _ctx()
    entries: dict[str, Any] = {}
    for rows, label in ((0, "zero_rows"), (2, "all_null")):
        for name, python_type in _polars_types().items():
            arrow_type = ctx.type_converter.python_type_to_arrow_type(python_type)
            storage = pa.array([None] * rows, arrow_type.storage_type)
            column = pa.ExtensionArray.from_storage(arrow_type, storage)
            entries[f"{label}.{name}"] = ctx.arrow_hasher.hash_table(
                pa.table({"c": column})
            ).to_string()
    return entries


def _polars_exprs() -> dict[str, Any]:
    """Build representative Polars expression predicates, keyed by name."""
    from datetime import date, datetime, timezone

    import polars as pl

    return {
        "gt_dyn_int": pl.col("x") > 0,
        "gt_dyn_float": pl.col("x") > 0.5,
        "gt_int8": pl.col("x") > pl.lit(0, dtype=pl.Int8),
        "eq_string": pl.col("s") == "abc",
        "and_two_columns": (pl.col("x") > 0) & (pl.col("y") < 5),
        "is_in_strings": pl.col("s").is_in(["a", "b"]),
        "is_between_dates": pl.col("d").is_between(
            date(2024, 1, 1), date(2024, 12, 31)
        ),
        "gt_datetime_utc": pl.col("t") > datetime(2024, 1, 1, tzinfo=timezone.utc),
        "is_not_null_cast": pl.col("x").cast(pl.Float32).is_not_null(),
        "str_contains": pl.col("s").str.contains("^a"),
    }


# In the library only PolarsFilter feeds polars_expr_v1 (its predicates), so these hashes are
# as provisional as PolarsFilter is (ITL-1266), although the case builds no operator.
@case("semantic", "polars_expr", provisional=("PolarsFilter",))
def semantic_polars_exprs() -> dict[str, Any]:
    """Record the ``polars_expr_v1`` semantic hash of each representative expression."""
    hasher = _ctx().semantic_hasher
    return {k: hasher.hash_object(e).to_string() for k, e in _polars_exprs().items()}


@case("pipeline", "polars_filter_expr", provisional=("PolarsFilter",))
def pipeline_polars_filter_expr() -> dict[str, Any]:
    """Record expression-only and expression-plus-keyword filter streams."""
    import polars as pl

    from orcapod.operators import PolarsFilter

    out = _prefixed(
        "expr",
        stream_summary(PolarsFilter(predicates=[pl.col("weight") > 20])(_grouped())),
    )
    out.update(
        _prefixed(
            "mixed",
            stream_summary(
                PolarsFilter(
                    predicates=[pl.col("weight") > 20], constraints={"cohort": "a"}
                )(_grouped())
            ),
        )
    )
    return out


@case("pipeline", "polars_filter_expr_job", provisional=("PolarsFilter",))
def pipeline_polars_filter_expr_job() -> dict[str, Any]:
    """Record a job with an expression filter upstream of a join and a function pod."""
    import polars as pl

    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.operators import Join

    db = InMemoryArrowDatabase()
    job = PipelineJob(name="golden_filter_job", database=db)
    with job:
        older = _subjects().polars_filter(pl.col("age") > 3, label="older")
        joined = Join(label="joined")(older, _scans())
        golden_adjust.pod(joined, label="adjust")
    job.run()

    out: dict[str, Any] = {}
    for label in sorted(job.nodes):
        out.update(_prefixed(f"node[{label}]", stream_summary(job.nodes[label])))
    paths = sorted(db.list_sources())
    out["table_paths"] = ["/".join(p) for p in paths]
    for path in paths:
        name = "/".join(path)
        records = db.get_all_records(path, record_id_column="__golden_record_id")
        assert records is not None
        if "__input_data_hash" in records.column_names and "_result" in path:
            # Result-table record ids are UUIDv7 (volatile); pin the lookup key instead.
            out[f"table[{name}].input_data_hashes"] = sorted(
                _jsonable(v) for v in records.column("__input_data_hash").to_pylist()
            )
        else:
            out[f"table[{name}].record_ids"] = sorted(
                _jsonable(v) for v in records.column("__golden_record_id").to_pylist()
            )
    return out


# ---------------------------------------------------------------------------
# Group: arrow — value-hashed logical types in a column that holds no value
# ---------------------------------------------------------------------------


def _arrow_extension_columns_without_values(library: bool) -> dict[str, Any]:
    """Table hashes of value-hashed logical-type columns with zero rows or nulls only.

    Such a column has no cell token, so its logical type enters the hash through
    the hashed field alone (ITL-967). Every entry differs from every other and from
    the plain ``large_binary`` entries.
    """
    import orcapod as op

    ctx = _ctx()
    types: dict[str, Any] = {
        "uuid": uuid.UUID,
        "file": op.File,
        "directory": op.Directory,
        "ndarray": np.ndarray,
        "dataframe": pd.DataFrame,
        "series": pd.Series,
        "list_of_file": list[op.File],
        "set_of_uuid": set[uuid.UUID],
    }
    entries: dict[str, Any] = {}
    for rows, label in ((0, "zero_rows"), (2, "all_null")):
        for name, python_type in types.items():
            if bool(provisional_types_of(python_type)) != library:
                continue
            arrow_type = ctx.type_converter.python_type_to_arrow_type(python_type)
            storage = pa.array([None] * rows, arrow_type.storage_type)
            column = pa.ExtensionArray.from_storage(arrow_type, storage)
            entries[f"{label}.{name}"] = ctx.arrow_hasher.hash_table(
                pa.table({"c": column})
            ).to_string()
        if not library:
            entries[f"{label}.plain_large_binary"] = ctx.arrow_hasher.hash_table(
                pa.table({"c": pa.array([None] * rows, pa.large_binary())})
            ).to_string()
    return entries


@case("arrow", "extension_no_values")
def arrow_extension_columns_without_values() -> dict[str, Any]:
    """Every type but the provisional library value types."""
    return _arrow_extension_columns_without_values(library=False)


@case("arrow", "extension_no_values", provisional=NUMPY_PANDAS_TYPES)
def arrow_library_extension_columns_without_values() -> dict[str, Any]:
    """The NumPy and pandas columns of ``arrow:extension_no_values``: provisional
    (ITL-1324)."""
    return _arrow_extension_columns_without_values(library=True)


def _nullable_readings() -> Any:
    """A source with a nullable tag and two nullable data columns, each with a null."""
    from orcapod.sources import ArrowTableSource

    table = pa.table(
        {
            "k": pa.array(["r1", "r2", "r3", "r4"], pa.large_string()),
            "site": pa.array(["a", "b", "c", None], pa.large_string()),
            "x": pa.array([1, None, 3, 4], pa.int64()),
            "y": pa.array([1.5, 2.5, None, 4.5], pa.float64()),
        },
        schema=pa.schema(
            [
                pa.field("k", pa.large_string(), nullable=False),
                pa.field("site", pa.large_string(), nullable=True),
                pa.field("x", pa.int64(), nullable=True),
                pa.field("y", pa.float64(), nullable=True),
            ]
        ),
    )
    return ArrowTableSource(
        table,
        tag_columns=["k", "site"],
        tag_only_columns=True,
        source_id="golden_nullable",
    )


@case("pipeline", "drop_nulls")
def pipeline_drop_nulls() -> dict[str, Any]:
    """Pin ``DropNulls``: the operator's own identity and the narrowed rows."""
    from orcapod.operators import DropNulls

    out = _prefixed("source", stream_summary(_nullable_readings()))
    out["operator[x].content_hash"] = DropNulls("x").content_hash().to_string()
    out["operator[x,y].content_hash"] = DropNulls(["y", "x"]).content_hash().to_string()
    out.update(_prefixed("data", stream_summary(DropNulls("x")(_nullable_readings()))))
    out.update(
        _prefixed(
            "data_many", stream_summary(DropNulls(["x", "y"])(_nullable_readings()))
        )
    )
    out.update(
        _prefixed("tag", stream_summary(_nullable_readings().drop_nulls("site")))
    )
    return out


@case("pipeline", "drop_nulls_all")
def pipeline_drop_nulls_all() -> dict[str, Any]:
    """Pin the no-argument ``DropNulls``: its fixed identity and the narrowed rows."""
    from orcapod.operators import DropNulls

    out: dict[str, Any] = {}
    out["operator[all].content_hash"] = DropNulls().content_hash().to_string()
    out["operator[all].pipeline_hash"] = DropNulls().pipeline_hash().to_string()
    out.update(_prefixed("all", stream_summary(DropNulls()(_nullable_readings()))))
    out.update(_prefixed("method", stream_summary(_nullable_readings().drop_nulls())))
    return out


@case("pipeline", "add_tags", provisional=("AddTags",))
def pipeline_add_tags() -> dict[str, Any]:
    """Pin ``AddTags`` (ITL-1196): its identity, one value of each type, and the rows."""
    from orcapod.operators import AddTags

    every_type = {"cohort": "A", "run": 3, "ok": True, "w": 1.5, "raw": b"\x01"}
    out: dict[str, Any] = {}
    out["operator[every_type].content_hash"] = (
        AddTags(every_type).content_hash().to_string()
    )
    out["operator[every_type].pipeline_hash"] = (
        AddTags(every_type).pipeline_hash().to_string()
    )
    out.update(
        _prefixed(
            "every_type", stream_summary(AddTags(every_type)(_nullable_readings()))
        )
    )
    out.update(
        _prefixed(
            "method", stream_summary(_nullable_readings().add_tags(cohort="A", run=3))
        )
    )
    return out


@case("pipeline", "drop_nulls_job")
def pipeline_drop_nulls_job() -> dict[str, Any]:
    """Record a job with ``DropNulls`` upstream of a pod with non-optional parameters."""
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase

    db = InMemoryArrowDatabase()
    job = PipelineJob(name="golden_drop_null_job", database=db)
    with job:
        present = (
            _nullable_readings()
            .drop_nulls(["x", "y"], label="present")
            .map_data({"x": "age", "y": "weight"}, drop_unmapped=True, label="renamed")
        )
        golden_ratio.pod(present, label="ratio")
    job.run()

    out: dict[str, Any] = {}
    for label in sorted(job.nodes):
        out.update(_prefixed(f"node[{label}]", stream_summary(job.nodes[label])))
    paths = sorted(db.list_sources())
    out["table_paths"] = ["/".join(p) for p in paths]
    for path in paths:
        name = "/".join(path)
        records = db.get_all_records(path, record_id_column="__golden_record_id")
        assert records is not None
        if "__input_data_hash" in records.column_names and "_result" in path:
            # Result-table record ids are UUIDv7 (volatile); pin the lookup key instead.
            out[f"table[{name}].input_data_hashes"] = sorted(
                _jsonable(v) for v in records.column("__input_data_hash").to_pylist()
            )
        else:
            out[f"table[{name}].record_ids"] = sorted(
                _jsonable(v) for v in records.column("__golden_record_id").to_pylist()
            )
    return out


@case("pipeline", "default_source_ids")
def pipeline_default_source_ids() -> dict[str, Any]:
    """Pin the default source ids built from a hash (ITL-957).

    A source built with no ``source_id`` is named ``<source kind>:<algorithm
    label>:<hex>``; a source made with ``node.as_source()`` is named
    ``derived:<algorithm label>:<hex>``.
    """
    import polars as pl

    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.operators import Join
    from orcapod.sources import (
        ArrowTableSource,
        DataFrameSource,
        DictSource,
        ListSource,
    )

    sources: dict[str, Any] = {
        "arrow_table": ArrowTableSource(
            _builder_table(), tag_columns=["subject"], tag_only_columns=True
        ),
        "dict": DictSource(
            data=SUBJECT_ROWS, tag_columns=["subject"], tag_only_columns=True
        ),
        "list": ListSource("weight", [20.5, 22.25, 19.0]),
        "data_frame": DataFrameSource(
            pl.DataFrame(SUBJECT_ROWS), tag_columns=["subject"], tag_only_columns=True
        ),
    }
    out: dict[str, Any] = {}
    for name, source in sources.items():
        out[f"{name}.source_id"] = source.source_id
        out[f"{name}.content_hash"] = source.content_hash().to_string()

    job = PipelineJob(
        name="golden_default_source_ids", database=InMemoryArrowDatabase()
    )
    with job:
        joined = Join(label="joined")(_subjects(), _scans())
        golden_adjust.pod(joined, label="adjust")
    job.run()
    out["as_source[operator_node].source_id"] = (
        job.nodes["joined"].as_source().source_id
    )
    out["as_source[function_node].source_id"] = (
        job.nodes["adjust"].as_source().source_id
    )
    return out


# ---------------------------------------------------------------------------
# Group: torch — torch.Tensor values (ITL-871); needs the optional torch package
# ---------------------------------------------------------------------------


def _torch_values() -> dict[str, Any]:
    """Tensors whose hashes are pinned: the ndarray cases' values, torch-only dtypes
    and one case per optional property of the value hash."""
    import torch

    base = torch.arange(6, dtype=torch.float32).reshape(2, 3)
    return {
        # The same values as the ndarray.* semantic cases.
        "int32": torch.arange(6, dtype=torch.int32).reshape(2, 3),
        "int64": torch.arange(6, dtype=torch.int64).reshape(2, 3),
        "float64": torch.tensor([[0.5, 1.5], [2.5, 3.5]], dtype=torch.float64),
        "empty": torch.zeros((0,), dtype=torch.float32),
        "bool": torch.tensor([True, False, True]),
        "bfloat16": torch.tensor([1.5, -2.25, 3.0], dtype=torch.bfloat16),
        "complex32": torch.tensor([1 + 2j, -0.5 + 0.25j], dtype=torch.complex32),
        "zero_dim": torch.tensor(2.5),
        # One case per optional property of the hash (all others canonical).
        "non_contiguous": base.T,
        "requires_grad": base.clone().requires_grad_(True),
        "parameter": torch.nn.Parameter(base.clone(), requires_grad=False),
        "conj": torch.tensor([1 + 2j, 3 - 4j], dtype=torch.complex64).conj(),
        # A view of a larger buffer is hashed as its clone: contiguous strides.
        "step_slice": torch.arange(12, dtype=torch.float32).reshape(2, 6)[:, ::2],
        # Powers of two are exact in every float8 format. A dtype this torch lacks is
        # left out; its pinned entry is listed by unavailable_entries.
        **{
            name: torch.tensor([0.5, 1.0, 2.0]).to(dtype)
            for name in TORCH_VERSIONED_DTYPES
            if (dtype := installed_torch_dtype(name)) is not None
        },
    }


TORCH_VERSIONED_DTYPES = (
    "float8_e4m3fn",
    "float8_e5m2",
    "float8_e4m3fnuz",
    "float8_e5m2fnuz",
    "float8_e8m0fnu",
)
"""Pinned torch dtypes that a supported torch may lack (``float8_e8m0fnu`` needs 2.7)."""


def installed_torch_dtype(name: str) -> Any:
    """Return the installed torch's dtype called ``name``, or ``None`` if it has none.

    The one place the torch cases ask whether a dtype exists, so a test can stand
    in for an older torch by replacing this function.

    Args:
        name: A torch dtype name, for example ``"float8_e8m0fnu"``.

    Returns:
        The ``torch.dtype``, or ``None``.
    """
    import torch

    dtype = getattr(torch, name, None)
    return dtype if isinstance(dtype, torch.dtype) else None


def unavailable_entries(group: str) -> set[str]:
    """Pinned entries of ``group`` that this environment cannot compute.

    These are the ``value.*`` entries of torch dtypes the installed torch lacks. The
    golden test skips them and the generator keeps their pins.

    Args:
        group: One of ``GROUPS``.

    Returns:
        The entry names; empty for every group but an available ``torch`` group.
    """
    if group != "torch" or not group_available(group):
        return set()
    return {
        f"value.{name}"
        for name in TORCH_VERSIONED_DTYPES
        if installed_torch_dtype(name) is None
    }


@case("torch", "value", provisional=(TORCH_TENSOR,))
def torch_values() -> dict[str, Any]:
    """Semantic hashes of the pinned tensors."""
    hasher = _ctx().semantic_hasher
    return {k: hasher.hash_object(v).to_string() for k, v in _torch_values().items()}


@case("torch", "type", provisional=(TORCH_TENSOR,))
def torch_type() -> dict[str, Any]:
    """Semantic hash of the ``torch.Tensor`` type."""
    import torch

    return {
        "torch_tensor": _ctx().semantic_hasher.hash_object(torch.Tensor).to_string()
    }


@case("torch", "single_field", provisional=(TORCH_TENSOR,))
def torch_single_field() -> dict[str, Any]:
    """Arrow type and schema hash of a one-field ``torch.Tensor`` schema."""
    import torch

    from orcapod.types import Schema

    ctx = _ctx()
    return {
        "torch_tensor.arrow_type": _arrow_type_text(
            ctx.type_converter.python_type_to_arrow_type(torch.Tensor)
        ),
        "torch_tensor.schema_hash": ctx.semantic_hasher.hash_object(
            Schema({"c": torch.Tensor})
        ).to_string(),
    }


@case("torch", "datagram", provisional=(TORCH_TENSOR,))
def torch_datagram() -> dict[str, Any]:
    """Identity of a two-row tensor source."""
    import torch

    from orcapod.sources import DictSource

    rows = [
        {"k": "r1", "c": torch.arange(4, dtype=torch.int64)},
        {"k": "r2", "c": torch.tensor([[0.5, 1.5]], dtype=torch.float64)},
    ]
    return stream_summary(
        DictSource(
            data=rows,
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="golden_torch_tensor",
        )
    )


@case("pipeline", "tagless_producerless_stream")
def pipeline_tagless_producerless_stream() -> dict[str, Any]:
    """Pin the content hash of a stream with no tag column and no producer (ITL-1053).

    Its identity hashes its rows in the canonical row order of an identity table
    (ITL-1227), so the hash is the same for rows given in any order.

    Returns:
        The content hash for two orders of the same rows.
    """
    from orcapod.core.streams import ArrowTableStream

    def table(record_ids: list[str], xs: list[int]) -> pa.Table:
        return pa.table(
            [pa.array(record_ids, pa.string()), pa.array(xs, pa.int64())],
            names=["record_id", "x"],
        )

    rows = table(["b", "c", "a"], [1, 2, 3])
    reordered = table(["c", "a", "b"], [2, 3, 1])
    return {
        "as_given": ArrowTableStream(
            rows, tag_columns=[], system_tag_columns=["record_id"]
        )
        .content_hash()
        .to_string(),
        "reordered": ArrowTableStream(
            reordered, tag_columns=[], system_tag_columns=["record_id"]
        )
        .content_hash()
        .to_string(),
    }


@case("pipeline", "list_tag_producerless_stream")
def pipeline_list_tag_producerless_stream() -> dict[str, Any]:
    """Pin the content hash of a stream with a list-valued tag and no producer (ITL-1053).

    A list column is no sort key of the canonical row order of an identity table
    (ITL-1227): the rows' own hashes settle their order, so the same rows in two orders
    hash the same.

    Returns:
        The content hash for two orders of the same rows.
    """
    from orcapod.core.streams import ArrowTableStream

    def stream(ks: list[list[int]], xs: list[int]) -> ArrowTableStream:
        table = pa.table(
            [pa.array(ks, pa.large_list(pa.int64())), pa.array(xs, pa.int64())],
            names=["k", "x"],
        )
        return ArrowTableStream(table, tag_columns=["k"])

    return {
        "as_given": stream([[2], [1]], [20, 10]).content_hash().to_string(),
        "reversed": stream([[1], [2]], [10, 20]).content_hash().to_string(),
    }


# ---------------------------------------------------------------------------
# Cases brought in from tests/test_hashing/hash_samples (ITL-946)
#
# The values, annotations, schemas and annotated functions that the older sample files
# pinned and no case above did. The functions are defined here, so their pins carry this
# module's path. DO NOT MOVE OR RENAME them.
# ---------------------------------------------------------------------------

SAMPLE_VALUES: dict[str, Any] = {
    "int_42": 42,
    "float_pi": 3.14159,
    "float_negative_e": -2.71828,
    "str_hello": "hello",
    "str_hello_world": "Hello, World!",
    "str_special_chars": "Special chars: !@#$%^&*()",
    "str_unicode": "Unicode: 你好, Привет, こんにちは",
    "bytes_hello": b"hello",
    "bytes_binary": b"\x00\x01\x02\x03",
    "bytearray_hello_world": bytearray(b"hello world"),
    "bytearray_abc": bytearray([65, 66, 67]),
    "list_strs": ["a", "b", "c"],
    "list_mixed": [1, "a", True],
    "set_empty": set(),
    "set_strs": {"a", "b", "c"},
    "frozenset_empty": frozenset(),
    "tuple_empty": (),
    "dict_one": {"a": 1},
    "dict_other_order": {"b": 1, "a": 2},
    "dict_nested": {"nested": {"a": 1, "b": 2}},
    "nested_lists": [1, [2, [3, [4, [5]]]]],
    "nested_dicts": {"a": {"b": {"c": {"d": {"e": 42}}}}},
    "nested_dict_list": {"a": [1, 2, {"b": [3, 4, {"c": 5}]}]},
    "list_of_dicts": [{"a": 1}, {"b": 2}, {"c": [3, 4, 5]}],
    "dict_keys_values": {"keys": ["a", "b", "c"], "values": [1, 2, 3]},
    "list_of_dicts_with_lists": [{"a": 1, "b": [2, 3]}, {"c": 4, "d": [5, 6]}],
    "dict_users": {"users": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]},
    "dict_points_labels": {
        "data": {"points": [[1, 2], [3, 4], [5, 6]], "labels": ["A", "B", "C"]}
    },
    "ordered_dict": collections.OrderedDict([("a", 1), ("b", 2), ("c", 3)]),
    "lists_and_dict": [[1, 2], [3, 4], {"a": [5, 6], "b": [7, 8]}],
}


@case("semantic", "sample_value")
def semantic_sample_values() -> dict[str, Any]:
    """Semantic hashes of the plain Python values of the older hash sample file."""
    hasher = _ctx().semantic_hasher
    return {k: hasher.hash_object(v).to_string() for k, v in SAMPLE_VALUES.items()}


def _annotations() -> dict[str, Any]:
    import typing

    import orcapod as op

    return {
        "file": op.File,
        "directory": op.Directory,
        "list_file": list[op.File],
        "dict_str_file": dict[str, op.File],
        "int_or_str": int | str,
        "file_or_none": op.File | None,
        "optional_file": typing.Optional[op.File],  # noqa: UP045  (the typing form is the case)
    }


@case("semantic", "annotation")
def semantic_annotations() -> dict[str, Any]:
    """Semantic hashes of type annotations over Orcapod's logical types (ITL-638)."""
    hasher = _ctx().semantic_hasher
    return {k: hasher.hash_object(t).to_string() for k, t in _annotations().items()}


@case("schema", "semantic_schema")
def schema_semantic_hashes() -> dict[str, Any]:
    """Semantic hashes of ``Schema`` objects with primitive and logical types."""
    import orcapod as op
    from orcapod.types import Schema

    schemas = {
        "x_int": Schema({"x": int}),
        "x_int_y_str": Schema({"x": int, "y": str}),
        "f_file": Schema({"f": op.File}),
        "d_directory": Schema({"d": op.Directory}),
        "p_path": Schema({"p": op.Path}),
        "x_int_f_file": Schema({"x": int, "f": op.File}),
        "f_file_d_directory": Schema({"f": op.File, "d": op.Directory}),
        "f_optional_file": Schema({"f": op.File | None}),
    }
    hasher = _ctx().semantic_hasher
    return {k: hasher.hash_object(s).to_string() for k, s in schemas.items()}


def golden_annotated_none():
    """Function without annotations (ITL-946). DO NOT MOVE."""
    return None


def golden_annotated_builtin(x: int, y: str) -> float:
    """Function annotated with builtins (ITL-946). DO NOT MOVE."""
    return float(x)


def golden_annotated_file_param(f: op.File) -> str:
    """Function taking a ``File`` (ITL-946). DO NOT MOVE."""
    return str(f)


def golden_annotated_file_return(s: str) -> op.File:
    """Function returning a ``File`` (ITL-946). DO NOT MOVE."""
    return op.File(s)  # pyright: ignore[reportArgumentType]  (never called; only hashed)


def golden_annotated_file_list(files: list[op.File]) -> list[str]:
    """Function taking a list of ``File`` (ITL-946). DO NOT MOVE."""
    return []


def golden_annotated_optional_file(f: op.File | None) -> op.File | None:
    """Function taking and returning an optional ``File`` (ITL-946). DO NOT MOVE."""
    return f


def golden_annotated_mixed(f: op.File, n: int) -> op.Directory:
    """Function mixing ``File``, ``int`` and ``Directory`` (ITL-946). DO NOT MOVE."""
    return op.Directory(str(f))  # pyright: ignore[reportArgumentType]  (never called)


@case("semantic", "annotated_function")
def semantic_annotated_functions() -> dict[str, Any]:
    """Hashes of annotated functions: the extracted signature info and the function value."""
    from orcapod.hashing.semantic_hashing.function_info_extractors import (
        FunctionSignatureExtractor,
    )

    functions = {
        "none": golden_annotated_none,
        "builtin": golden_annotated_builtin,
        "file_param": golden_annotated_file_param,
        "file_return": golden_annotated_file_return,
        "file_list": golden_annotated_file_list,
        "optional_file": golden_annotated_optional_file,
        "mixed": golden_annotated_mixed,
    }
    hasher = _ctx().semantic_hasher
    extractor = FunctionSignatureExtractor(
        include_module=True,
        include_defaults=True,
        type_converter=_ctx().type_converter,
    )
    out: dict[str, Any] = {}
    for name, fn in functions.items():
        info = extractor.extract_function_info(fn)
        out[f"{name}.info"] = hasher.hash_object(info).to_string()
        out[f"{name}.object"] = hasher.hash_object(fn).to_string()
    return out


# ---------------------------------------------------------------------------
# GroupBy over several tags and several data columns (ITL-1226)
# ---------------------------------------------------------------------------

# Two non-key tags, ``x`` and ``y``, whose orders disagree; ``y`` and ``b`` hold nulls;
# the rows 1 and 2 share every tag, so only the record id orders them.
GROUP_MEMBER_ORDER_FIELDS = {
    "g": (pa.large_string(), False, ["a", "a", "a", "a", "a", "a"]),
    "k": (pa.int64(), False, [1, 1, 1, 2, 2, 2]),
    "x": (pa.int64(), False, [2, 1, 1, 1, 2, 1]),
    "y": (pa.large_string(), True, ["p", "q", "q", None, "p", "r"]),
    "w": (pa.float64(), False, [0.5, 1.5, 2.5, 3.5, 4.5, 5.5]),
    "n": (pa.int32(), True, [10, 20, 30, 40, None, 60]),
    "s": (pa.large_string(), False, ["u", "v", "w", "x", "y", "z"]),
}


def _group_member_order_source(tag_columns: list[str], layout: list[str]) -> Any:  # pyright: ignore[reportExplicitAny]  # a source
    """One set of rows, with the tags declared and the table laid out as given."""
    from orcapod.sources import ArrowTableSource

    fields = GROUP_MEMBER_ORDER_FIELDS
    table = pa.table(
        {name: pa.array(fields[name][2], fields[name][0]) for name in layout},
        schema=pa.schema(
            [
                pa.field(name, fields[name][0], nullable=fields[name][1])
                for name in layout
            ]
        ),
    )
    return ArrowTableSource(
        table,
        tag_columns=tag_columns,
        tag_only_columns=True,
        source_id="golden_group_member_order",
    )


GROUP_MEMBER_ORDER_ARRANGEMENTS: dict[str, tuple[list[str], list[str], list[str]]] = {
    # name: (tag_columns, table layout, by)
    "tags_first": (
        ["g", "k", "x", "y"],
        ["g", "k", "x", "y", "w", "n", "s"],
        ["g", "k"],
    ),
    "reversed": (["y", "x", "k", "g"], ["s", "n", "w", "y", "x", "k", "g"], ["k", "g"]),
    "interleaved": (
        ["k", "y", "g", "x"],
        ["w", "k", "s", "y", "n", "g", "x"],
        ["g", "k"],
    ),
}
"""Arrangements of one input whose ``GroupBy`` entries must be equal (ITL-1226):
``tests/test_hashing/golden/test_group_by_arrangements.py`` asserts it."""


@case("pipeline", "group_by_arrangement", provisional=("GroupBy",))
def pipeline_group_by_arrangement() -> dict[str, Any]:
    """Pin ``GroupBy`` over several tags and data columns, in several arrangements.

    The default member order (the non-key tags in name order, then the record id),
    an explicit ``member_order``, and a grouped stream grouped again. Every
    arrangement of the input's columns and of ``by`` pins the same values.
    """
    from orcapod.operators import GroupBy

    out: dict[str, Any] = {}
    for name, (tags, layout, by) in GROUP_MEMBER_ORDER_ARRANGEMENTS.items():
        source = _group_member_order_source(tags, layout)
        out.update(_prefixed(f"default.{name}", stream_summary(GroupBy(by=by)(source))))
        out.update(
            _prefixed(
                f"member_order_y.{name}",
                stream_summary(GroupBy(by=by, member_order=["y"])(source)),
            )
        )
        out.update(
            _prefixed(
                f"grouped_again.{name}",
                stream_summary(GroupBy(by=["g"])(GroupBy(by=by)(source))),
            )
        )
    return out


# GroupBy over a float key with NaN and -0.0 (ITL-1234)
# ---------------------------------------------------------------------------


@case("pipeline", "group_by_nan_keys", provisional=("GroupBy",))
def pipeline_group_by_nan_keys() -> dict[str, Any]:
    """Pin ``GroupBy`` over a float key whose NaN and zero values differ in bits.

    The three NaN keys (the canonical quiet NaN, one with the sign bit set, one with
    a payload) form one group, which emits the canonical NaN; ``0.0`` and ``-0.0``
    form one group, which emits ``0.0``; the null keys form one group.
    """
    from orcapod.operators import GroupBy
    from orcapod.sources import ArrowTableSource

    key_bits = [
        0xFFF8000000000000,
        0x8000000000000000,
        0x7FF8000000000001,
        None,
        0x0,
        0x7FF8000000000000,
        None,
    ]
    table = pa.table(
        {
            "k": pa.array(key_bits, pa.uint64()).view(pa.float64()),
            "t": pa.array([1, 2, 3, 4, 5, 6, 7], pa.int64()),
            "v": pa.array([10, 20, 30, 40, 50, 60, 70], pa.int64()),
        }
    )
    source = ArrowTableSource(
        table,
        tag_columns=["k", "t"],
        tag_only_columns=True,
        source_id="golden_group_by_nan_keys",
    )
    grouped = GroupBy(by=["k"])(source)
    out = stream_summary(grouped)
    out.update(_group_key_identity(grouped, pa.uint64()))

    # A data-tag key of float32 (the key is data too): a signaling NaN, whose bits are
    # the smallest of the NaN group, is emitted bit for bit; the key's data hash and
    # its folded source-info token are pinned with the rest.
    data_tag_bits = [0x7FC00000, 0x80000000, 0x7F800001, None, 0x0, 0xFFC00000]
    data_tag_table = pa.table(
        {
            "k": pa.array(data_tag_bits, pa.uint32()).view(pa.float32()),
            "t": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
            "v": pa.array([10, 20, 30, 40, 50, 60], pa.int64()),
        }
    )
    data_tag_source = ArrowTableSource(
        data_tag_table,
        tag_columns=["k", "t"],
        tag_only_columns=["t"],
        source_id="golden_group_by_nan_keys_data_tag",
    )
    data_tag_grouped = GroupBy(by=["k"])(data_tag_source)
    out.update(
        _prefixed(
            "data_tag_key",
            {
                **stream_summary(data_tag_grouped),
                **_group_key_identity(data_tag_grouped, pa.uint32()),
            },
        )
    )
    return out


def _group_key_identity(stream: Any, unsigned: Any) -> dict[str, Any]:  # pyright: ignore[reportExplicitAny]  # a stream and an Arrow type
    """Per row of a grouping on ``k``: the tag hash, the key's bits and its source info.

    ``stream_summary`` keys a row by its tag values, where every NaN reads ``NaN``;
    the bits and the tag hash pin which NaN (or which zero) the group emits.
    """
    out: dict[str, Any] = {}
    bits = stream.as_table().column("k").combine_chunks().view(unsigned).to_pylist()
    for (tag, data), key_bits in zip(stream.iter_data(), bits, strict=True):
        row = _row_key(tag.as_dict())
        out[f"row[{row}].tag_hash"] = tag.content_hash().to_string()
        out[f"row[{row}].key_bits"] = None if key_bits is None else f"{key_bits:x}"
        source_info = data.as_dict(all_info=True).get("_source_k")
        if source_info is not None:
            out[f"row[{row}]._source_k"] = _jsonable(source_info)
    return out
