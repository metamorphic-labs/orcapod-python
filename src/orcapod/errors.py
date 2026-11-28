from collections.abc import Mapping
from typing import Any, Self


class InputValidationError(Exception):
    """
    Exception raised when the inputs are not valid.
    This is used to indicate that the inputs do not meet the requirements of the operator.
    """


class RowIdentifierError(InputValidationError, ValueError):
    """A source's rows cannot be identified by the given key columns (ITL-1154).

    A key column is missing or of a type that cannot identify a row, a key value is
    null, or two rows share a key. The rows are wrong, not the moment: a retry reads
    the same rows and fails again, so a polling source stops on it at once.
    """


class SchemaInconsistencyError(InputValidationError):
    """Raised when a data batch has a schema that is incompatible with the expected schema.

    This can happen in two situations:

    - A fetched batch is missing a field declared in ``tag_schema`` or ``data_schema``,
      or one of those fields has a different type than declared.
    - Consecutive batches from a ``PollingSource`` have different column sets or
      column types (schema drift between polls).

    ``SchemaInconsistencyError`` is a subclass of ``InputValidationError`` so existing
    ``except InputValidationError`` handlers continue to work.
    """


class TypeIdentityError(ValueError):
    """Raised when stored values and a registered user class disagree on identity.

    A stored dataclass or pydantic column records its class's identity (name, kind and
    field types). This error is raised when the class found for that name has a
    different identity or a different Arrow storage type, or when the column was
    written by an Orcapod version that did not record the identity. The message names
    the column where it is known, the class, and both shapes.
    """


class RecursiveTypeError(TypeError):
    """Raised when a class refers back to itself through its fields.

    Arrow cannot store a recursive struct, so a dataclass or pydantic model whose
    fields reach the class again (``next: Node | None``) cannot be stored. The message
    names the field path, e.g. ``Node.next -> Node``.
    """


class SourceTooLargeError(ValueError):
    """Raised when a read of a source would return more rows than its ``max_rows``.

    The message gives the number of rows found or expected, the cap, and the ways to
    read fewer rows or lift the cap.
    """


class DuplicateTagError(ValueError):
    """Raised when duplicate tag values are found and skip_duplicates=False"""

    pass


class CanonicalOrderError(ValueError):
    """Raised when a dict or set value has no canonical order.

    Dict entries and set elements are stored in canonical order (ITL-905), so dict keys
    and set elements must be totally ordered and unique. Raised for a NaN key or element,
    values that cannot be compared, two keys or elements with the same stored value, or
    a null entry in a stored key/value list.
    """


class UnsupportedValueError(ValueError):
    """A value cannot be stored by the logical type of its column.

    Raised at write time, for example for a masked or object-dtype NumPy array.
    The type converter prefixes the message with the column name.
    """


class UnstorableDatetimeError(ValueError):
    """A ``datetime`` value that a ``datetime`` column cannot hold as it is (ITL-1268).

    A ``datetime`` column holds an instant, in UTC, at microsecond precision
    (``timestamp[us, tz=UTC]``). The type converter's one rule for storing a
    ``datetime`` (``universal_converter.storable_instant``) raises a subclass for what
    the converter stores: a function's outputs, sync or async, at the top level or
    inside a list, dict or class field, and the Python values of ``DictSource`` and
    ``ListSource``. A ``DataFrameSource`` takes no Python values: it refuses a dict or
    a list when it is built (ITL-1330). ``reason`` and
    ``hint`` name no value, so a function's ``OutputTypeError`` shows them as they
    are; the message adds the value.

    Attributes:
        reason: What is wrong with the value, without the value.
        hint: How to convert the value.
        value: The value.
    """

    reason: str = ""
    hint: str = ""

    def __init__(self, value: object) -> None:
        """Create the error for ``value``.

        Args:
            value: The ``datetime`` (or subclass, such as a ``pandas.Timestamp``) that
                cannot be stored.
        """
        self.value: object = value
        super().__init__(f"{self.reason}. {self.hint}. Got: {value!r}")

    def __reduce__(self) -> tuple[object, ...]:
        """Pickle with the value (executors send errors across processes)."""
        return (type(self), (self.value,))


class NaiveDatetimeError(UnstorableDatetimeError):
    """A ``datetime`` without a time zone, under the ``strict`` time zone policy.

    A ``datetime`` column holds an instant, and a value with no time zone names none. A
    type converter built with ``datetime_timezone="coerce_utc"`` reads such a value as
    UTC instead.
    """

    reason = (
        "Naive datetime (no timezone info) is not supported under the current "
        "'strict' timezone policy: a datetime is stored as an instant, in UTC, and a "
        "value without a time zone names no instant"
    )
    hint = (
        "Use a timezone-aware datetime: attach the time zone its wall time is in, "
        "e.g. value.replace(tzinfo=timezone.utc) when it is UTC or "
        'value.replace(tzinfo=ZoneInfo("Europe/Berlin")) for another zone '
        "(value.astimezone() takes this machine's local time zone)"
    )


class SubMicrosecondDatetimeError(UnstorableDatetimeError):
    """A ``datetime`` with a part below one microsecond, such as a ``pandas.Timestamp``
    with nanoseconds: storing it would drop that part, so it is refused."""

    reason = (
        "Datetime with a part below one microsecond is not supported: a datetime is "
        "stored at microsecond precision, and storing it would drop that part"
    )
    hint = (
        "Round or truncate it to whole microseconds first, "
        'e.g. value.floor("us") or value.round("us") for a pandas.Timestamp'
    )


class InstantOutOfRangeError(UnstorableDatetimeError):
    """A ``datetime`` whose instant in UTC lies outside the years a ``datetime`` holds.

    Only a value whose UTC offset has a part below one second reaches it: such a value is
    converted to UTC before Arrow sees it (Arrow drops that part of an offset), and near
    year 1 or year 9999 its UTC instant falls in year 0 or year 10000. Stored, it could not
    be read back as a ``datetime``, so it is refused.
    """

    reason = (
        "Datetime whose UTC offset has a part below one second names an instant outside "
        "the years a datetime can hold in UTC (1 to 9999): it could not be read back"
    )
    hint = (
        "Give the value an offset of whole seconds, or a date farther from the ends of "
        "that range"
    )


# UnstorableDatetimeError.__init__ calls super().__init__, which reaches
# BaseException.__init__ through ValueError: the base initializers do run.
class NotADatetimeError(UnstorableDatetimeError, TypeError):  # pyright: ignore[reportUnsafeMultipleInheritance]
    """A value that is not a ``datetime`` where a ``datetime`` is declared.

    For example a ``numpy.datetime64`` in a source column declared ``datetime``: a
    ``datetime`` column holds an instant, which only a timezone-aware ``datetime`` (or a
    subclass, such as a ``pandas.Timestamp``) names. It is a ``TypeError`` (a value of the
    wrong type) and, like every ``UnstorableDatetimeError``, a ``ValueError``. Its
    ``reason`` names the value's type, never the value.
    """

    hint = (
        "Convert it to a timezone-aware datetime first, e.g. "
        'pandas.Timestamp(value, tz="UTC") for a numpy.datetime64 that holds a UTC time'
    )

    def __init__(self, value: object) -> None:
        """Create the error for ``value``.

        Args:
            value: The value that is not a ``datetime``.
        """
        self.reason = (
            f"Value of type {type_text(type(value))} is not a datetime: a datetime is "
            "stored as an instant, which only a timezone-aware datetime names"
        )
        super().__init__(value)


class UnstorableColumnError(ValueError):
    """A database cannot store a column of a table it is asked to hold (ITL-1162).

    Raised by a database whose backend states which columns it stores faithfully (a
    ``ConnectorArrowDatabase``: column types on the SQLite and PostgreSQL connectors,
    reserved column names on SpiralDB), before anything of the table is written. A
    job raises it when the database is attached to a node whose stored tables hold
    such a column, before anything runs, for the columns known from declared
    schemas alone: a node's own columns and a function's outputs. The columns a
    node carries from its input (tags, system tags, source info) and an operator's
    output columns are refused when their table is first written, before anything
    of it is stored.

    Attributes:
        database_kind: The kind of database that refused, e.g. ``"SQLiteConnector"``.
        column: Name of the column.
        arrow_type: The column's Arrow type.
        reason: Why the database cannot store it.
        node_label: Label of the node whose table holds the column, or ``None`` when
            the refusal is not tied to one node.
    """

    def __init__(
        self,
        *,
        database_kind: str,
        column: str,
        arrow_type: object,
        reason: str,
        node_label: str | None = None,
    ) -> None:
        """Create the error.

        Args:
            database_kind: The kind of database that refused.
            column: Name of the column.
            arrow_type: The column's Arrow type.
            reason: Why the database cannot store it.
            node_label: Label of the node whose table holds the column, if known.
        """
        self.database_kind = database_kind
        self.column = column
        self.arrow_type = arrow_type
        self.reason = reason
        self.node_label = node_label
        super().__init__(database_kind, column, arrow_type, reason)

    def __reduce__(self) -> tuple[Any, ...]:
        """Rebuild from the keyword fields, so pickling and copying keep them."""
        return (
            _rebuild_unstorable_column_error,
            (
                self.database_kind,
                self.column,
                self.arrow_type,
                self.reason,
                self.node_label,
            ),
        )

    def add_context(self, node_label: str | None = None) -> None:
        """Fill in the node label where the raising site did not know it.

        Args:
            node_label: Label of the node whose table holds the column.
        """
        if self.node_label is None:
            self.node_label = node_label

    def __str__(self) -> str:
        node = f"node {self.node_label!r}: " if self.node_label is not None else ""
        return (
            f"{node}a {self.database_kind} database cannot store column "
            f"{self.column!r} of Arrow type {self.arrow_type}: {self.reason}. "
            # The in-memory database keeps every type; which others keep this one is
            # on the generated page (ITL-1334), so no hint names one that refuses it.
            "Use an InMemoryArrowDatabase instead; docs/reference/database-types.md "
            "lists what each database keeps."
        )


class UnstorableTableNameError(ValueError):
    """A database cannot hold a table under the name it is asked to use (ITL-1162).

    PostgreSQL truncates a table name longer than 63 bytes, so two Orcapod tables
    whose names share their first 63 bytes would silently become one. The
    connector states which names it holds (``unstorable_table_name_reason``), and
    a ``ConnectorArrowDatabase`` refuses any other name at every entry that
    creates, writes, reads or checks a table, so a truncated name is never sent.

    Attributes:
        database_kind: The kind of database that refused, e.g.
            ``"PostgreSQLConnector"``.
        table_name: The table name the database was asked to use.
        reason: Why the database cannot hold it.
    """

    def __init__(self, *, database_kind: str, table_name: str, reason: str) -> None:
        """Create the error.

        Args:
            database_kind: The kind of database that refused.
            table_name: The table name the database was asked to use.
            reason: Why the database cannot hold it.
        """
        self.database_kind = database_kind
        self.table_name = table_name
        self.reason = reason
        super().__init__(database_kind, table_name, reason)

    def __reduce__(self) -> tuple[Any, ...]:
        """Rebuild from the keyword fields, so pickling and copying keep them."""
        return (
            _rebuild_unstorable_table_name_error,
            (self.database_kind, self.table_name, self.reason),
        )

    def __str__(self) -> str:
        return (
            f"a {self.database_kind} database cannot hold a table named "
            f"{self.table_name!r}: {self.reason}. Use an InMemoryArrowDatabase or a "
            "DeltaTableDatabase instead."
        )


def _rebuild_unstorable_table_name_error(
    database_kind: str, table_name: str, reason: str
) -> UnstorableTableNameError:
    """Rebuild an ``UnstorableTableNameError`` from its fields (``__reduce__``)."""
    return UnstorableTableNameError(
        database_kind=database_kind, table_name=table_name, reason=reason
    )


def _rebuild_unstorable_column_error(
    database_kind: str,
    column: str,
    arrow_type: object,
    reason: str,
    node_label: str | None,
) -> UnstorableColumnError:
    """Rebuild an ``UnstorableColumnError`` from its fields (``__reduce__``)."""
    return UnstorableColumnError(
        database_kind=database_kind,
        column=column,
        arrow_type=arrow_type,
        reason=reason,
        node_label=node_label,
    )


class MissingOptionalDependencyError(ImportError):
    """A stored value needs an optional dependency that is not installed.

    Raised when a column's Arrow extension type belongs to a value type that is
    registered lazily (for example ``orcapod.torch.tensor``, which needs PyTorch)
    and its library cannot be imported. The message names the extension type and
    the missing module.
    """


class DataFunctionUnavailableError(RuntimeError):
    """Raised when a data function proxy is invoked without a bound function.

    This occurs when a pipeline is loaded in an environment where the
    original data function is not available. Only cached results can
    be accessed.
    """


class ValueConversionWarning(UserWarning):
    """Orcapod accepted a value and stored a converted form of it.

    Example: a torch tensor on a GPU or with autograd history (a ``grad_fn``), which
    is stored as a CPU tensor without that history. The message names who gave the value (a function pod's output, a source's
    column), what was converted and how to avoid the conversion. A value passed in by
    the caller is never modified. The code that issues the warning issues it once per
    (function or source, output or column, conversion), however many rows a run has,
    and does not rely on Python's warning registry to do so. To make it an error, call
    ``warnings.simplefilter("error", ValueConversionWarning)``: every conversion then
    raises, on every row, and in a pipeline that row fails. Python's ``-W`` option
    and ``PYTHONWARNINGS`` cannot name this class: they are read before installed
    packages can be imported.
    """


def type_text(annotation: object) -> str:
    """Return a short, readable name for a type or annotation, for error messages.

    Builtin classes are shown bare (``int``); other classes with their module
    (``numpy.int8``, ``decimal.Decimal``); generic aliases and unions as Python prints
    them (``list[int]``, ``int | None``). Never use this for hashing or identity.

    Args:
        annotation: A class or a typing annotation.

    Returns:
        The display name.
    """
    if isinstance(annotation, type) and not hasattr(annotation, "__origin__"):
        module = annotation.__module__
        if module in ("builtins", "__main__"):
            return annotation.__qualname__
        return f"{module}.{annotation.__qualname__}"
    return str(annotation).replace("typing.", "")


class OutputTypeError(TypeError, ValueError):
    """A function returned a value that does not match its declared output type.

    Orcapod rejects incompatible returned values (ITL-895): pyarrow would
    otherwise truncate a ``float`` returned for an output declared ``int``. The data
    function raises this before anything is stored or hashed, so a pipeline treats it
    like any other failure of the function: a row failure under ``error_policy``,
    reported to the observer's ``on_data_crash``.

    It subclasses both ``TypeError`` (a value of the wrong type) and ``ValueError``
    (it also wraps conversion failures of a value of the right type, such as an
    integer out of range for the declared width or a naive ``datetime`` under the
    strict timezone policy, which were ``ValueError`` before).

    Attributes:
        function_name: Name of the function pod's function.
        output_key: Name of the output column, or ``None`` if the error concerns
            the whole return.
        declared_type: The declared type of the output or the whole return, if annotated.
        value: The value the function returned for that output. Container mismatches
            retain only the returned type's name.
        detail: Where inside the value the mismatch is, when that is not the whole
            value; ``None`` otherwise.
        hint: What the function should return instead, when there is a specific fix.
    """

    def __init__(
        self,
        function_name: str,
        output_key: str,
        declared_type: object,
        value: object,
        detail: str | None = None,
        hint: str | None = None,
    ) -> None:
        """Create the error for one output of one call.

        Args:
            function_name: Name of the function pod's function.
            output_key: Name of the output column.
            declared_type: The declared type of that output.
            value: The value the function returned for that output.
            detail: Where inside the value the mismatch is, or why the conversion
                failed, when that is not the value as a whole.
            hint: What the function should return instead, when there is a specific
                fix (``Return a float instead: 0.0 or float(value)``).
        """
        self.function_name: str = function_name
        self.output_key: str | None = output_key
        self.declared_type: object = declared_type
        self.value: object = value
        self.detail: str | None = detail
        self.hint: str | None = hint
        where = f": {detail}" if detail else ""
        fix = f". {hint}." if hint else "; convert the value in the function."
        # Types only, never the value: pipelines log this message, and a returned value
        # can be large or sensitive. The value itself stays on ``self.value``.
        super().__init__(
            f"Function {function_name!r} returned {type_text(type(value))} "
            + f"for output {output_key!r}, which is declared "
            + f"{type_text(declared_type)}{where}. The output does not conform{fix}"
        )

    def __reduce__(self) -> tuple[object, ...]:
        """Pickle with the constructor arguments (executors send errors across processes)."""
        return (
            type(self),
            (
                self.function_name,
                self.output_key,
                self.declared_type,
                self.value,
                self.detail,
                self.hint,
            ),
        )


class OutputContainerTypeError(OutputTypeError):
    """A function with multiple output keys returned the wrong container (ITL-1254).

    The mismatch applies to the whole return, before individual outputs are selected.
    Only the returned type is retained, since the invalid value may be a generator or
    another object that cannot cross an executor boundary.

    Attributes:
        returned_type: Display name of the returned value's type.
        return_container: Runtime type required for the returned value.
    """

    def __init__(
        self,
        function_name: str,
        returned_type: str,
        return_container: type,
        annotation: object | None,
    ) -> None:
        """Create the error for the returned container.

        Args:
            function_name: Name of the function pod's function.
            returned_type: Display name of the returned value's type.
            return_container: Runtime type required for the returned value.
            annotation: Declared return annotation, or None if there is none.
        """
        self.function_name = function_name
        self.output_key = None
        self.declared_type = annotation
        self.value = None
        self.detail = None
        self.returned_type = returned_type
        self.return_container = return_container
        container = type_text(return_container)
        target = (
            f"a {container}"
            if return_container in (tuple, list)
            else f"an instance of {container}"
        )
        hint = f"Return {target} with one value per output key, in the order of output_keys"
        if annotation is None:
            declaration = "has no return annotation"
            hint += ", or annotate the return type"
        else:
            declaration = f"declares {type_text(annotation)}"
        self.hint = hint
        TypeError.__init__(
            self,
            f"Function {function_name!r} returned {returned_type} for multiple "
            f"output keys and {declaration}. {self.hint}.",
        )

    def __reduce__(self) -> tuple[object, ...]:
        """Pickle the diagnostic without serializing the invalid returned value."""
        return (
            type(self),
            (
                self.function_name,
                self.returned_type,
                self.return_container,
                self.declared_type,
            ),
        )


class NoOutputsDeclaredError(OutputTypeError):
    """A function that declares no outputs returned a value (ITL-1257).

    A function with no ``output_keys``, annotated ``-> None`` or not annotated, declares
    an empty return: the number of outputs is the number of output keys. A returned value
    other than ``None`` contradicts the declaration, so it is refused rather than
    dropped. It is an ``OutputTypeError``, so a pipeline treats it as one: a row failure
    under ``error_policy``.

    It keeps the attributes of ``OutputTypeError``: ``output_key`` and
    ``declared_type`` are ``None`` (there is no output), ``detail`` is ``None`` and
    ``hint`` says how to fix it.

    Attributes:
        function_name: Name of the function pod's function.
        value: The value the function returned.
    """

    def __init__(self, function_name: str, value: object) -> None:
        """Create the error for one call.

        Args:
            function_name: Name of the function pod's function.
            value: The value the function returned.
        """
        self.function_name: str = function_name
        self.output_key: str | None = None
        self.declared_type: object = None
        self.value: object = value
        self.detail: str | None = None
        self.hint: str | None = (
            "Declare an output key with its type (a return "
            "annotation or output_schema), or return nothing"
        )
        # Types only, never the value (see ``OutputTypeError``).
        TypeError.__init__(
            self,
            f"Function {function_name!r} declares no outputs (no output_keys) but "
            + f"returned {type_text(type(value))}. {self.hint}.",
        )

    def __reduce__(self) -> tuple[object, ...]:
        """Pickle with the constructor arguments (executors send errors across processes)."""
        return (type(self), (self.function_name, self.value))


class UnboundSourceError(RuntimeError):
    """Raised when a data-producing method is called on an unbound SourceNode.

    Occurs when ``iter_data()`` or ``as_table()`` is called on a ``SourceNode``
    that has not been bound to a concrete source in a ``PipelineJob``.
    """


class SourceSpecMismatchError(InputValidationError, ValueError):
    """Raised when a concrete source's schema differs from a source slot's declared schema.

    A source fits a slot when its tag and data columns have the slot's names, roles and
    Python types (ITL-1112): those are what every downstream pipeline hash, and so every
    stored path, is built from. The message names the slot and the column, and for a type
    difference both types. Raised by ``PipelineJob.bind(sources=...)`` and by assigning a
    source node's ``bound_source``, before anything is bound.
    """


class InconsistentSourceError(ValueError):
    """Raised when two source nodes in the same pipeline share a name but differ in schema.

    Source node names are identity-forming: the same name in a given pipeline
    must always refer to the same input slot (same schema).  If ``compile()``
    finds two source nodes with identical names but different schemas — which
    can happen when two ``RootSource`` objects share the same ``source_id`` but
    produce different column sets — it raises this error rather than silently
    renaming one of them.

    Resolution: assign distinct ``source_id`` values to the conflicting sources
    so each slot has a unique, stable identity.
    """


def _in_table(message: str, table: str | None) -> str:
    """Prefix ``message`` with the table it concerns, when known."""
    return message if table is None else f"table {table}: {message}"


class TableSchemaError(ValueError):
    """A SpiralDB table's row in the schema table cannot be read, or disagrees with it.

    A dataset's schema table holds, for each table Orcapod wrote, the schema it is read
    with (ITL-1186). An unknown format or version, a row of another table id, or columns
    the table does not have (or has beyond the row's) are errors, never ignored. Also
    raised for a schema that cannot be recorded.

    Attributes:
        reason: What is wrong.
        table: The table, as ``"<project>/<dataset>/<table>"``, added by the caller that
            knows it; ``None`` otherwise.
    """

    def __init__(self, reason: str, *, table: str | None = None) -> None:
        self.reason = reason
        self.table = table
        super().__init__(_in_table(f"table schema: {reason}", table))


class SpiralDBWriteNotSupportedError(NotImplementedError):
    """Orcapod does not write SpiralDB through spiral-sdk yet (ITL-1186): it reads it.

    Raised by every write of ``SpiralDBConnector`` (creating or deleting a table, storing
    rows), so also by a job whose database is a ``ConnectorArrowDatabase`` on one. Nothing
    is written. Writing is a follow-up of ITL-1186.
    """


class SpiralDBObserverNotSupportedError(SpiralDBWriteNotSupportedError):
    """A status or logging observer was given a SpiralDB store at setup (ITL-1540)."""


class SpiralDBSignInError(RuntimeError):
    """SpiralDB cannot be reached without a person signing in; the message says how.

    Raised before spiral-sdk would start a device sign-in inside a source or a job.
    """


class UnrecordedTableSchemaWarning(UserWarning):
    """A SpiralDB table that holds rows has no row in its dataset's schema table.

    Its columns are read as SpiralDB reports them: every column and struct field
    nullable, with no Arrow metadata. A table written by an Orcapod release on pyspiral
    reads this way until its dataset is migrated
    (``scripts/migrate_spiral_schemas.py``), so its schema and its hashes differ from
    what they were. Usually issued once per table and process: threads reading the
    table at the same time may each issue it.
    """


class NullabilityRecordMismatchError(ValueError):
    """A write declares different non-nullable columns than the table's recorded schema.

    The recorded flags are never widened, narrowed or rewritten by a write.

    Attributes:
        only_in_record: Paths the recorded schema lists that the write declares nullable.
        only_in_write: Paths the write declares non-nullable that it does not list.
        table: The table, added by the caller; ``None`` otherwise.
    """

    def __init__(
        self,
        only_in_record: list[str],
        only_in_write: list[str],
        *,
        table: str | None = None,
    ) -> None:
        self.only_in_record = list(only_in_record)
        self.only_in_write = list(only_in_write)
        self.table = table
        parts = []
        if only_in_record:
            parts.append(
                f"recorded non-nullable but nullable in the write: {only_in_record}"
            )
        if only_in_write:
            parts.append(f"non-nullable in the write but not recorded: {only_in_write}")
        super().__init__(
            _in_table(
                "the write's nullability differs from the table's recorded "
                "schema (" + "; ".join(parts) + ")",
                table,
            )
        )


class NullInNonNullableColumnError(ValueError):
    """A column declared or recorded non-nullable holds a null.

    Attributes:
        column: The column path, e.g. ``"quality.snr"`` or ``"spikes[]"``.
        table: The table, added by the caller; ``None`` otherwise.
    """

    def __init__(self, column: str, *, table: str | None = None) -> None:
        self.column = column
        self.table = table
        super().__init__(
            _in_table(f"column {column!r} is non-nullable but holds nulls", table)
        )


class CursorInvalidatedError(Exception):
    """Raised by a ``DynamicSourceProtocol`` implementation when the previous
    cursor is no longer valid and the source state must be rebuilt from scratch.

    This is a terminal condition for ``PollingSource``. Rows already emitted
    downstream cannot be retracted, so continuing would leave downstream
    operators with a corrupted view. ``PollingSource`` logs the error, calls
    ``impl.close()``, and re-raises so the caller receives the exception rather
    than a silent end-of-stream.

    If full-reset semantics are required, use a static source re-run instead
    of ``PollingSource``.
    """


class PipelineJobRequiredError(RuntimeError):
    """Raised when a lightweight blueprint node is asked to produce data.

    Blueprint nodes (``FunctionNode``, ``OperatorNode``) carry no database
    references.  Wrap the containing ``Pipeline`` in a ``PipelineJob`` to
    obtain executable ``FunctionJobNode`` / ``OperatorJobNode`` variants.
    """


class RowFailuresWarning(UserWarning):
    """Issued once at the end of a job run in which rows failed.

    With the default ``error_policy="continue"``, a row whose function raises is skipped
    and the run goes on. The warning names each node with failed rows, how many failed out
    of how many, and the first error, and says how to print its full traceback. The run's
    ``RunSummary`` (returned by ``PipelineJob.run``, kept as ``job.last_run``) holds the
    same counts.
    """


class SourceCheckFailedWarning(UserWarning):
    """A source could not be checked for new data at the start of a run (ITL-1102).

    Every run (``job.run()``, ``run_async()``, a job node's ``run()``) first asks each
    source feeding it whether its data changed (``check_for_new_data``). A check that
    raises does not fail the run: this warning is issued once per source and run, naming
    the source and the error, the source keeps the data and modified time it had, and
    the run proceeds with that data. The run's ``RunSummary`` lists the source in
    ``unchecked_sources``. A later failure to read the source raises as it always has.
    """


class SourceNameWarning(UserWarning):
    """A given source name starts like the id of a source kind (ITL-1425).

    Every source id is ``<kind>_v<N>:<name>``, and Orcapod always adds the kind part of
    the source that reads the data. A given ``source_id`` is the name: one that starts
    with ``<registered kind>_v<N>:`` (``delta_table_v1:units``) is accepted as it is,
    nested after the reader's own kind part (``csv_v1:delta_table_v1:units``), and this
    warning says so. Give the name alone.
    """


class SourceVersionWarning(UserWarning):
    """A saved source was written by another version of its kind (ITL-1425).

    A saved job records each source's kind, implementation version and name. Loaded
    where the source class declares another version, the source is rebuilt as the
    installed version, so its source id names that version (``delta_table_v2:units``
    for a source saved as ``delta_table_v1:units``): a different source id, a different
    source. The saved id is not rewritten.
    """


class CachedSourceIdMismatchError(ValueError):
    """A ``CachedSource``'s table holds rows of another source id (ITL-1425).

    A cache keeps the rows of one source, with that source's id in every source-info
    token and source-id system tag. A table whose stored rows name another id (most
    often the same source read by another version of its kind, after a saved job is
    loaded where the class declares a newer ``SOURCE_VERSION``) is refused when the
    cache is first read or checked, before the wrapped source is checked or its rows
    are read: once a cache is found to hold another id, no row of the wrapped source
    is read or returned and nothing is written. Building the wrapped source (loading
    a saved cache config) may read it before that. The cache must be rebuilt for the
    new id (a new ``cache_path``, or a new cache database), or the reader's version
    pinned.
    """


class SourceSlotNameCollisionError(ValueError):
    """Two different sources would take one slot name in a job (ITL-1425).

    A job slot is named by its source's name, without the kind part, so two
    different sources (another kind, another version or another id) whose names are
    equal would share one slot and one would replace the other. The job refuses
    them; give one of them its own name (``source_id=`` when it is built).
    """


class RestrictedAggregationWarning(UserWarning):
    """A restricted source feeds an aggregating step of a job run (ITL-1218).

    A restriction (``SpiralDBTableSource.where``) chooses which rows flow without
    changing any identity, so a ``GroupBy``, ``Batch`` or ``MergeJoin`` below it builds
    its groups from the rows let through only. That is allowed: an aggregated row
    records its members, so what a group was computed over stays visible. Issued once
    per run (``job.run()``, ``run_async()``), naming each source slot and step.
    """


class OrcapodDatabaseContentWarning(UserWarning):
    """A loaded node's database holds another number of rows than the saved run recorded.

    A job file may record, in its optional ``run.expected_rows`` section, how many rows
    the database held under each node's run key when the job was saved. Reading a loaded
    node always returns what the attached database holds, which is the source of truth;
    when that differs from the recorded number, this warning is issued once per node.
    The database may not be the one the run wrote, or it may have changed since. A file
    without the section, or without a node's entry, is not checked.
    """


class ProvisionalHashWarning(UserWarning):
    """A value type or an operator was used whose identity is provisional.

    The library value types (``numpy.ndarray``, ``torch.Tensor``, pandas and polars
    ``DataFrame`` and ``Series``) are hashed and stored with formats that may still
    change in v0.4. A result computed now from such a value may then get another hash,
    so it is computed again after the upgrade: nothing stored is lost or wrong. The
    warning is issued once per type per process, on whichever use comes first: a value
    hashed, a column converted to or from Arrow, a function pod annotated with the type,
    a stored column read. Which types are provisional is declared in the ``provisional``
    list of ``orcapod/contexts/logical_types.json``.

    A provisional operator (``orcapod.operators.PROVISIONAL_OPERATORS``, ITL-1266) has an
    identity that is not frozen with data context v1: it may change in a later release,
    and with it the stored paths and record ids of its results and of every step
    downstream of it, which are then computed again. The warning is issued once per
    operator per process, when the operator is constructed.

    A provisional source (``orcapod.sources.PROVISIONAL_SOURCES``, ITL-1314; today
    ``PollingSource``) has a default source id and a content hash that are not frozen
    with data context v1. The warning is issued once per source kind per process, when
    the source is constructed (also from a saved config).

    To silence it, call ``warnings.filterwarnings("ignore",
    category=ProvisionalHashWarning)`` after importing Orcapod. Python's ``-W`` option
    and ``PYTHONWARNINGS`` cannot name this class: they are read before installed
    packages can be imported.
    """


class InDevelopmentWarning(UserWarning):
    """A feature still in development was used: it may change in any release.

    Its interface, behaviour and appearance may change in any release without a
    deprecation period. The warning is issued once per feature per process, the first
    time the feature is used; the message names the feature. Today: the pipeline
    diagram (``Pipeline.diagram()``, ``PipelineJob.diagram()``, ``PipelineDiagram`` and
    the notebook display of a pipeline or job; ITL-1197). A feature that is no longer
    in development stops issuing it.

    To silence it, call ``warnings.filterwarnings("ignore",
    category=InDevelopmentWarning)`` after importing Orcapod. Python's ``-W`` option
    and ``PYTHONWARNINGS`` cannot name this class: they are read before installed
    packages can be imported.
    """


class SemiJoinIgnoredColumnWarning(UserWarning):
    """``SemiJoin`` inputs share a column name that is not a match key.

    ``SemiJoin`` matches on the columns that are tag columns of both inputs and on
    nothing else (ITL-1088). A name the inputs share as data columns, or as a tag on
    one side and a data column on the other, is ignored: it does not take part in
    matching, and the right input's data is never read. This warning names such
    columns, once per operator and set of columns, because before v0.3 they were
    match keys. The behaviour for them is provisional in v0.3 and may become an
    error.

    To silence it, rename or drop the column on the right input before the semi join
    (``map_data``, ``drop_data_columns``, ``select_data_columns``, or ``map_tags``
    for a tag), or call ``warnings.filterwarnings("ignore",
    category=SemiJoinIgnoredColumnWarning)``.
    """


class PolarsFilterWarning(UserWarning):
    """A ``PolarsFilter`` was built: it can change the values it passes through.

    ``PolarsFilter`` passes every column of its input through Polars, also the columns
    its predicates do not read, and Polars does not hold every Arrow type exactly. The
    known case: a ``date64`` value that is not at midnight comes back at midnight, with
    no error (ITL-1296). A ``decimal256`` column, read by a predicate or not, makes
    Polars panic: the filter raises ``pyo3_runtime.PanicException``, a
    ``BaseException`` that ``except Exception`` does not catch, and returns no row.
    ``PolarsFilter`` is not recommended. ``Filter``
    (``stream.filter(...)``) is the operator to use: it chooses rows with an Arrow mask
    and passes every value through as it is.

    The warning is issued every time a ``PolarsFilter`` is built (the class, or
    ``stream.polars_filter(...)``), not once per process, in addition to the
    ``ProvisionalHashWarning`` of a provisional operator.

    To silence it, call ``warnings.filterwarnings("ignore",
    category=PolarsFilterWarning)`` after importing Orcapod.
    """


class UpstreamFailedError(Exception):
    """An input channel's producer failed before it finished (ITL-1347).

    Raised by an ``InputChannel``'s next read once the node that writes into it has
    failed or been cancelled, so a reader never takes a producer's failure for a normal
    end: a barrier operator never computes over part of its input, and no node stores a
    result as if its input were complete. The producer's own error is the cause
    (``__cause__``), and it is the error a run raises: this one only stops the readers.
    """


class RejoinDifferingColumnWarning(UserWarning):
    """``Rejoin`` dropped rows of one origin row because a shared column differed.

    ``Rejoin`` puts rows together when every column its inputs share is equal: the
    system tags, a tag several inputs have, the source info of a shared data-tag and
    the data context (ITL-1355). Rows that come from one origin row (their system tags
    match) and differ in another shared column are not put together: they are left out
    of the result. A branch changed that column, which is rarely meant, so this warning
    names such columns, once per operator and set of columns.

    System tags are assumed unique in each input. Should an input repeat an origin row,
    the repeats are compared by occurrence, the first of one input with the first of
    another; a surplus repeat matches nothing whatever its values, and no warning is
    issued for it.

    To keep the rows, drop or rename the column in one branch before the rejoin
    (``drop_tag_columns``, ``map_tags``). To silence the warning, call
    ``warnings.filterwarnings("ignore", category=RejoinDifferingColumnWarning)``.
    """


class EmptyDataAccessError(Exception):
    """Raised when a payload-access method is called on an ``EmptyData`` instance.

    ``EmptyData`` carries no data payload. Any method that would access the
    underlying columns (``as_dict``, ``as_table``, ``keys``, ``schema``,
    ``arrow_schema``, ``identity_structure``) raises this exception instead of
    returning empty or ``None`` results.

    Attributes:
        empty_data: The ``EmptyData`` instance on which the method was called.
        method_name: The name of the method that was called.
    """

    def __init__(self, empty_data: object, method_name: str) -> None:
        self.empty_data = empty_data
        self.method_name = method_name
        super().__init__(
            f"Cannot call {method_name!r} on EmptyData — "
            "this instance carries no data payload. "
            "Check isinstance(data, EmptyData) before accessing the payload."
        )


class EmptyDataHashMissingError(Exception):
    """Raised when ``content_hash()`` is called on an ``EmptyData`` that has no cached hash.

    An ``EmptyData`` constructed without a ``cached_content_hash`` is in degraded
    mode (old pipeline DB row lacking the hash column). Any code path that tries
    to use the hash fails loudly here.

    Attributes:
        empty_data: The ``EmptyData`` instance on which ``content_hash()`` was called.
    """

    def __init__(self, empty_data: object) -> None:
        self.empty_data = empty_data
        super().__init__(
            "content_hash() called on EmptyData with no cached_content_hash. "
            "The pipeline DB row that produced this token is missing the stored "
            "hash column (OUTPUT_DATA_HASH_COL or INPUT_DATA_HASH_COL). "
            "Flow-through is unavailable for this row."
        )


class EphemeralResultMissingError(Exception):
    """Raised when a downstream pod has a cache miss for an ``EmptyData`` input.

    Indicates that the upstream ephemeral result is gone AND the downstream pod
    has not yet computed a result for the same input content hash. No recovery
    is possible — the information has been lost.

    Attributes:
        tag: The tag associated with the missing input.
        cached_content_hash: The cached content hash from the ``EmptyData``, or
            ``None`` if the ``EmptyData`` itself lacked a hash.
        node_identity_path: Identity path of the downstream node raising this error.
    """

    def __init__(
        self,
        tag: object,
        cached_content_hash: object,
        node_identity_path: tuple[str, ...],
        message: str,
    ) -> None:
        self.tag = tag
        self.cached_content_hash = cached_content_hash
        self.node_identity_path = node_identity_path
        super().__init__(
            f"{message} "
            f"node={node_identity_path!r} "
            f"cached_content_hash={cached_content_hash!r}"
        )


class TableListingNotSupportedError(NotImplementedError):
    """Raised by ``list_table_paths`` when a database cannot enumerate its tables.

    Some databases can read and write a table at a known path but have no way to list
    the tables they hold (for example a Delta database on cloud storage for which no
    file system package is installed). Orcapod then cannot look for tables written
    by an earlier version; it says so in a warning and carries on.
    """


class OrcapodDatabaseError(RuntimeError):
    """Base of the errors raised when the database could not be read or written.

    A database error always fails the run, whatever the orchestrator's
    ``error_policy``: ``error_policy="continue"`` skips rows whose *function*
    raised, never rows whose stored data could not be read or written. Catch
    ``OrcapodDatabaseError`` to handle both ``OrcapodDatabaseReadError`` and ``OrcapodDatabaseWriteError``.

    The backend's own exception is chained as ``__cause__``.

    Attributes:
        node_label: Label of the node the failure belongs to, or ``None`` when it
            is not known or not tied to one node.
        tag: Tag values of the row being processed, or ``None`` when the failure
            is not tied to one row.
    """

    def __init__(
        self,
        *args: object,
        node_label: str | None = None,
        tag: Mapping[str, Any] | None = None,
    ) -> None:
        """Create the error.

        Args:
            *args: The exception arguments, as for any exception.
            node_label: Label of the node the failure belongs to, if known.
            tag: Tag values of the row being processed, if known.
        """
        self.node_label: str | None = node_label
        self.tag: dict[str, Any] | None = dict(tag) if tag is not None else None
        super().__init__(*args)

    @classmethod
    def in_group(cls, group: BaseExceptionGroup) -> list[Self]:
        """Return every error of this class in an exception group, nested ones included.

        Args:
            group: An exception group, such as the one an ``asyncio.TaskGroup``
                raises.

        Returns:
            The matching leaves in the order the group holds them.
        """
        found: list[Self] = []
        for exc in group.exceptions:
            if isinstance(exc, BaseExceptionGroup):
                found.extend(cls.in_group(exc))
            elif isinstance(exc, cls):
                found.append(exc)
        return found

    def add_context(
        self, node_label: str | None = None, tag: Mapping[str, Any] | None = None
    ) -> None:
        """Fill in the node label and row tag where the raising site did not know them.

        Called by the code that runs the node or the row as the error passes
        through it. Values that are already set are kept.

        Args:
            node_label: Label of the node the failure belongs to.
            tag: Tag values of the row being processed.
        """
        if self.node_label is None:
            self.node_label = node_label
        if self.tag is None and tag is not None:
            self.tag = dict(tag)


class StoredTableSchemaError(OrcapodDatabaseError):
    """A table's layout does not permit committing the queued records."""


class OrcapodDatabaseWriteError(OrcapodDatabaseError):
    """Raised when a node's results or records could not be written to the database.

    The run stops at the first failed write. Rows stored before the failure are
    kept, and re-running the job does not recompute stored function results.
    A write that fails because the stored table cannot be read raises
    ``OrcapodDatabaseReadError`` instead.

    Attributes:
        record_path: Path of the table the write was for, relative to the
            database view the writer holds. For a failed flush, the base path of
            the database view that was flushed.
        node_label: Label of the node whose write failed, or ``None`` when the
            failure is not tied to one node (a flush at the end of a run).
        tag: Tag values of the row being stored, or ``None`` when the write
            covered many rows (an operator's output), no row, or was a flush.
        flush: ``True`` when committing pending writes failed. Every view of a
            database shares one set of pending writes, so the write that failed may
            belong to an earlier row or another table than the one being stored.
    """

    def __init__(
        self,
        record_path: tuple[str, ...],
        node_label: str | None = None,
        tag: Mapping[str, Any] | None = None,
        *,
        flush: bool = False,
        table_flush: bool = False,
    ) -> None:
        """Create the error for one failed write.

        Args:
            record_path: Path of the table the write was for, or of the database
                view that was flushed.
            node_label: Label of the node whose write failed, if the write site knows it.
            tag: Tag values of the row being stored, if the write covered one row.
            flush: Whether committing pending writes failed.
            table_flush: Whether the flush was scoped to the given table.
        """
        self.record_path: tuple[str, ...] = tuple(record_path)
        self.flush: bool = flush
        self._table_flush = table_flush
        super().__init__(
            self.record_path, node_label, tag, node_label=node_label, tag=tag
        )

    def add_context(
        self, node_label: str | None = None, tag: Mapping[str, Any] | None = None
    ) -> None:
        """Fill in the node label and row tag if the write site did not know them.

        A failed flush never takes a row tag: the pending write that failed may
        belong to another row.

        Args:
            node_label: Label of the node that owns the write.
            tag: Tag values of the row being stored.
        """
        super().add_context(node_label, None if self.flush else tag)

    def __str__(self) -> str:
        """Describe the failed write: node, row, table and the backend's reason."""
        where = f"node {self.node_label!r}" if self.node_label else "the pipeline"
        row = f" for row {self.tag!r}" if self.tag else ""
        cause = self.__cause__
        reason = (
            f": {type(cause).__name__}: {str(cause).rstrip('.')}"
            if cause is not None
            else ""
        )
        retry = (
            "The run was stopped. Rows stored before this failure are kept; "
            "re-running the job does not recompute stored function results."
        )
        if self.flush:
            node = f" for node {self.node_label!r}" if self.node_label else ""
            if self._table_flush:
                return (
                    f"Could not commit pending writes to the database{node} "
                    f"(table {'/'.join(self.record_path)!r}){reason}. " + retry
                )
            return (
                f"Could not commit pending writes to the database{node} "
                f"(database {'/'.join(self.record_path)!r}){reason}. The failing write "
                "may belong to an earlier row or another table of this database. "
                + retry
            )
        return (
            f"Could not write to the database for {where}{row} "
            f"(table {'/'.join(self.record_path)!r}){reason}. " + retry
        )


class OrcapodDatabaseReadError(OrcapodDatabaseError):
    """Raised when a stored table exists but could not be read.

    A table that does not exist reads as empty. A table that exists but cannot be
    read (a missing or damaged data file, a log the backend cannot load, columns
    whose stored data no longer matches the table schema) raises this error
    instead, so stored results never look dropped. Its rows are not recomputed
    and the table is not overwritten.

    Attributes:
        table: Location of the table that could not be read (a directory or a
            cloud URI).
        record_path: Path of the table, including the database view's prefix.
        node_label: Label of the node whose read failed, or ``None`` when the
            read did not happen while a node ran.
        tag: Tag values of the row being processed when the read failed, or
            ``None`` when the read was not for one row.
    """

    def __init__(self, table: str, record_path: tuple[str, ...]) -> None:
        """Create the error for one table that could not be read.

        Args:
            table: Location of the table (a directory or a cloud URI).
            record_path: Path of the table, including the database view's prefix.
        """
        self.table: str = table
        self.record_path: tuple[str, ...] = tuple(record_path)
        super().__init__(table, self.record_path)

    def __str__(self) -> str:
        """Describe the failed read: table, node, row and the backend's reason."""
        node = f" for node {self.node_label!r}" if self.node_label else ""
        row = f" for row {self.tag!r}" if self.tag else ""
        cause = self.__cause__
        reason = f": {type(cause).__name__}: {cause}" if cause is not None else ""
        return (
            f"Could not read the stored table at {self.table!r}{node}{row}{reason}. "
            "The table exists, so its rows are not treated as missing and the table "
            "was not overwritten."
        )


class SideEffectLogDataContextError(OrcapodDatabaseError):
    """Raised when a side-effecting node finds completions logged under another data context.

    A node whose run is an external action finds what it already did by keys hashed
    under its data context: a side-effect pod in its invocation log, a function pod
    that takes the invocation context (``ctx_arg``) in its function tables.
    Completions stored under another data context cannot be matched with current
    inputs: tables written by Orcapod up to v0.2 (an older table layout, data context
    ``std:v0.1:default``), or rows of another data context in the current tables
    (ITL-944). Running the node would perform every side effect again. Unlike a
    computed result, a side effect cannot be taken back, so the node raises this
    error before it does anything and leaves the tables as they are. Other function,
    operator and source tables do not fail this way: there, such rows are reported,
    skipped and computed again.

    Tables written by Orcapod up to v0.2 are found by name: every table at an older
    layout stored under the node's function or pod name in the job counts,
    whatever hashes its path holds (ITL-954). A table of another function or pod
    with the same name in the same job, or of another node of the same function,
    therefore raises this error too; ``record_paths`` names every table found.

    A fresh database is not a safe way around this error: it holds no completions, so on
    it the node performs every side effect again. Run the node on a fresh database only
    when a repeated delivery is acceptable; otherwise keep using the Orcapod version
    that wrote the tables. Migrating stored rows to the current data context is
    planned.

    Attributes:
        record_paths: Paths, from the root of the database, of the tables holding such
            completions.
        record_path: The first of ``record_paths``.
        found_context: Data context key of the completions that do not match.
        current_context: Data context key of the node.
        node_label: Label of the node.
    """

    def __init__(
        self,
        record_paths: tuple[tuple[str, ...], ...],
        found_context: str,
        current_context: str,
        node_label: str | None = None,
    ) -> None:
        """Create the error for the tables of one node.

        Args:
            record_paths: Paths of the tables holding completions that do not match.
            found_context: Data context key of those completions.
            current_context: Data context key of the node.
            node_label: Label of the node, if known.
        """
        self.record_paths: tuple[tuple[str, ...], ...] = tuple(
            tuple(path) for path in record_paths
        )
        self.record_path: tuple[str, ...] = self.record_paths[0]
        self.found_context: str = found_context
        self.current_context: str = current_context
        super().__init__(
            self.record_paths, found_context, current_context, node_label=node_label
        )

    def __str__(self) -> str:
        """Describe the tables, both data contexts and the remedy."""
        node = f"Node {self.node_label!r}" if self.node_label else "This node"
        tables = ", ".join(repr("/".join(path)) for path in self.record_paths)
        return (
            f"{node} performs side effects, and its stored completions were logged "
            f"under the data context {self.found_context!r} (in {tables}), but the "
            f"node runs under {self.current_context!r}. They cannot be matched with "
            "current inputs, so running the node would perform every side effect "
            "again; nothing was delivered or written. Keep using the Orcapod "
            "version that wrote these tables; they are left in place and can be "
            "migrated to the current data context later. A fresh database holds no "
            "completions, so on it the node performs every side effect again: use "
            "one only when a repeated delivery is acceptable."
        )


class ProvenanceInconsistencyError(ValueError):
    """What a job holds and what its databases store disagree about a row's provenance.

    Raised by ``node.trace`` and ``node.why_missing`` (ITL-1432) when a link they follow
    cannot be proven: a group's or batch's members whose system tags do not fold to the
    group's folded values, a row with no stored pipeline entry while the node has a
    database, or a stored entry whose input is not the row the node holds. The tools never
    guess a link; the message names the node and what disagreed.
    """
