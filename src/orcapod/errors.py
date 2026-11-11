class InputValidationError(Exception):
    """
    Exception raised when the inputs are not valid.
    This is used to indicate that the inputs do not meet the requirements of the operator.
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


class DuplicateTagError(ValueError):
    """Raised when duplicate tag values are found and skip_duplicates=False"""

    pass


class DataFunctionUnavailableError(RuntimeError):
    """Raised when a data function proxy is invoked without a bound function.

    This occurs when a pipeline is loaded in an environment where the
    original data function is not available. Only cached results can
    be accessed.
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

    Orcapod never converts a returned value implicitly (ITL-895): pyarrow would
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
        output_key: Name of the output column.
        declared_type: The declared type of that output.
        value: The value the function returned for that output.
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
        self.output_key: str = output_key
        self.declared_type: object = declared_type
        self.value: object = value
        self.detail: str | None = detail
        self.hint: str | None = hint
        where = f": {detail}" if detail else ""
        value_repr = repr(value)
        if len(value_repr) > 80:
            value_repr = value_repr[:77] + "..."
        fix = f". {hint}." if hint else "; convert the value in the function."
        super().__init__(
            f"Function {function_name!r} returned {type_text(type(value))} "
            + f"({value_repr}) for output {output_key!r}, which is declared "
            + f"{type_text(declared_type)}{where}. Orcapod does not convert types "
            + f"implicitly{fix}"
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


class OutputConversionWarning(UserWarning):
    """A function returned a near miss for an output, and Orcapod converted it.

    A few values whose type is not exactly the declared type are accepted and converted
    (ITL-895): a ``tuple`` for ``list[T]`` becomes a list, a ``datetime`` for ``date``
    loses its time of day, a Python ``float`` for ``numpy.float32`` is rounded to 32
    bits. The stored value, and the value a downstream pod receives, is the converted
    one. The message names the types, never the value, so Python's default warning
    filter shows it once per function, output and conversion, however many rows a run
    has. To make it an error (a row failure in a pipeline), run with
    ``-W error::orcapod.errors.OutputConversionWarning``.

    Attributes:
        function_name: Name of the function pod's function.
        output_key: Name of the output column.
        location: Where inside the returned value, by structure (``[*]`` for any list
            element or dict value); empty for the value as a whole.
        declared_type: The declared type at that place.
        returned_type: The type of the returned value at that place.
        conversion: What Orcapod did to the value (``converted it to a list``).
        hint: How the function avoids the conversion (``Return value.date()``).
    """

    def __init__(
        self,
        function_name: str,
        output_key: str,
        location: str,
        declared_type: object,
        returned_type: type,
        conversion: str,
        hint: str,
    ) -> None:
        """Create the warning for one conversion of one output.

        Args:
            function_name: Name of the function pod's function.
            output_key: Name of the output column.
            location: Where inside the returned value, by structure; empty for the
                value as a whole.
            declared_type: The declared type at that place.
            returned_type: The type of the returned value at that place.
            conversion: What Orcapod did to the value.
            hint: How the function avoids the conversion.
        """
        self.function_name: str = function_name
        self.output_key: str = output_key
        self.location: str = location
        self.declared_type: object = declared_type
        self.returned_type: type = returned_type
        self.conversion: str = conversion
        self.hint: str = hint
        at = f" at {location}" if location else ""
        super().__init__(
            f"Function {function_name!r} returned {type_text(returned_type)} for "
            + f"output {output_key!r}{at}, which is declared "
            + f"{type_text(declared_type)}; Orcapod {conversion}. {hint} to avoid "
            + "this warning."
        )

    def __reduce__(self) -> tuple[object, ...]:
        """Pickle with the constructor arguments (executors send warnings across processes)."""
        return (
            type(self),
            (
                self.function_name,
                self.output_key,
                self.location,
                self.declared_type,
                self.returned_type,
                self.conversion,
                self.hint,
            ),
        )


class FieldNotResolvableError(LookupError):
    """
    Raised when a source cannot resolve a field value for a given record ID.

    This may happen because:
    - The source is transient or randomly generated (no stable backing data)
    - The record ID is not found in the source
    - The field name does not exist in the source schema
    - The source type does not support field resolution

    The exception message should describe which condition applies.
    """

    pass


class UnboundSourceError(RuntimeError):
    """Raised when a data-producing method is called on an unbound SourceNode.

    Occurs when ``iter_data()`` or ``as_table()`` is called on a ``SourceNode``
    that has not been bound to a concrete source in a ``PipelineJob``.
    """


class SourceSpecMismatchError(ValueError):
    """Raised when a concrete source's schema is incompatible with a SourceNode slot.

    The class name ``SourceSpecMismatchError`` is preserved for compatibility
    with any code that catches it by name.

    Contains the slot name and a description of the incompatible field(s).
    Raised at ``bind()`` time — schema mismatches are rejected before execution.
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


class SchemaVersionError(Exception):
    """Raised when an old schema version is detected at a database path.

    Occurs when a table exists at the unversioned (v0) path but not at the
    expected versioned path, and the node has not opted in to tolerating
    the old schema via ``NodeConfig.ignore_schema``.

    To suppress this error, set ``node.node_config = NodeConfig(ignore_schema=("v0",))``
    and re-run. The node will recompute all results from scratch rather than
    reading from the v0 cache.
    """


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
