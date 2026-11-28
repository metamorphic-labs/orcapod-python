from __future__ import annotations

import inspect
import io
import logging
import re
import sys
import tokenize
import types
import uuid
from abc import abstractmethod
from collections.abc import Callable, Iterable, Sequence
import typing
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, TypeVar

import pyarrow as pa
from uuid_utils import uuid7

from orcapod.config import OrcapodConfig
from orcapod.contexts import DataContext
from orcapod.contexts.stored import require_same_data_context
from orcapod.core.base import TraceableBase
from orcapod.core.data_function_types import DataFunctionTypeDeclaration, StoredColumn
from orcapod.core.datagrams import Datagram, Data
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    DEFAULT_VALUE_REFUSALS,
    default_value_refused,
    hash_default_value,
)
from orcapod.core.result_cache import ResultCache, context_result_path
from orcapod.errors import (
    NoOutputsDeclaredError,
    OutputContainerTypeError,
    OutputTypeError,
    UnstorableDatetimeError,
)
from orcapod.protocols.core_protocols import DataFunctionProtocol, DataProtocol
from orcapod.protocols.core_protocols.executor import (
    DataFunctionExecutorProtocol,
    PythonFunctionExecutorProtocol,
    PythonFunctionReturnCheckProtocol,
)
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.semantic_types.output_conformance import (
    check_conformance,
    normalize_output_numbers,
)
from orcapod.types import ContentHash, DataValue, Schema, SchemaLike
from orcapod.utils import schema_utils
from orcapod.utils.canonical_type_names import stabilize_type_names
from orcapod.utils.git_utils import get_git_info_for_python_object
from orcapod.utils.output_containers import (
    UnexpectedOutputContainer,
    output_container_mismatch,
)
from orcapod.utils.tensor_conversions import describe, tensor_conversions, warn_once

from orcapod.protocols.observability_protocols import DataExecutionLoggerProtocol

if TYPE_CHECKING:
    from orcapod.logical_types.protocols import (
        TypeConverterProtocol as LogicalTypeConverterProtocol,
    )

module_logger = logging.getLogger(__name__)

error_handling_options = Literal["raise", "ignore", "warn"]

# ---------------------------------------------------------------------------
# Shared executor for running async functions synchronously from within
# an event loop (see PythonDataFunction._call_async_function_sync).
# ---------------------------------------------------------------------------

_sync_executor = None


def _get_sync_executor():
    """Return a shared single-thread executor for sync fallback of async fns."""
    global _sync_executor
    if _sync_executor is None:
        from concurrent.futures import ThreadPoolExecutor

        _sync_executor = ThreadPoolExecutor(1)
    return _sync_executor


def parse_function_outputs(
    output_keys: Sequence[str], values: Any, return_container: type = tuple
) -> dict[str, DataValue]:
    """
    Map raw function return values to a keyed output dict.

    Rules:
        - ``output_keys = []``: empty dict returned. ``PythonDataFunction`` refuses a
          returned value other than ``None`` before this is called (ITL-1257).
        - ``output_keys = ["result"]``: any value (including iterables) is stored as-is
          under the single key.
        - ``output_keys = ["a", "b", ...]``: ``values`` must match the declared container
          and hold one value per key.

    Args:
        output_keys: Ordered list of output key names.
        values: Raw return value from the function.
        return_container: Runtime type required for several outputs.

    Returns:
        Dict mapping each output key to its corresponding value.

    Raises:
        UnexpectedOutputContainer: If the value is not an accepted output container.
        ValueError: If the number of values does not match the number of keys.
    """
    if len(output_keys) == 0:
        output_values: list[Any] = []
    elif len(output_keys) == 1:
        output_values = [values]
    else:
        returned_type = output_container_mismatch(values, return_container)
        if returned_type is not None:
            raise UnexpectedOutputContainer(returned_type)
        output_values = list(values)

    if len(output_values) != len(output_keys):
        raise ValueError(
            f"Number of output keys {len(output_keys)}:{output_keys} does not match "
            f"number of values returned by function {len(output_values)}"
        )

    return dict(zip(output_keys, output_values))


E = TypeVar("E", bound=DataFunctionExecutorProtocol)


class DataFunctionBase(TraceableBase, Generic[E]):
    """Abstract base class for DataFunctionProtocol.

    Type-parameterized with the executor protocol ``E``.  Concrete
    subclasses that bind ``E`` (e.g. ``class Foo(DataFunctionBase[SomeProto])``)
    get automatic ``isinstance`` validation in ``set_executor`` at class
    definition time via ``__init_subclass__``.
    """

    _resolved_executor_protocol: ClassVar[type | None] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for base in getattr(cls, "__orig_bases__", ()):
            origin = typing.get_origin(base)
            if origin is DataFunctionBase:
                args = typing.get_args(base)
                if args and not isinstance(args[0], TypeVar):
                    cls._resolved_executor_protocol = args[0]
                    return

    def __init__(
        self,
        version: str = "v0.0",
        label: str | None = None,
        data_context: str | DataContext | None = None,
        config: OrcapodConfig | None = None,
        executor: DataFunctionExecutorProtocol | None = None,
    ):
        super().__init__(label=label, data_context=data_context, config=config)
        self._active = True
        self._version = version
        self._executor: E | None = None

        # Parse version string to extract major and minor versions
        # 0.5.2 -> 0 and 5.2, 1.3rc -> 1 and 3rc
        match = re.match(r"\D*(\d+)\.(.*)", version)
        if match:
            self._major_version = int(match.group(1))
            self._minor_version = match.group(2)
        else:
            raise ValueError(
                f"Version string {version} does not contain a valid version number"
            )

        self._output_data_schema_hash = None

        # Validate and set via the property setter.  This works because
        # concrete subclasses define data_function_type_id as a simple
        # constant property that does not depend on instance state set
        # *after* super().__init__().
        if executor is not None:
            self.executor = executor

    def computed_label(self) -> str | None:
        """Return the canonical function name as the label if no explicit label is given."""
        return self.canonical_function_name

    @property
    def output_data_schema_hash(self) -> str:
        """Return the hash of the output data schema as a bare hex digest.

        The schema is hashed as the Arrow schema the data context's type converter
        gives the declared output types (the Arrow types the function's results are
        stored with), by ``arrow_hasher.hash_schema`` (ITL-640); that labelled hash is
        hashed by the semantic hasher, as every hash fed into another. The digest is
        part of the function ``uri`` and so of table paths and source-info tokens: a
        hash inside a name carries no label. It is computed lazily on first access and cached for
        subsequent calls.

        Returns:
            The hex digest of the output data schema hash, without a label.
        """
        if self._output_data_schema_hash is None:
            context = self.data_context
            arrow_schema = schema_utils.declared_arrow_schema(
                self.output_data_schema, context.type_converter
            )
            # The labelled schema hash is fed into the semantic hasher as a one-schema
            # tuple (a hash fed into a hash carries its label; a source's system-tag
            # names hash the tag and data pair the same way), and the name holds the
            # bare digest.
            schema_hash = context.arrow_hasher.hash_schema(arrow_schema)
            self._output_data_schema_hash = context.semantic_hasher.hash_object(
                (schema_hash,)
            ).to_hex()
        return self._output_data_schema_hash

    @property
    def declaration(self) -> DataFunctionTypeDeclaration:
        """The declaration of this function's data function type (ITL-1259).

        Looked up by ``data_function_type_id``: the type and its major. It gives the
        revision of the result table and the columns the type stores.
        """
        return declaration_for(self.data_function_type_id)

    @property
    def uri(self) -> tuple[str, ...]:
        return (
            self.canonical_function_name,
            self.output_data_schema_hash,
            f"v{self.major_version}",
            self.data_function_type_id,
        )

    def identity_structure(self) -> Any:
        return self.uri

    def pipeline_identity_structure(self) -> Any:
        return self.uri

    @property
    def major_version(self) -> int:
        return self._major_version

    @property
    def minor_version_string(self) -> str:
        return self._minor_version

    @property
    @abstractmethod
    def data_function_type_id(self) -> str:
        """Unique function type identifier (e.g. ``"python.function.v1"``)."""
        ...

    @property
    @abstractmethod
    def canonical_function_name(self) -> str:
        """Human-readable function identifier."""
        ...

    @property
    @abstractmethod
    def input_data_schema(self) -> Schema:
        """Schema describing the input data this function accepts."""
        ...

    @property
    def accepts_arbitrary_keywords(self) -> bool:
        """Whether the function takes input columns beyond ``input_data_schema``.

        ``False`` for every data function kind today: the parameters are a fixed
        set (``PythonDataFunction`` rejects ``**kwargs``). A kind that passes every
        input column to its function overrides this to return ``True``; a function
        pod then never drops a column for it (ITL-807).
        """
        return False

    @property
    @abstractmethod
    def output_data_schema(self) -> Schema:
        """Schema describing the output data this function produces."""
        ...

    @abstractmethod
    def get_function_variation_data(self) -> dict[str, Any]:
        """Raw data defining function variation"""
        ...

    @abstractmethod
    def get_execution_data(self) -> dict[str, Any]:
        """Raw data defining execution context"""
        ...

    @abstractmethod
    def get_function_variation_data_schema(self) -> Schema:
        """Schema for the data returned by ``get_function_variation_data``."""
        ...

    @abstractmethod
    def get_execution_data_schema(self) -> Schema:
        """Schema for the data returned by ``get_execution_data``."""
        ...

    # ==================== Executor ====================

    @property
    def executor(self) -> E | None:
        """Return the executor used to run this data function, or ``None`` for direct execution."""
        return self._executor

    @executor.setter
    def executor(self, executor: E | None) -> None:
        """Set or clear the executor for this data function.

        Delegates to ``set_executor`` for validation.
        """
        self.set_executor(executor)

    def set_executor(self, executor: DataFunctionExecutorProtocol | None) -> None:
        """Set or clear the executor, validating type compatibility.

        Performs two checks:
        1. The executor supports this function's ``data_function_type_id``.
        2. If the subclass bound ``E`` via ``Generic[E]``, the executor is an
           instance of the resolved protocol (checked once at assignment time,
           not in the hot path).

        Raises:
            TypeError: If *executor* fails either compatibility check.
        """
        if executor is not None:
            if not executor.supports(self.data_function_type_id):
                raise TypeError(
                    f"Executor {executor.executor_type_id!r} does not support "
                    f"data function type {self.data_function_type_id!r}. "
                    f"Supported types: {executor.supported_function_type_ids()}"
                )
            proto = getattr(type(self), "_resolved_executor_protocol", None)
            if proto is not None and not isinstance(executor, proto):
                raise TypeError(
                    f"{type(self).__name__} requires an executor implementing "
                    f"{proto.__name__}, got {type(executor).__name__}"
                )
        self._executor = executor

    # ==================== Execution ====================

    def call(
        self,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        **kwargs: Any,
    ) -> DataProtocol | None:
        """Process a single data.

        Base implementation calls ``direct_call``.  Subclasses that support
        executors (e.g. ``PythonDataFunction``) override to route through
        the executor's ``execute_callable``.

        ``**kwargs`` are forwarded to ``direct_call`` and merged into the
        function call dict alongside the data fields.  Callers (e.g. a pod)
        use this to supply auto-provided arguments such as an
        ``InvocationContext`` that are not part of the data schema.
        """
        if logger is not None and self._executor is None:
            import warnings

            warnings.warn(
                "A logger was passed but no executor is set — "
                "capture will not occur. Set an executor to enable logging.",
                stacklevel=2,
            )
        return self.direct_call(data, **kwargs)

    async def async_call(
        self,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        **kwargs: Any,
    ) -> DataProtocol | None:
        """Asynchronously process a single data.

        Base implementation calls ``direct_async_call``.  Subclasses that
        support executors override to route through the executor.

        ``**kwargs`` are forwarded to ``direct_async_call`` and merged into
        the function call dict alongside the data fields.
        """
        if logger is not None and self._executor is None:
            import warnings

            warnings.warn(
                "A logger was passed but no executor is set — "
                "capture will not occur. Set an executor to enable logging.",
                stacklevel=2,
            )
        return await self.direct_async_call(data, **kwargs)

    @abstractmethod
    def direct_call(self, data: DataProtocol, **kwargs: Any) -> DataProtocol | None:
        """Execute the function's native computation on *data*.

        This is the method executors invoke.  It bypasses executor routing
        and runs the computation directly.  On user-function failure the
        exception is re-raised.  Subclasses must implement this.

        ``**kwargs`` are merged with ``data.as_dict()`` before the underlying
        callable is invoked.
        """
        ...

    @abstractmethod
    async def direct_async_call(
        self, data: DataProtocol, **kwargs: Any
    ) -> DataProtocol | None:
        """Asynchronous counterpart of ``direct_call``."""
        ...


FUNCTION_SIGNATURE_HASH_LABEL = "function_signature_v1"
"""Label of the stored function signature hash (``function_signature_hash``)."""

FUNCTION_CONTENT_HASH_LABEL = "function_content_v1"
"""Label of the stored function content hash (``function_content_hash``)."""


def function_signature_hash(
    function: Callable[..., Any],
    input_schema: Schema,
    output_schema: Schema,
    hasher: Any,
) -> ContentHash:
    """The hash of a function's declared interface, as a data function runs it.

    One hash over two named components, built from the effective values (ITL-1256):

    ``{"inputs": {<name>: {"type": <type hash>, "default": <value hash>}},
    "outputs": [{"key": <output key>, "type": <type hash>}, ...]}``

    Inputs are keyed by name and in no order (a pod binds its inputs by name); a
    parameter without a default has no ``"default"`` entry. Outputs are in declared
    order: the mapping of the returned positions to the output keys. Not in it: the
    function's name and module, the parameters' kind and order, the version, the body.

    Args:
        function: The function, for its parameters' defaults.
        input_schema: The effective input types (``input_schema=``, else annotations).
        output_schema: The effective output types, keys in declared order.
        hasher: The semantic hasher.

    Returns:
        The hash, labelled ``FUNCTION_SIGNATURE_HASH_LABEL``.

    Raises:
        TypeError: If the hasher refuses a parameter's default (any of
            ``DEFAULT_VALUE_REFUSALS``: a ``TypeError`` or a ``ValueError`` such as
            ``UnsupportedValueError``); the message names the parameter and the
            function, and the hasher's error is its cause.
    """
    parameters = inspect.signature(function).parameters
    inputs: dict[str, dict[str, ContentHash]] = {}
    for name, python_type in input_schema.items():
        entry = {"type": hasher.hash_object(python_type)}
        parameter = parameters.get(name)
        if parameter is not None and parameter.default is not inspect.Parameter.empty:
            try:
                entry["default"] = hash_default_value(parameter.default, hasher)
            except DEFAULT_VALUE_REFUSALS as exc:
                raise default_value_refused(name, function, exc) from exc
        inputs[name] = entry
    outputs = [
        {"key": key, "type": hasher.hash_object(python_type)}
        for key, python_type in output_schema.items()
    ]
    digest = hasher.hash_object({"inputs": inputs, "outputs": outputs}).digest
    return ContentHash(FUNCTION_SIGNATURE_HASH_LABEL, digest)


def function_source(function: Callable[..., Any]) -> str | None:
    """A function's source text from its ``def`` line on, as written.

    Docstring, comments and whitespace are kept; decorator lines are not. For a lambda
    the enclosing source line (or lines) count as its source, as ``inspect`` gives them.

    The ``def`` line is found by tokenizing the text as written (``_def_line_index``),
    never by dedenting or parsing it, so what the body holds cannot move the cut.

    Args:
        function: The function.

    Returns:
        The text, or ``None`` when the source cannot be read (a function defined in a
        plain REPL, with ``python -c`` or through ``exec``) or its ``def`` line cannot
        be found: never a text that holds a decorator line.
    """
    try:
        source = inspect.getsource(function)
    except (OSError, TypeError):
        return None
    if _is_lambda(function):
        return source
    lines = io.StringIO(source).readlines()
    index = _def_line_index(lines)
    if index is None:
        return None
    return "".join(lines[index:])


def _is_lambda(function: Callable[..., Any]) -> bool:
    """Whether the function ``inspect.getsource`` reads is a lambda."""
    code: object = getattr(inspect.unwrap(function), "__code__", None)
    return isinstance(code, types.CodeType) and code.co_name == "<lambda>"


_LINE_LAYOUT_TOKENS = frozenset(
    {tokenize.NL, tokenize.COMMENT, tokenize.INDENT, tokenize.DEDENT}
)
_OPENING_BRACKETS = frozenset("([{")
_CLOSING_BRACKETS = frozenset(")]}")


def _def_line_index(lines: list[str]) -> int | None:
    """The index in ``lines`` of the line where the function's ``def`` statement starts.

    ``lines`` is the source of one function as ``inspect.getsource`` gives it: decorator
    statements, then the ``def`` (or ``async def``) statement. The ``def`` statement is
    the first logical line that is not a decorator, found by tokenizing the text as
    written: a string, comment or bracket that spans lines stays inside its decorator.
    Tokens are read lazily, up to the ``def`` line only.

    Returns:
        The index, or ``None`` when the first logical line that is not a decorator is no
        ``def`` statement or the text cannot be tokenized.
    """
    remaining = iter(lines)
    depth = 0
    at_logical_line_start = True
    async_row: int | None = None
    try:
        for token in tokenize.generate_tokens(lambda: next(remaining, "")):
            if token.type in _LINE_LAYOUT_TOKENS:
                continue
            if token.type == tokenize.NEWLINE:
                at_logical_line_start = True
                continue
            if async_row is not None:
                return async_row - 1 if token.string == "def" else None
            if at_logical_line_start and depth == 0:
                at_logical_line_start = False
                if token.string == "def":
                    return token.start[0] - 1
                if token.string == "async":
                    async_row = token.start[0]
                    continue
                if token.string != "@":
                    return None
            if token.type == tokenize.OP:
                if token.string in _OPENING_BRACKETS:
                    depth += 1
                elif token.string in _CLOSING_BRACKETS:
                    depth -= 1
    except (tokenize.TokenError, SyntaxError):
        return None
    return None


def function_content_hash(
    function: Callable[..., Any], hasher: Any
) -> ContentHash | None:
    """The hash of a function as written: ``{"source": <function_source text>}``.

    Args:
        function: The function.
        hasher: The semantic hasher.

    Returns:
        The hash, labelled ``FUNCTION_CONTENT_HASH_LABEL``, or ``None`` when
        ``function_source`` gives no text (the source cannot be read, or the source of a
        function other than a lambda does not start at a ``def`` statement): the record
        then holds no content hash rather than one that would claim a match it cannot
        support.
    """
    source = function_source(function)
    if source is None:
        return None
    digest = hasher.hash_object({"source": source}).digest
    return ContentHash(FUNCTION_CONTENT_HASH_LABEL, digest)


_TEXT = "large_string"
_BINARY = "large_binary"
_TEXT_MAP = "large_list<item: struct<key: large_string, value: large_string>>"


PYTHON_FUNCTION = DataFunctionTypeDeclaration(
    type_name="python.function",
    major=1,
    revision=1,
    identity=(
        "the function's name: its __name__, or the function_name given",
        "the hash of the output schema: each output key with its type, in no order",
        "v<major> of the function's own version (the part before the first dot)",
        "the type and its major: python.function.v1",
    ),
    run_rules=(
        "inputs are bound to the function's parameters by name, as keyword arguments",
        "a parameter with a default may be left out of the inputs",
        "the returned value is mapped to the output keys by position: one key takes "
        + "the whole value, several keys take the items of a returned sequence",
        "a function with no output keys (-> None or no return annotation) has no "
        + "outputs; a returned value other than None is refused",
        "a returned value must conform to its declared output type",
        "variadic (*args, **kwargs) and positional-only parameters are refused",
    ),
    variation=(
        StoredColumn(
            "function_name",
            str,
            _TEXT,
            True,
            "the function's name, as in the uri",
        ),
        StoredColumn(
            "function_version",
            str,
            _TEXT,
            True,
            "the full version string the function was declared with (v3.7)",
        ),
        StoredColumn(
            "function_module",
            str | None,
            _TEXT,
            True,
            "the function's module (__module__)",
        ),
        StoredColumn(
            "function_signature_hash",
            bytes,
            _BINARY,
            True,
            "the hash of the function's declared interface",
            hash_label=FUNCTION_SIGNATURE_HASH_LABEL,
            covers='{"inputs": {<name>: {"type": <type hash>, "default": <value hash, '
            + 'only when the parameter has one>}}, "outputs": [{"key": <output key>, '
            + '"type": <type hash>}]}: effective types (input_schema= / output_schema=, '
            + "else the annotations); inputs keyed by name in no order, outputs in "
            + "declared order; not the name, module, parameter kind or order, version "
            + "or body",
        ),
        StoredColumn(
            "function_content_hash",
            bytes | None,
            _BINARY,
            True,
            "the hash of the function as written (a lambda: its enclosing lines); absent "
            + "when its source cannot be read (a plain REPL, python -c, exec) or, for a "
            + "function other than a lambda, does not start at a def statement",
            hash_label=FUNCTION_CONTENT_HASH_LABEL,
            covers='{"source": <the source text from the def line on, as written: '
            + "docstring, comments and whitespace included, decorator lines excluded; "
            + "for a lambda the enclosing line>}",
        ),
        StoredColumn(
            "git_hash",
            str,
            _TEXT,
            True,
            "the git commit of the function's file or working directory, with "
            + "-dirty, -untracked and -<source> suffixes; unknown outside git",
        ),
    ),
    execution=(
        StoredColumn(
            "executor_type",
            str,
            _TEXT,
            True,
            "the executor that ran the function (local, ray.v0, none)",
        ),
        StoredColumn(
            "executor_info",
            dict[str, str],
            _TEXT_MAP,
            True,
            "the executor's own settings, as text",
        ),
        StoredColumn(
            "python_version",
            str,
            _TEXT,
            True,
            "the Python version that ran the function (3.12.3)",
        ),
        StoredColumn(
            "extra_info",
            dict[str, str],
            _TEXT_MAP,
            True,
            "further facts about the run, as text; empty today",
        ),
    ),
)
"""The Python function type: ``python.function.v1``, record ``rev1``."""


DECLARATIONS: dict[str, DataFunctionTypeDeclaration] = {
    declaration.type_id: declaration for declaration in (PYTHON_FUNCTION,)
}
"""Every data function type this version of Orcapod knows, by type id."""


def declaration_for(type_id: str) -> DataFunctionTypeDeclaration:
    """The declaration of the data function type ``type_id``.

    Args:
        type_id: A type and its major, as the last part of a function's uri
            (``python.function.v1``).

    Returns:
        The declaration this version of Orcapod has for that type.

    Raises:
        ValueError: If no data function type has that id.
    """
    try:
        return DECLARATIONS[type_id]
    except KeyError:
        raise ValueError(
            f"Unknown data function type {type_id!r}; this version of Orcapod knows "
            + ", ".join(repr(known) for known in sorted(DECLARATIONS))
        ) from None


def output_source_info(
    uri: tuple[str, ...], result_id: str, columns: Iterable[str]
) -> dict[str, str]:
    """Build the source-info token of each output column of one result.

    A token is ``<function uri>::<result id>::<column>``, the uri's parts joined
    by ``:``. This is the one place an output's tokens are built: a computed
    result gives its id as hex text, and the placeholder of an ephemeral result
    that is gone gives the sentinel ``row_ids.EPHEMERAL_RESULT_GONE`` in its
    place (ITL-1471).

    Args:
        uri: The data function's uri.
        result_id: The text that names the result: the hex of its id, or the
            sentinel.
        columns: The output columns.

    Returns:
        The token of each column, by column name, in the order given.
    """
    source = ":".join(uri)
    return {column: f"{source}::{result_id}::{column}" for column in columns}


class PythonDataFunction(DataFunctionBase[PythonFunctionExecutorProtocol]):
    @property
    def data_function_type_id(self) -> str:
        """The type and its major, from the type's declaration (``python.function.v1``)."""
        return PYTHON_FUNCTION.type_id

    def __init__(
        self,
        function: Callable[..., Any],
        output_keys: str | Sequence[str] | None = None,
        function_name: str | None = None,
        version: str = "v0.0",
        input_schema: SchemaLike | None = None,
        output_schema: SchemaLike | Sequence[type] | None = None,
        label: str | None = None,
        data_context: str | DataContext | None = None,
        config: OrcapodConfig | None = None,
        executor: PythonFunctionExecutorProtocol | None = None,
    ) -> None:

        # default to the basic PythonFunctionExecutor
        if executor is None:
            from orcapod.core.executors.local import LocalPythonFunctionExecutor

            executor = LocalPythonFunctionExecutor()

        self._function = function
        self._is_async = inspect.iscoroutinefunction(function)
        # Reject functions with variadic parameters -- PythonDataFunction maps
        # data keys to named parameters, so the full parameter set must be fixed.
        _sig = inspect.signature(function)
        _variadic = [
            name
            for name, param in _sig.parameters.items()
            if param.kind
            in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            )
        ]
        if _variadic:
            raise ValueError(
                f"PythonDataFunction does not support functions with variadic "
                f"parameters (*args / **kwargs). "
                f"Offending parameters: {_variadic!r}."
            )
        # A pod passes its inputs by name, so a positional-only parameter could never
        # be given (ITL-1258).
        _positional_only = [
            name
            for name, param in _sig.parameters.items()
            if param.kind is inspect.Parameter.POSITIONAL_ONLY
        ]
        if _positional_only:
            raise ValueError(
                f"PythonDataFunction does not support functions with positional-only "
                f"parameters: a function pod passes its inputs by name. "
                f"Offending parameters: {_positional_only!r}; remove the '/' from the "
                f"signature."
            )

        if output_keys is None:
            output_keys = []
        if isinstance(output_keys, str):
            output_keys = [output_keys]
        # A copy taken once: a later change to the caller's list changes nothing.
        output_keys = tuple(output_keys)
        self._output_keys = output_keys
        if function_name is None:
            if hasattr(self._function, "__name__"):
                function_name = getattr(self._function, "__name__")
            else:
                raise ValueError(
                    "function_name must be provided if function has no __name__"
                )

        assert function_name is not None
        self._function_name = function_name
        # (output key, conversion) pairs already reported with ValueConversionWarning.
        self._reported_conversions: set[tuple[str, str]] = set()

        super().__init__(
            label=label,
            version=version,
            data_context=data_context,
            config=config,
            executor=executor,
        )

        # extract input and output schema from the function signature
        (
            self._input_schema,
            self._output_schema,
            self._return_container,
            self._container_annotation,
        ) = schema_utils.extract_function_schemas_with_container(
            self._function,
            self._output_keys,
            input_typespec=input_schema,
            output_typespec=output_schema,
        )

        # get git info for the function
        # TODO: turn this into optional addition
        env_info = get_git_info_for_python_object(self._function, try_cwd=True)
        if env_info is None:
            git_hash = "unknown"
        else:
            git_hash = env_info.get("git_commit_hash", "unknown")
            if env_info.get("git_repo_status") == "dirty":
                git_hash += "-dirty"
            if env_info.get("has_untracked_files") == "true":
                git_hash += "-untracked"
            if (
                git_source := env_info.get("git_source")
            ) is not None and git_source != "function":
                git_hash += f"-{git_source}"
        self._git_hash = git_hash

        semantic_hasher = self.data_context.semantic_hasher
        # The record of PYTHON_FUNCTION rev1 (ITL-1256): the declared interface and the
        # function as written, each with its own label.
        self._function_module: str | None = getattr(function, "__module__", None)
        self._function_signature_hash = function_signature_hash(
            function, self._input_schema, self._output_schema, semantic_hasher
        ).to_prefixed_digest()
        content_hash = function_content_hash(function, semantic_hasher)
        self._function_content_hash: bytes | None = (
            None if content_hash is None else content_hash.to_prefixed_digest()
        )

    @property
    def canonical_function_name(self) -> str:
        """Human-readable function identifier."""
        return self._function_name

    def get_function_variation_data(self) -> dict[str, Any]:
        """Raw data defining function variation, stored with every result row.

        ``function_version`` is the full version string the function was declared
        with (``"v3.7"``); only its major part is in the table path.
        """
        return {
            "function_name": self._function_name,
            "function_version": self._version,
            "function_module": self._function_module,
            "function_signature_hash": self._function_signature_hash,
            "function_content_hash": self._function_content_hash,
            "git_hash": self._git_hash,
        }

    def get_function_variation_data_schema(self) -> Schema:
        """Schema for the data returned by ``get_function_variation_data``.

        Built from the type's declaration (``PYTHON_FUNCTION.variation``).
        """
        return Schema({c.name: c.python_type for c in PYTHON_FUNCTION.variation})

    def get_execution_data(self) -> dict[str, Any]:
        """Raw data defining execution context."""
        import json

        executor = self._executor
        if executor is not None:
            executor_data = executor.get_executor_data()
            executor_type = str(
                executor_data.pop("executor_type", executor.executor_type_id)
            )
            executor_info = {
                k: v
                if isinstance(v, str)
                else json.dumps(v)
                if isinstance(v, (dict, list))
                else str(v)
                for k, v in executor_data.items()
            }
        else:
            executor_type = "none"
            executor_info = {}

        python_version_info = sys.version_info
        python_version_str = f"{python_version_info.major}.{python_version_info.minor}.{python_version_info.micro}"
        return {
            "executor_type": executor_type,
            "executor_info": executor_info,
            "python_version": python_version_str,
            "extra_info": {},
        }

    def get_execution_data_schema(self) -> Schema:
        """Schema for the data returned by ``get_execution_data``.

        Built from the type's declaration (``PYTHON_FUNCTION.execution``).
        """
        return Schema({c.name: c.python_type for c in PYTHON_FUNCTION.execution})

    @property
    def input_data_schema(self) -> Schema:
        """Schema describing the input data this function accepts."""
        return self._input_schema

    @property
    def output_data_schema(self) -> Schema:
        """Schema describing the output data this function produces."""
        return self._output_schema

    def is_active(self) -> bool:
        """Return whether the function is active (will process data)."""
        return self._active

    def set_active(self, active: bool = True) -> None:
        """Set the active state. If False, ``call`` returns None for every data."""
        self._active = active

    @property
    def is_async(self) -> bool:
        """Return whether the wrapped function is an async coroutine function."""
        return self._is_async

    def _build_output_data(self, values: Any) -> DataProtocol:
        """Build an output Data from raw function return values.

        Every returned value is checked against its declared output type and validated
        against Arrow storage eagerly. Numeric exceptions are normalized; compatible
        subclass and structural objects pass unchanged in memory. The declared logical
        type controls their stored representation (ITL-895).
        Without the check pyarrow would coerce some mismatches silently, for example
        truncating ``1.5`` returned for ``-> int``. The shared conformance policy
        permits only value-preserving numeric exceptions.

        Args:
            values: Raw return value from the wrapped function.

        Returns:
            A Data containing the parsed outputs with source info.

        Raises:
            NoOutputsDeclaredError: If the function declares no outputs and returned a
                value other than ``None``.
            OutputTypeError: If a returned value does not conform to its declared type,
                or cannot be converted to it.
        """
        if not self._output_keys and values is not None:
            # No output keys declare an empty return (ITL-1257): a returned value
            # contradicts the declaration and is refused, never dropped.
            raise NoOutputsDeclaredError(self.canonical_function_name, values)
        try:
            output_data = parse_function_outputs(
                self._output_keys, values, self._return_container
            )
        except UnexpectedOutputContainer as exc:
            raise self._output_container_error(exc) from exc
        output_schema = self.output_data_schema
        for key, value in output_data.items():
            mismatch = check_conformance(
                value,
                output_schema[key],
                # DataContext's legacy protocol omits the logical-type operations.
                typing.cast(
                    "LogicalTypeConverterProtocol",
                    typing.cast(object, self.data_context.type_converter),
                ),
            )
            if mismatch is not None:
                raise OutputTypeError(
                    self.canonical_function_name,
                    key,
                    output_schema[key],
                    value,
                    mismatch.describe(),
                    mismatch.hint,
                )
            conversions = tensor_conversions(value)
            if conversions:
                warn_once(
                    self._reported_conversions,
                    key,
                    conversions,
                    f"Function {self._function_name!r} output {key!r} returned a "
                    f"torch tensor {describe(conversions)}; Orcapod stored a CPU "
                    "tensor without autograd history, and the returned tensor is "
                    "unchanged. Return value.detach().cpu() to avoid this warning.",
                )

        new_record_uuid = uuid.UUID(bytes=uuid7().bytes)
        source_info = output_source_info(self.uri, new_record_uuid.hex, output_data)

        if not output_data:
            return Data(
                output_data,
                source_info=source_info,
                record_uuid=new_record_uuid,
                python_schema=output_schema,
                data_context=self.data_context,
            )
        # Validate storage eagerly, but retain compatible user-class instances. Reading
        # the table back here would rebind structural objects to the registry's class.
        self._convert_outputs(output_data, output_schema)
        normalized = {
            key: typing.cast(
                DataValue, normalize_output_numbers(value, output_schema[key])
            )
            for key, value in output_data.items()
        }
        return Data(
            normalized,
            python_schema=output_schema,
            source_info=source_info,
            record_uuid=new_record_uuid,
            data_context=self.data_context,
        )

    def _output_container_error(
        self, error: UnexpectedOutputContainer
    ) -> OutputContainerTypeError:
        """Attach the function and its declaration to a container mismatch."""
        return OutputContainerTypeError(
            self.canonical_function_name,
            error.returned_type,
            self._return_container,
            self._container_annotation,
        )

    def _executor_return_options(self) -> dict[str, object] | None:
        """Request a container check from an executor that can check before transport."""
        if len(self._output_keys) > 1 and isinstance(
            self._executor, PythonFunctionReturnCheckProtocol
        ):
            return self._executor.return_container_options(self._return_container)
        return None

    def _convert_outputs(
        self, output_data: dict[str, DataValue], output_schema: Schema
    ) -> pa.Table:
        """Convert checked outputs to a one-row Arrow table with the declared types.

        Args:
            output_data: The parsed outputs, already checked against ``output_schema``.
            output_schema: The declared output types.

        Returns:
            The one-row table, typed by the declared schema.

        Raises:
            OutputTypeError: If an output cannot be converted, for example an integer
                out of range for its declared width, a ``datetime`` without a time zone
                or one with a part below one microsecond; it names that output.
        """
        converter = self.data_context.type_converter
        try:
            return converter.python_dicts_to_arrow_table(
                [output_data], python_schema=output_schema
            )
        except Exception as exc:
            # Find the output that fails on its own, so the error can name it.
            for key, value in output_data.items():
                try:
                    _ = converter.python_dicts_to_arrow_table(
                        [{key: value}], python_schema={key: output_schema[key]}
                    )
                except UnstorableDatetimeError as failure:
                    # The datetime rule says what is wrong and how to convert,
                    # without the value (ITL-1268); its own message names the value,
                    # so it is not chained.
                    raise OutputTypeError(
                        self.canonical_function_name,
                        key,
                        output_schema[key],
                        value,
                        failure.reason,
                        failure.hint,
                    ) from None
                except Exception:
                    raise OutputTypeError(
                        self.canonical_function_name,
                        key,
                        output_schema[key],
                        value,
                        type(exc).__name__,
                    ) from None
            raise

    def _call_async_function_sync(self, data: DataProtocol, **kwargs: Any) -> Any:
        """Run the wrapped async function synchronously.

        Uses ``asyncio.run()`` when no event loop is running. When called
        from within a running loop, offloads to a new thread to avoid
        nested event loop errors.

        The coroutine is constructed inside the executor thread (not in the
        caller thread) to avoid unawaited-coroutine warnings if submission
        fails.

        Args:
            data: The input data whose dict form is passed to the function.
            **kwargs: Additional keyword arguments merged with ``data.as_dict()``
                before the function is called.

        Returns:
            The raw return value of the async function.
        """
        import asyncio

        call_dict = {**data.as_dict(), **kwargs}
        fn = self._function
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # No running loop — safe to use asyncio.run()
            return asyncio.run(fn(**call_dict))
        else:
            # Already in a loop — run in a separate thread with its own loop.
            # The lambda ensures the coroutine is created inside the executor
            # thread, avoiding unawaited-coroutine warnings on submission failure.
            return (
                _get_sync_executor()
                .submit(lambda: asyncio.run(fn(**call_dict)))
                .result()
            )

    def call(
        self,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        **kwargs: Any,
    ) -> DataProtocol | None:
        """Process a single data, routing through the executor if one is set.

        When an executor implementing ``PythonFunctionExecutorProtocol`` is
        set, the raw callable and call dict are handed to ``execute_callable``
        which captures I/O and records to the logger.

        Args:
            data: The input data to process.
            logger: Optional logger for I/O capture.
            **kwargs: Extra keyword arguments merged with ``data.as_dict()``
                before invoking the function. Pods use this to supply
                auto-provided arguments (e.g. ``InvocationContext``) that
                are not part of the data schema.
        """
        if self._executor is not None:
            if not self._active:
                return None
            try:
                raw = self._executor.execute_callable(
                    self._function,
                    {**data.as_dict(), **kwargs},
                    executor_options=self._executor_return_options(),
                    logger=logger,
                )
            except UnexpectedOutputContainer as exc:
                raise self._output_container_error(exc) from exc
            return self._build_output_data(raw)
        if logger is not None:
            import warnings

            warnings.warn(
                "A logger was passed but no executor is set — "
                "capture will not occur. Set an executor to enable logging.",
                stacklevel=2,
            )
        return self.direct_call(data, **kwargs)

    async def async_call(
        self,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        **kwargs: Any,
    ) -> DataProtocol | None:
        """Async counterpart of ``call``."""
        if self._executor is not None:
            if not self._active:
                return None
            try:
                raw = await self._executor.async_execute_callable(
                    self._function,
                    {**data.as_dict(), **kwargs},
                    executor_options=self._executor_return_options(),
                    logger=logger,
                )
            except UnexpectedOutputContainer as exc:
                raise self._output_container_error(exc) from exc
            return self._build_output_data(raw)
        if logger is not None:
            import warnings

            warnings.warn(
                "A logger was passed but no executor is set — "
                "capture will not occur. Set an executor to enable logging.",
                stacklevel=2,
            )
        return await self.direct_async_call(data, **kwargs)

    def direct_call(self, data: DataProtocol, **kwargs: Any) -> DataProtocol | None:
        """Execute the function on *data* synchronously (no executor path).

        On user-function failure the exception is re-raised.
        For async functions, the coroutine is driven to completion via
        ``asyncio.run()`` (or a helper thread when already inside an event loop).

        Args:
            data: The input data to process.
            **kwargs: Extra keyword arguments merged with ``data.as_dict()``
                before invoking the function.
        """
        if not self._active:
            return None

        if self._is_async:
            raw_result = self._call_async_function_sync(data, **kwargs)
        else:
            raw_result = self._function(**data.as_dict(), **kwargs)
        return self._build_output_data(raw_result)

    async def direct_async_call(
        self, data: DataProtocol, **kwargs: Any
    ) -> DataProtocol | None:
        """Execute the function on *data* asynchronously (no executor path).

        Async functions are ``await``-ed directly. Sync functions are
        offloaded to a thread pool via ``run_in_executor``.  On failure,
        the exception is re-raised.

        Args:
            data: The input data to process.
            **kwargs: Extra keyword arguments merged with ``data.as_dict()``
                before invoking the function.
        """
        import asyncio

        if not self._active:
            return None

        call_dict = {**data.as_dict(), **kwargs}
        if self._is_async:
            raw_result = await self._function(**call_dict)
        else:
            import contextvars
            import functools

            loop = asyncio.get_running_loop()
            task_ctx = contextvars.copy_context()
            raw_result = await loop.run_in_executor(
                None,
                functools.partial(
                    task_ctx.run,
                    self._function,
                    **call_dict,
                ),
            )
        return self._build_output_data(raw_result)

    def to_config(self) -> dict[str, Any]:
        """Serialize this data function to a JSON-compatible config dict.

        Returns:
            A dict with ``data_function_type_id`` and a nested ``config``
            containing enough information to reconstruct this instance via
            `from_config`.
        """
        return {
            "data_function_type_id": self.data_function_type_id,
            "uri": list(self.uri),
            "config": {
                "module_path": self._function.__module__,
                "callable_name": self._function_name,
                "version": self._version,
                "input_data_schema": {
                    k: stabilize_type_names(str(v), v)
                    for k, v in self.input_data_schema.items()
                },
                "output_data_schema": {
                    k: stabilize_type_names(str(v), v)
                    for k, v in self.output_data_schema.items()
                },
                "output_keys": list(self._output_keys) if self._output_keys else None,
            },
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "PythonDataFunction":
        """Reconstruct a PythonDataFunction by importing the callable.

        Args:
            config: A dict as produced by `to_config`.

        Returns:
            A new ``PythonDataFunction`` wrapping the imported callable.

        Raises:
            ImportError: If the module specified in *config* cannot be imported.
            AttributeError: If the callable name does not exist in the module.
        """
        import importlib

        inner = config.get("config", config)
        module = importlib.import_module(inner["module_path"])
        func = getattr(module, inner["callable_name"])
        return cls(
            function=func,
            output_keys=inner.get("output_keys"),
            version=inner.get("version", "v0.0"),
        )


class DataFunctionWrapper(DataFunctionBase[E]):
    """Wrapper around a DataFunctionProtocol to modify or extend its behavior.

    Remains generic over ``E`` — the executor protocol is not bound here
    so that wrappers inherit the executor type constraint of the wrapped
    function.
    """

    def __init__(self, data_function: DataFunctionProtocol, **kwargs) -> None:
        """Wrap ``data_function``.

        Args:
            data_function: The data function to wrap.
            **kwargs: Passed on to ``DataFunctionBase``. Without a ``data_context``
                the wrapper works under the wrapped function's data context, so
                that what it hashes and stores agrees with the function's data.
        """
        self._data_function = data_function
        if kwargs.get("data_context") is None:
            kwargs["data_context"] = getattr(data_function, "data_context", None)
        super().__init__(**kwargs)

    def computed_label(self) -> str | None:
        return self._data_function.label

    @property
    def is_async(self) -> bool:
        """Whether the wrapped function is an async coroutine function."""
        return getattr(self._data_function, "is_async", False) is True

    @property
    def major_version(self) -> int:
        return self._data_function.major_version

    @property
    def minor_version_string(self) -> str:
        return self._data_function.minor_version_string

    @property
    def data_function_type_id(self) -> str:
        return self._data_function.data_function_type_id

    @property
    def declaration(self) -> DataFunctionTypeDeclaration:
        """The wrapped function's declaration, as its type id is the wrapped one's."""
        return self._data_function.declaration

    @property
    def canonical_function_name(self) -> str:
        return self._data_function.canonical_function_name

    @property
    def input_data_schema(self) -> Schema:
        return self._data_function.input_data_schema

    @property
    def accepts_arbitrary_keywords(self) -> bool:
        return self._data_function.accepts_arbitrary_keywords

    @property
    def output_data_schema(self) -> Schema:
        return self._data_function.output_data_schema

    def get_function_variation_data(self) -> dict[str, Any]:
        return self._data_function.get_function_variation_data()

    def get_function_variation_data_schema(self) -> Schema:
        return self._data_function.get_function_variation_data_schema()

    def get_execution_data(self) -> dict[str, Any]:
        return self._data_function.get_execution_data()

    def get_execution_data_schema(self) -> Schema:
        return self._data_function.get_execution_data_schema()

    def to_config(self) -> dict[str, Any]:
        """Delegate serialization to the wrapped data function."""
        return self._data_function.to_config()

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "DataFunctionWrapper":
        """Reconstruct by delegating to the wrapped function type.

        Args:
            config: A dict as produced by `to_config`.

        Returns:
            A new instance reconstructed from *config*.
        """
        return cls._data_function_class_for_config(config).from_config(config)

    @staticmethod
    def _data_function_class_for_config(config: dict[str, Any]) -> type:
        """Return the concrete class to use when reconstructing from *config*.

        Currently only ``PythonDataFunction`` is supported. Subclasses may
        override this to handle additional types.

        Args:
            config: A config dict as produced by `to_config`.

        Returns:
            The class to call ``from_config`` on.

        Raises:
            ValueError: If the ``data_function_type_id`` is not recognized.
        """
        type_id = config.get("data_function_type_id")
        if type_id == PYTHON_FUNCTION.type_id:
            return PythonDataFunction
        raise ValueError(f"Unrecognized data_function_type_id: {type_id!r}")

    # -- Executor delegation: setting/getting the executor on a wrapper
    #    transparently targets the wrapped (leaf) data function.

    @property
    def executor(self) -> DataFunctionExecutorProtocol | None:
        return self._data_function.executor

    @executor.setter
    def executor(self, executor: DataFunctionExecutorProtocol | None) -> None:
        self._data_function.executor = executor

    # -- Execution: call/async_call delegate to the wrapped function's
    #    call/async_call which handles executor routing.  direct_call /
    #    direct_async_call bypass executor routing as their names imply.

    def call(
        self,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        **kwargs: Any,
    ) -> DataProtocol | None:
        return self._data_function.call(data, logger=logger, **kwargs)

    async def async_call(
        self,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        **kwargs: Any,
    ) -> DataProtocol | None:
        return await self._data_function.async_call(data, logger=logger, **kwargs)

    def direct_call(self, data: DataProtocol, **kwargs: Any) -> DataProtocol | None:
        return self._data_function.direct_call(data, **kwargs)

    async def direct_async_call(
        self, data: DataProtocol, **kwargs: Any
    ) -> DataProtocol | None:
        return await self._data_function.direct_async_call(data, **kwargs)


class CachedDataFunction(DataFunctionWrapper):
    """Wrapper around a DataFunctionProtocol that caches results for identical input data.

    Uses a shared ``ResultCache`` for lookup/store/conflict-resolution
    logic (same mechanism as ``CachedFunctionPod``).
    """

    # Expose RESULT_COMPUTED_FLAG from the shared ResultCache
    RESULT_COMPUTED_FLAG = ResultCache.RESULT_COMPUTED_FLAG

    def __init__(
        self,
        data_function: DataFunctionProtocol,
        result_database: ArrowDatabaseProtocol,
        record_path_prefix: tuple[str, ...] = (),
        *,
        context_parameter: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(data_function, **kwargs)
        self._result_database = result_database
        self._record_path_prefix = record_path_prefix
        if context_parameter is not None:
            record_path_prefix += ("side_effect_function",)
        self._cache = ResultCache(
            result_database=result_database,
            record_path=context_result_path(
                record_path_prefix + self.uri, context_parameter
            ),
            revision=self.declaration.revision_component,
            auto_flush=True,
            delivers_side_effects=context_parameter is not None,
            # The first uri component is the function's name; no hash before it.
            name_path=record_path_prefix + self.uri[:1],
        )

    @property
    def result_database(self) -> ArrowDatabaseProtocol:
        """The database this function's results are cached in."""
        return self._cache.result_database

    def _build_metadata_datagrams(self) -> tuple[Datagram, Datagram]:
        """Build variation and execution Datagrams for cache storage."""
        variation_datagram = Datagram(
            self.get_function_variation_data(),
            python_schema=self.get_function_variation_data_schema(),
            data_context=self.data_context,
        )
        execution_datagram = Datagram(
            self.get_execution_data(),
            python_schema=self.get_execution_data_schema(),
            data_context=self.data_context,
        )
        return variation_datagram, execution_datagram

    def set_auto_flush(self, on: bool = True) -> None:
        """Set auto-flush behavior. If True, the database flushes after each record."""
        self._cache.set_auto_flush(on)

    @property
    def record_path(self) -> tuple[str, ...]:
        """Return the path to the cached records in the result database (versioned)."""
        return self._cache.record_path

    def call(
        self,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        skip_cache_lookup: bool = False,
        skip_cache_insert: bool = False,
        **kwargs: Any,
    ) -> DataProtocol | None:
        """Look up cache keyed on ``data``; on miss, call and store result.

        ``**kwargs`` are forwarded to the wrapped data function's ``call``
        so that auto-provided arguments (e.g. ``InvocationContext``) reach
        the underlying function. The configured context parameter, when present,
        selects a separate result table. Lookup within a table uses the input
        data hash.

        Raises:
            InputValidationError: If ``data`` is of another data context than this
                function's; the wrapped function has not been called then, whatever
                ``skip_cache_lookup`` and ``skip_cache_insert`` are.
        """
        # Checked before anything else: with ``skip_cache_lookup`` the lookup, which
        # checks it too, does not run, and the function must not be called.
        require_same_data_context(data.data_context_key, self.data_context_key)
        output_data = None
        if not skip_cache_lookup:
            module_logger.info("Checking for cache...")
            output_data = self._cache.lookup(
                data, data_context_key=self.data_context_key
            )
            if output_data is not None:
                module_logger.info(f"Cache hit for {data}!")
                return output_data
        output_data = self._data_function.call(data, logger=logger, **kwargs)
        if output_data is not None:
            if not skip_cache_insert:
                var_dg, exec_dg = self._build_metadata_datagrams()
                self._cache.store(
                    data,
                    output_data,
                    var_dg,
                    exec_dg,
                    data_context_key=self.data_context_key,
                )
            output_data = output_data.with_meta_columns(
                **{self.RESULT_COMPUTED_FLAG: True}
            )
        return output_data

    async def async_call(
        self,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        skip_cache_lookup: bool = False,
        skip_cache_insert: bool = False,
        **kwargs: Any,
    ) -> DataProtocol | None:
        """Async counterpart of ``call`` with cache check and recording.

        Raises:
            InputValidationError: If ``data`` is of another data context than this
                function's; the wrapped function has not been called then.
        """
        # Checked before anything else: with ``skip_cache_lookup`` the lookup, which
        # checks it too, does not run, and the function must not be called.
        require_same_data_context(data.data_context_key, self.data_context_key)
        output_data = None
        if not skip_cache_lookup:
            module_logger.info("Checking for cache...")
            output_data = self._cache.lookup(
                data, data_context_key=self.data_context_key
            )
            if output_data is not None:
                module_logger.info(f"Cache hit for {data}!")
                return output_data
        output_data = await self._data_function.async_call(
            data, logger=logger, **kwargs
        )
        if output_data is not None:
            if not skip_cache_insert:
                var_dg, exec_dg = self._build_metadata_datagrams()
                self._cache.store(
                    data,
                    output_data,
                    var_dg,
                    exec_dg,
                    data_context_key=self.data_context_key,
                )
            output_data = output_data.with_meta_columns(
                **{self.RESULT_COMPUTED_FLAG: True}
            )
        return output_data

    def get_cached_output_for_data(
        self, input_data: DataProtocol
    ) -> DataProtocol | None:
        """Retrieve the cached output data for *input_data*.

        If multiple cached entries exist, the most recent (by timestamp) wins.

        Returns:
            The cached output data, or ``None`` if no entry was found.
        """
        return self._cache.lookup(input_data, data_context_key=self.data_context_key)

    def record_data(
        self,
        input_data: DataProtocol,
        output_data: DataProtocol,
        skip_duplicates: bool = False,
    ) -> DataProtocol:
        """Record the output data against the input data in the result store."""
        var_dg, exec_dg = self._build_metadata_datagrams()
        self._cache.store(
            input_data,
            output_data,
            var_dg,
            exec_dg,
            skip_duplicates=skip_duplicates,
            data_context_key=self.data_context_key,
        )
        return output_data

    def get_all_cached_outputs(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """Return all cached records from the result database for this function.

        Args:
            include_system_columns: If True, include system columns
                (e.g. record_id) in the result.

        Returns:
            A PyArrow table of cached results, or ``None`` if empty.
        """
        return self._cache.get_all_records(
            self.data_context_key,
            include_system_columns=include_system_columns,
        )
