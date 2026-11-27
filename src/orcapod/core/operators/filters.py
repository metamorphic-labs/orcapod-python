import logging
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from typing import Any, ClassVar, TYPE_CHECKING, TypeAlias

import polars as pl
import pyarrow as pa

from orcapod.core.unique_keys import UniqueKeys
from orcapod.core.data_tags import stream_data_tags
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams import ArrowTableStream
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import arrow_utils, source_tokens

if TYPE_CHECKING:
    import numpy as np
    import polars._typing as pl_type

logger = logging.getLogger(__name__)

polars_predicate: TypeAlias = "pl_type.IntoExprColumn| Iterable[pl_type.IntoExprColumn]| bool| list[bool]| np.ndarray[Any, Any]"


_SAFE_SCALARS: tuple[type, ...] = (bool, int, float, str, bytes, type(None))
"""Values whose ``repr`` is the built-in one: written as they are by ``explain()``."""


def _safe_text(value: object, write: Callable[[object], str]) -> str:
    """Return ``value`` written by ``write`` when its type's writer is Polars' or the
    built-in one (exactly ``pl.Expr`` or a built-in scalar); otherwise ``<type name>``,
    so no user-defined ``__str__`` or ``__repr__`` runs (``explain()`` runs no user code).
    """
    if type(value) in _SAFE_SCALARS or type(value) is pl.Expr:
        return write(value)
    return f"<{type(value).__name__}>"


class PolarsFilter(UnaryOperator):
    """
    Operator that applies Polars filtering to a stream
    """

    CARRIES_TOKENS: ClassVar[bool] = True

    # Data-tags (ITL-1023): handled.
    DATA_TAGS_ISSUE: ClassVar[str | None] = None

    # A filter chooses rows and never changes a column (ITL-912).
    OUTPUT_SCHEMA_IS_INPUT_SCHEMA = True

    # Its identity is not frozen with data context v1 (ITL-1266, ITL-1247).
    PROVISIONAL = True

    def derive_unique_keys(self, stream: StreamProtocol) -> UniqueKeys:
        """The input's unique keys (``core/unique_keys``): rows are only left out."""
        return stream.unique_keys

    def __init__(
        self,
        predicates: Collection[
            "pl_type.IntoExprColumn| Iterable[pl_type.IntoExprColumn]| bool| list[bool]| np.ndarray[Any, Any]"
        ] = (),
        constraints: Mapping[str, Any] | None = None,
        **kwargs,
    ):
        # A tuple, so PolarsFilter(predicates=[e]) and stream.polars_filter(e) share one
        # identity (a list and a tuple hash differently).
        self.predicates = tuple(predicates)
        self.constraints = constraints if constraints is not None else {}
        super().__init__(**kwargs)

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        if len(self.predicates) == 0 and len(self.constraints) == 0:
            logger.info(
                "No predicates or constraints specified. Returning stream unaltered."
            )
            return stream

        # TODO: improve efficiency here...
        whole = self._input_table(
            stream, columns={"source": True, "system_tags": True, "sort_by_tags": False}
        )
        # Polars reads no derived token column: one is left out, and put back for the
        # rows kept (its recipe reads the system tags kept with them, ITL-1464), unless
        # a predicate could read it.
        derived = source_tokens.derived_columns(whole.schema)
        others = {c for c in whole.column_names if c not in derived}
        if derived and not self._reads_only(others):
            whole, derived = source_tokens.materialize(whole), ()
        table = whole.drop_columns(list(derived))
        df = pl.DataFrame(table)
        filtered_table = df.filter(*self.predicates, **self.constraints).to_arrow()
        # A filter passes every column through, so each one takes the
        # large-normalized form of its input Arrow type (string ->
        # large_string, list -> large_list, recursively; hash-identical).
        # This also undoes Polars' other changes, such as fixed_size_binary ->
        # large_binary for system-tag record ids.
        input_fields = [
            field.with_type(
                arrow_utils.normalize_to_large_types(field.type, keep_null=True)
            )
            for field in table.schema
        ]
        filtered_table = arrow_utils.restore_column_types(
            filtered_table, {field.name: field.type for field in input_fields}
        )
        # Polars returns every field nullable; every column gets its input
        # nullability back, so a filtered stream keeps its input schema and
        # still joins with streams whose tags are non-nullable.
        filtered_table = arrow_utils.restore_schema_nullability(
            filtered_table, pa.schema(input_fields)
        )
        for name in derived:
            field = whole.schema.field(name)
            recipe = source_tokens.derived_recipe(field.type)
            assert recipe is not None
            filtered_table = filtered_table.append_column(
                field, source_tokens.derived_array(recipe, filtered_table.num_rows)
            )
        filtered_table = filtered_table.select(whole.column_names)

        return ArrowTableStream(
            filtered_table,
            tag_columns=stream.keys()[0],
            data_tags=stream_data_tags(stream),
        )

    def _reads_only(self, columns: Collection[str]) -> bool:
        """Whether every predicate certainly reads columns of ``columns`` only.

        Certain only for expressions that name each column they read: a selector
        (``pl.all()``, a regular expression, a type) can read any column, and a predicate
        that is not an expression is not looked into. A name that is no column is not
        certain either: Polars' error then lists the columns there are.
        """
        if not set(self.constraints) <= set(columns):
            return False
        return all(
            isinstance(predicate, pl.Expr)
            and not predicate.meta.has_multiple_outputs()
            and set(predicate.meta.root_names()) <= set(columns)
            for predicate in (
                *self.predicates,
                *(v for v in self.constraints.values() if isinstance(v, pl.Expr)),
            )
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # Any valid stream would work
        return

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
        include_system_tags: bool = False,
    ) -> tuple[Schema, Schema]:
        # data types are not modified
        return stream.output_schema(columns=columns, all_info=all_info)

    def to_config(self) -> dict[str, Any]:
        """Serialize this PolarsFilter operator to a config dict.

        Polars ``Expr`` predicates are serialized to JSON strings when possible.
        If any predicate cannot be serialized, ``reconstructable`` is set to
        ``False`` and ``predicates`` is set to ``None``.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys.
        """
        config = super().to_config()
        serialized_predicates = []
        reconstructable = True
        for pred in self.predicates:
            if hasattr(pred, "meta") and hasattr(pred.meta, "serialize"):
                serialized = pred.meta.serialize(format="json")
                # serialize() returns bytes in some Polars versions, str in others
                if isinstance(serialized, bytes):
                    serialized = serialized.decode()
                serialized_predicates.append(serialized)
            else:
                reconstructable = False
                break
        config["config"] = {
            "constraints": dict(self.constraints) if self.constraints else None,
            "predicates": serialized_predicates if reconstructable else None,
            "reconstructable": reconstructable,
        }
        return config

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "PolarsFilter":
        """Reconstruct a PolarsFilter from a config dict.

        Args:
            config: Dict as returned by ``to_config()``.

        Returns:
            A new ``PolarsFilter`` instance.

        Raises:
            NotImplementedError: If ``reconstructable`` is ``False``.
        """
        inner = config.get("config", {})
        if not inner.get("reconstructable", True):
            raise NotImplementedError(
                "PolarsFilter with non-serializable predicates cannot be reconstructed"
            )
        predicates = []
        if inner.get("predicates"):
            import polars as pl

            predicates = []
            for p in inner["predicates"]:
                # deserialize() accepts bytes in some Polars versions, str in others
                try:
                    predicates.append(pl.Expr.deserialize(p.encode(), format="json"))
                except TypeError:
                    predicates.append(pl.Expr.deserialize(p, format="json"))
        constraints = inner.get("constraints")
        return cls(predicates=predicates, constraints=constraints)

    def rows_dropped_text(
        self,
        position: int,
        others: Sequence[str],
        inputs: Sequence[StreamProtocol],
    ) -> str | None:
        """What the filter keeps, for ``explain()``: its constraints and predicates as
        text, each sorted (display only).

        No user code runs: a Polars expression is written by Polars (``pl.Expr``
        exactly), a constraint value by ``repr`` when it is exactly a built-in scalar,
        and anything else by its type name only. The constraints are read only when
        they are exactly a ``dict`` with ``str`` names (a mapping of another class could
        run its own ``items()``); otherwise they are written as their type name.
        """
        constraints = self.constraints
        if type(constraints) is dict and all(type(name) is str for name in constraints):
            parts = sorted(
                f"{name}={_safe_text(value, repr)}"
                for name, value in dict.items(constraints)
            )
        else:
            parts = [f"<{type(constraints).__name__}>"]
        parts += sorted(_safe_text(predicate, str) for predicate in self.predicates)
        return ", ".join(parts) if parts else None

    def identity_structure(self) -> Any:
        self._note_provisional_identity_use()  # a direct read is a use (ITL-1279)
        return (
            self.__class__.__name__,
            self.predicates,
            self.constraints,
        )
