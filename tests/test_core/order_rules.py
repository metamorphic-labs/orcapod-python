"""Where order counts, and the open issues that still break the rule (ITL-1271).

The rule (developer, November 18, 2026): no operator result and no stream depends on the
order of the tags or columns fed into it, nor on the order of the items of an argument
that names a set, nor on the order of the inputs of a commutative operator. "If any
operations COULD vary output by tag order, then explicit ordering of tags to be used
should be an explicit input into the function/operator."

``ORDER_EXCEPTIONS`` is the one list of the places where an order is meant to count. Each
entry names the argument and why. ``tests/test_core/test_order_invariance.py`` uses it, and
the section "Order never counts, except where you give it" of
``docs/concepts/identity.md`` lists every entry (``test_the_docs_list_every_exception``).

``KNOWN_VIOLATIONS`` names the open issue that owns each case the test marks as an
expected failure (``xfail(strict=True)``): when the issue lands, its case passes, the
strict mark fails, and the mark is removed.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OrderException:
    """An argument or input whose order is meant to count.

    Attributes:
        key: A short name, used by the test and listed in the docs.
        what: The argument or input, as the user writes it.
        reason: Why its order counts.
    """

    key: str
    what: str
    reason: str


ORDER_EXCEPTIONS: tuple[OrderException, ...] = (
    OrderException(
        "record_id_columns",
        "`record_id_columns` of every source (by default a database table's primary key)",
        "A record id is the canonical JSON of the key columns in key order, so the key "
        "order is part of every record id and of the source's identity. It is the "
        "explicit argument that states the order.",
    ),
    OrderException(
        "member_order",
        "`member_order` of `GroupBy` (ITL-1226; not in a release yet)",
        "The explicit order of the members inside each group's lists, the argument that "
        "replaces the input's tag order there.",
    ),
    OrderException(
        "non_commutative_inputs",
        "the inputs of an operator whose `argument_symmetry` is a tuple (`SemiJoin`)",
        "The inputs have roles: `SemiJoin(left, right)` keeps the rows of `left`.",
    ),
    OrderException(
        "positional_rows",
        "the order of the ROWS given to a source without key columns",
        "Its record ids are the row positions, so a reorder is a change (by decision). "
        "Row order, never column order.",
    ),
    OrderException(
        "batch_rows",
        "the order of the ROWS given to `Batch`",
        "`Batch` groups consecutive rows: row order, never column order.",
    ),
    OrderException(
        "structural_fields",
        "the fields of a user class (`@dataclass`, `orcapod.dataclass`, pydantic)",
        "A class is identified with its fields in declared order: reordering them "
        "declares another type.",
    ),
    OrderException(
        "column_placement",
        "the declared `tag_columns` order and a table's data column order",
        "Presentation only: they place the columns of `keys()`, `as_table()` and stored "
        "tables (data-tags first, ITL-1205), and enter no hash, identity or stored value. "
        "That this placement is consistent across layouts and paths is ITL-1249.",
    ),
)

# Open issues whose cases the test marks as expected failures.
KNOWN_VIOLATIONS: dict[str, str] = {
    "ITL-1227": "a raw ArrowTableStream's content hash depends on the order of its tags",
    "ITL-1246": "a function pod over data-tags: its content and pipeline hash (and its "
    "node's stored paths) depend on the input's tag order and table layout",
    "ITL-1247": "PolarsFilter: the order of its predicates is in its identity (undecided)",
    "ITL-1269": "AddTags: its identity holds the new tags in keyword order",
    "ITL-1270": "the presentation sort (head, sort_by_tags, preview) orders rows by the "
    "tags in tag column order",
}
