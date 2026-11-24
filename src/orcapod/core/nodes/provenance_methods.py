"""The provenance methods of job nodes: ``trace`` and ``why_missing`` (ITL-1432)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcapod.types.provenance import Absence, Trace


class ProvenanceMethods:
    """Mixin of the job nodes (source, operator, function) that can be traced.

    Both methods read only what the job's nodes hold and what their databases store:
    they never compute, call user code, poll or write.
    """

    def trace(self, row: Mapping[str, Any], column: str) -> Trace:
        """Trace cells of this node back to the source cells they were made from.

        Provisional in 0.4.0: answers may be refined; a case that cannot be proven is
        reported unavailable or refused (the known limits are in the provenance concept
        page).

        Every row this node holds whose columns equal ``row``'s values is traced (any
        number; ``{}`` traces every row). Each step names the node, the cell's value
        there and its row's tags. A function step says which function produced the
        value, with its version, body hash, executor and time, and whether this row's
        entry computed the result or reused one stored for equal inputs. A ``GroupBy``
        or ``Batch`` step lists its members, found by the operator's own rule and
        checked: their system tags must fold to the group's. The leaves are source
        cells. A loaded job traces the same once it has run with ``load_only=True``.

        Args:
            row: Column values selecting the rows to trace, usually tags:
                ``{"subject_id": "mouse_03", "session": "s2"}``.
            column: The data column of the cells.

        Returns:
            The trace: one root per matching row, its header saying how many match
            (status ``"no_match"`` when none does); its one root's status is
            ``"not_computed"`` when this node holds no run (``run()`` the job first).

        Raises:
            ValueError: If ``column`` is not a data column of this node, or ``row``
                names a column this node does not have.
            ProvenanceInconsistencyError: If a link cannot be proven from what the job
                holds and stores (a group whose members do not fold to its values).
        """
        from orcapod.pipeline.provenance import trace

        return trace(self, row, column)

    def why_missing(self, source: str, record_id: str) -> Absence:
        """Say whether rows from one source record reach this node, and if not, why.

        Provisional in 0.4.0: answers may be refined; a case that cannot be proven is
        reported unavailable or refused (the known limits are in the provenance concept
        page).

        It reads the run the job holds (``not_computed`` without one, even when the
        store holds entries). From the rows the nodes hold: the nodes that hold a row
        from the record, and the step that removed it, in ``explain()``'s words; for a
        join, the key missing from the other input. From the first function node's
        stored entries: a stored entry proves the record reached it
        (``reached_function``); no entry proves nothing (a function that raised stores
        none), so the answer is then unknown, and ``entry_recorded`` says what the store
        holds. Past an operator that is not one of Orcapod's own (a subclass, or your
        own), the answer is ``unavailable``.

        Args:
            source: The source: its node's label, its name or its full id (as in
                source-info tokens, ``dict_v1:trials``). It must name one source.
            record_id: The record id (``row_<n>``, or the key columns as JSON).

        Returns:
            The answer; its status is ``"not_computed"`` when a node on the way holds no
            run. For a source node whose rows are not read, its status is
            ``"unavailable"`` and the source id or label is not checked.

        Raises:
            ValueError: If no source with that label, name or id feeds this node
                (unless this node is a source whose rows are not read), or if it names
                several (one's name is another's id): give a node label or full id
                that names one.
            ProvenanceInconsistencyError: If a group's members cannot be proven.
        """
        from orcapod.pipeline.provenance import why_missing

        return why_missing(self, source, record_id)
