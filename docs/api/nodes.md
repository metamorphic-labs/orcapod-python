# Nodes
<!--
Input of the API reference generator (scripts/docs_site_api.py), not a page written by
hand: this file chooses the objects (one `::: <object path>` line each) and gives the
introduction. Every signature and docstring on the published page comes from the source
code. See site/README.md.
-->

Nodes are the elements of a compiled pipeline graph. The plain node classes describe the
graph of a `Pipeline` blueprint; the job node classes below add database-backed execution.

::: orcapod.core.nodes.FunctionNode

::: orcapod.core.nodes.OperatorNode

::: orcapod.core.nodes.SourceNode

## Job nodes

The database-backed nodes a `PipelineJob` builds; these are what `job.nodes[label]` returns.

::: orcapod.core.nodes.FunctionJobNode

::: orcapod.core.nodes.OperatorJobNode

::: orcapod.core.nodes.SourceJobNode

## Provenance of a job's rows

Every job node above (function, operator and source) has these two methods: they trace a
cell to its source cells, and say why a source record does not reach a node. Both are
provisional in 0.4.0: their answers may be refined in a later release; a case they cannot
prove is reported unavailable or refused, never guessed. The known limits are listed on
the [provenance concept page](../concepts/provenance.md#known-limits).

::: orcapod.core.nodes.provenance_methods.ProvenanceMethods
