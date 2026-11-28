# Pipeline
<!--
Input of the API reference generator (scripts/docs_site_api.py), not a page written by
hand: this file chooses the objects (one `::: <object path>` line each) and gives the
introduction. Every signature and docstring on the published page comes from the source
code. See site/README.md.
-->

Top-level objects for composing and executing Orcapod data pipelines. `Pipeline` is a pure
blueprint; `PipelineJob` binds a pipeline to sources and a database and runs it.

::: orcapod.pipeline.Pipeline

::: orcapod.pipeline.PipelineJob

::: orcapod.pipeline.PipelineDiagram

::: orcapod.pipeline.LoadStatus

::: orcapod.pipeline.RunSummary

::: orcapod.pipeline.NodeRunSummary

::: orcapod.pipeline.SyncPipelineOrchestrator

::: orcapod.pipeline.AsyncPipelineOrchestrator

::: orcapod.pipeline.CompositeObserver

::: orcapod.pipeline.LoggingObserver

::: orcapod.pipeline.StatusObserver

::: orcapod.pipeline.DataLogger
