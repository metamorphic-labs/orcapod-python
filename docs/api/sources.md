# Sources
<!--
Input of the API reference generator (scripts/docs_site_api.py), not a page written by
hand: this file chooses the objects (one `::: <object path>` line each) and gives the
introduction. Every signature and docstring on the published page comes from the source
code. See site/README.md.
-->

Source classes provide the entry point for external data into Orcapod pipelines.
All sources convert their input to a PyArrow Table and use `SourceStreamBuilder` for
enrichment (provenance columns, system tags, hashing). `PollingSource` is provisional: its
source id and hashes are not frozen with data context v1 (`PROVISIONAL_SOURCES`).

::: orcapod.core.sources.ArrowTableSource

::: orcapod.core.sources.DictSource

::: orcapod.core.sources.ListSource

::: orcapod.core.sources.DataFrameSource

::: orcapod.core.sources.CSVSource

::: orcapod.core.sources.DeltaTableSource

::: orcapod.core.sources.SQLiteTableSource

::: orcapod.core.sources.PostgreSQLTableSource

::: orcapod.sources.SpiralDBTableSource

::: orcapod.sources.DBTableSource

::: orcapod.core.sources.PollingSource

::: orcapod.core.sources.DerivedSource

::: orcapod.core.sources.CachedSource

::: orcapod.core.sources.SourceProxy

::: orcapod.core.sources.SourceRegistry

::: orcapod.core.sources.RootSource

::: orcapod.core.sources.RootSource.as_slot
