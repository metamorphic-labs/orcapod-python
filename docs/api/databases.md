# Databases
<!--
Input of the API reference generator (scripts/docs_site_api.py), not a page written by
hand: this file chooses the objects (one `::: <object path>` line each) and gives the
introduction. Every signature and docstring on the published page comes from the source
code. See site/README.md.
-->

Database backends store pipeline results. The SQLite and PostgreSQL connectors support
reads and writes through `ConnectorArrowDatabase`. The SpiralDB connector is read-only.

::: orcapod.databases.DeltaTableDatabase

::: orcapod.databases.InMemoryArrowDatabase

::: orcapod.databases.NoOpArrowDatabase

::: orcapod.databases.ConnectorArrowDatabase

::: orcapod.databases.ExtensionAwareDatabase

::: orcapod.databases.SQLiteConnector

::: orcapod.databases.PostgreSQLConnector

::: orcapod.databases.SpiralDBConnector
