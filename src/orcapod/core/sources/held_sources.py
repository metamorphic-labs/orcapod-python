"""The library's sources that hold their whole table once built (ITL-1464).

Reading such a source's rows computes and keeps nothing, so a preview, or the full
read of a ``DerivedSource`` subclass over it, may read it as it is: for exactly these
classes only (developer decision N55).
"""

from __future__ import annotations

from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.sources.csv_source import CSVSource
from orcapod.core.sources.data_frame_source import DataFrameSource
from orcapod.core.sources.db_table_source import DBTableSource
from orcapod.core.sources.delta_table_source import DeltaTableSource
from orcapod.core.sources.dict_source import DictSource
from orcapod.core.sources.list_source import ListSource
from orcapod.core.sources.postgresql_table_source import PostgreSQLTableSource
from orcapod.core.sources.sqlite_table_source import SQLiteTableSource

HELD_LIBRARY_SOURCES: frozenset[type] = frozenset(
    {
        ArrowTableSource,
        CSVSource,
        DataFrameSource,
        DBTableSource,
        DeltaTableSource,
        DictSource,
        ListSource,
        PostgreSQLTableSource,
        SQLiteTableSource,
    }
)
"""Every library source class that holds its table (``_stream``) once built."""
