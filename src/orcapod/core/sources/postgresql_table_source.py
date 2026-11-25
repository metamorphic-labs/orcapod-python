"""PostgreSQLTableSource — a read-only RootSource backed by a PostgreSQL table.

Wraps a PostgreSQL table as an OrcaPod Source. Primary-key columns are used
as tag columns by default.

Example::

    source = PostgreSQLTableSource(
        "postgresql://user:pass@localhost:5432/mydb", "measurements"
    )
"""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Collection, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, cast

from orcapod.core.data_tags import TagOnlyColumns
from typing_extensions import override

from orcapod.core.sources.db_table_source import DBTableSource
from orcapod.core.sources.source_ids import joined_location
from orcapod.core.sources.base import saved_tag_only_columns
from orcapod.databases.postgresql_connector import PostgreSQLConnector
from orcapod.protocols.db_connector_protocol import DBConnectorProtocol

if TYPE_CHECKING:
    from orcapod import contexts
    from orcapod.config import OrcapodConfig


# A drive-qualified Windows socket directory (``C:/PgSockets``, ``c:\\pg``): libpq takes
# it as a path, not a host name, so it keeps its case.
_WINDOWS_DIRECTORY = re.compile(r"[A-Za-z]:[\\/]")


def _server_name(host: str) -> str:
    """Return the host as it is written in a default source id.

    A host name is lower-cased (DNS names are case-insensitive); an IPv6 address is
    bracketed, as in a URI, in its exploded form, so it never holds ``::`` and has one
    spelling (a zone, ``%eth0``, is kept as given); an IPv4 address and a socket path
    (``/...``, ``@...`` for a Linux abstract socket, or a drive-qualified Windows
    directory such as ``C:/PgSockets`` or ``c:\\pg``) are kept as given.

    Args:
        host: The host the connection reports (``PQhost``).

    Returns:
        The host's name in the id.
    """
    address, zone_mark, zone = host.partition("%")
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        ip = None
    if isinstance(ip, ipaddress.IPv6Address):
        return f"[{ip.exploded}{zone_mark}{zone}]"
    if ip is not None or host.startswith(("/", "@")) or _WINDOWS_DIRECTORY.match(host):
        return host
    return host.lower()


def _one_address(host: str, hostaddr: str) -> bool:
    """Return whether a ``host`` and a ``hostaddr`` name one address.

    Equal text, or two IP literals of one parsed value (``0:0:0:0:0:0:0:1`` and
    ``::1``; an IPv6 zone such as ``%eth0`` is part of the value). A host name beside
    an address is never one address: what the name resolves to is not seen.
    """
    if host == hostaddr:
        return True
    try:
        return ipaddress.ip_address(host) == ipaddress.ip_address(hostaddr)
    except ValueError:
        return False


def postgresql_table_address(
    host: str, port: int, database: str, schema: str, table: str
) -> str:
    """Return ``<host>:<port>/<database>.<schema>.<table>``, a PostgreSQL table's address.

    The default name of the source (its id is ``postgresql_table_v1:<address>``; ITL-1267, ITL-1425):
    the server the connection reached, then the table's three-part name. The host is
    written by ``_server_name``; the port is always the number. The address names one
    table only: the parts of the three-part name hold neither ``.`` nor ``/``, so the
    last ``/`` ends the server and the last ``:`` before it ends the host.

    Args:
        host: The host the connection reports: a host name, an IP address or a
            socket directory.
        port: The port the connection used.
        database: The connection's ``current_database()``.
        schema: The connection's ``current_schema()``.
        table: The table's name.

    Returns:
        The table's address.

    Raises:
        ValueError: If the host is empty, the database, schema or table name is
            empty or holds ``.`` or ``/``, or the address would contain ``::`` (a
            socket directory holding it, or a host ending with ``:``); give the
            source a ``source_id``.
    """
    if not host:
        raise ValueError(
            "the PostgreSQL connection reports no host, so the table has no default "
            "source id; give the source a source_id"
        )
    joined_location("/", database, schema, table)
    name = joined_location(".", database, schema, table)
    address = f"{_server_name(host)}:{port}/{name}"
    if "::" in address:
        raise ValueError(
            f"the address {address!r} of the PostgreSQL table contains '::' (in the "
            "host, such as a socket directory, or in a name), which separates the "
            "parts of a source-info token, so the table has no default source id; "
            "give the source a source_id"
        )
    return address


class PostgreSQLTableSource(DBTableSource):
    """A read-only Source backed by a table in a PostgreSQL database.

    At construction time the source:
    1. Stores the DSN for serialisation.
    2. Opens a ``PostgreSQLConnector`` for *dsn*.
    3. Delegates to ``DBTableSource.__init__``, which validates the table,
       resolves tag columns (defaults to PK columns), fetches all rows as
       Arrow batches, and builds the stream.
    4. Closes the connector — all data is eagerly loaded into memory, so the
       connection is released immediately.

    PostgreSQL PK columns are always ``NOT NULL``, so NULL tag values can
    only arise when *tag_columns* is overridden to point at a nullable
    column. Such NULLs are passed through as-is (Arrow supports nulls).

    Args:
        dsn: libpq connection string.
            URI form: ``"postgresql://user:pass@host:5432/dbname"``
            Keyword form: ``"host=localhost dbname=mydb user=alice"``
        table_name: Name of the table to expose as a source.
        tag_columns: Columns to use as tag columns. A bare string is
            accepted as a single column name. If ``None`` (default),
            the table's primary-key columns are used. Raises ``ValueError``
            if the table has no primary key and no explicit columns are given.
        system_tag_columns: Additional system-level tag columns.
        record_id_columns: The key columns that identify a row; ``None``: the
            primary key (a table without one is refused); ``()``: the position.
        source_id: The source's name; its id is ``postgresql_table_v1:<name>``
            (ITL-1425). The name defaults to
            ``<host>:<port>/<database>.<schema>.<table>``
            (``postgresql_table_address``); connection parameters that name
            several servers, or a host beside a ``hostaddr``, give no default.
        label: Human-readable label for this source node.
        data_context: Data context governing type conversion and hashing.
        config: OrcaPod configuration.
        tag_only_columns: Tag columns kept as tags only, not data, exactly as in
            v0.3; ``True`` for all of them. Keyword-only. Every other tag column is
            a data-tag: also a data column of the same name (ITL-1023).

    Raises:
        ValueError: If the table is not found, is empty, or has no PK and
            no *tag_columns* are given.
        psycopg.OperationalError: If the DSN is invalid or connection fails.
    """

    SOURCE_KIND: ClassVar[str] = "postgresql_table"
    """Config type key and the kind in the source id's kind part (one vocabulary)."""
    SOURCE_VERSION: ClassVar[int] = 1
    """The version of this kind's implementation, in the kind part (ITL-1425)."""
    TABLE_IS_ROWS: ClassVar[bool] = True
    _carried_table = DBTableSource._carried_from_stream

    def __init__(
        self,
        dsn: str,
        table_name: str,
        tag_columns: str | Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_columns: str | Collection[str] | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: OrcapodConfig | None = None,
        *,
        tag_only_columns: TagOnlyColumns = (),
    ) -> None:
        self._dsn = dsn  # store before try — needed by to_config even if super() raises
        connector = PostgreSQLConnector(
            dsn
        )  # outside try — if this raises, finally never runs
        try:
            super().__init__(
                connector,
                table_name,
                tag_columns=tag_columns,
                system_tag_columns=system_tag_columns,
                record_id_columns=record_id_columns,
                source_id=source_id,
                label=label,
                data_context=data_context,
                config=config,
                tag_only_columns=tag_only_columns,
            )
        finally:
            try:
                connector.close()
            except Exception:
                pass  # suppress close errors; don't mask original __init__ failure

    @override
    def _table_address(self, connector: DBConnectorProtocol, table_name: str) -> str:
        """Return ``<host>:<port>/<database>.<schema>.<table>``, the table's address.

        The server the connection reached (host and port as libpq reports them),
        then the table's three-part name, ``database.schema.table``, from the
        connection's ``current_database()`` and ``current_schema()``, where the
        connector finds the table (ITL-1154, ITL-1267). Built by
        ``postgresql_table_address``.

        Args:
            connector: The connector the source reads through.
            table_name: The table's name.

        Returns:
            The table's address.

        Raises:
            ValueError: If the connection parameters name several servers (the one
                reached can change from one connection to the next), or give a
                ``hostaddr`` beside a host that is not that address (the connection
                is sent to the address, so the host does not name the server).
        """
        pg = cast("PostgreSQLConnector", connector)
        host, hostaddr = pg.given_server()
        if "," in host or "," in hostaddr:
            raise ValueError(
                f"the connection parameters of the PostgreSQL source over table "
                f"{table_name!r} (its DSN, PGHOST or PGHOSTADDR, or a service file) "
                "name several servers, and the one reached can change from one "
                "connection to the next, so the table has no default source id; give "
                "the source a source_id"
            )
        if host and hostaddr and not _one_address(host, hostaddr):
            raise ValueError(
                f"the connection parameters of the PostgreSQL source over table "
                f"{table_name!r} (its DSN, PGHOST or PGHOSTADDR, or a service file) "
                f"give the host {host!r} and the address {hostaddr!r}: the connection "
                "is sent to the address, so the host name alone does not identify "
                "the server, and the table has no default source id; give the source "
                "a source_id"
            )
        host, port, database, schema = pg.current_location()
        return postgresql_table_address(host, port, database, schema, table_name)

    @contextmanager
    @override
    def _open_connector(self) -> Iterator[PostgreSQLConnector]:
        """Open a new connector for a check and close it afterwards.

        The connector the source was built with was closed once the table was read.
        """
        connector = PostgreSQLConnector(self._dsn)
        try:
            yield connector
        finally:
            connector.close()

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict."""
        # super().to_config() calls self._connector.to_config(), which reads only
        # self._connector._dsn (a stored string) — safe even though the connector
        # was closed after construction.
        base = super().to_config()
        base.pop("connector", None)
        return {**base, "source_type": self.SOURCE_KIND, "dsn": self._dsn}

    @classmethod
    def from_config(
        cls, config: dict[str, Any], db_registry=None
    ) -> PostgreSQLTableSource:
        """Reconstruct a PostgreSQLTableSource from a config dict.

        Args:
            config: Dict as produced by ``to_config()``.

        Returns:
            A new ``PostgreSQLTableSource`` instance.
        """
        return cls._from_saved_name(
            config,
            lambda name: cls(
                dsn=config["dsn"],
                table_name=config["table_name"],
                tag_columns=config.get("tag_columns"),
                tag_only_columns=saved_tag_only_columns(config),
                system_tag_columns=config.get("system_tag_columns", ()),
                record_id_columns=config.get("record_id_columns"),
                source_id=name,
                label=config.get("label"),
                data_context=config.get("data_context"),
            ),
        )
