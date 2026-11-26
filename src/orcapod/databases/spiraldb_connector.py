"""SpiralDBConnector — DBConnectorProtocol implementation backed by SpiralDB (spiral-sdk).

Requires the ``spiraldb`` optional extra: ``pip install orcapod[spiraldb]``.
Authentication is spiral-sdk's: ``SPIRAL_*`` environment variables, or the credentials
``spiral login`` saved.

The connector is dataset-scoped: all tables are read from and written to
a single ``(project_id, dataset)`` pair. They are SpiralDB's keyed tables. The schema each
is read with is in the dataset's schema table (``spiraldb_schema_table``).

Example::

    connector = SpiralDBConnector(project_id="my-project-123456", dataset="default")
    rows = pa.Table.from_batches(connector.scan_batches("units"))
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import urllib.parse
import urllib.request
import warnings
from collections.abc import Collection, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager, nullcontext
from copy import copy
from threading import local
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import pyarrow.compute as pc
from typing_extensions import override

from orcapod.databases.spiraldb_nullability import (
    cast_changed_columns,
    check_no_nulls,
    schema_paths,
)
from orcapod.databases.spiraldb_store_tables import StoreTables
from orcapod.databases.spiraldb_schema_table import (
    SCHEMA_TABLE,
    SchemaTable,
    compared_columns,
    describe_difference,
)
from orcapod.databases.spiraldb_sign_in import require_sign_in
from orcapod.databases.table_names import (
    joined_table_name,
    joined_table_paths,
    whole_table,
)
from orcapod.errors import (
    NullInNonNullableColumnError,
    SpiralDBWriteNotSupportedError,
    TableSchemaError,
    UnrecordedTableSchemaWarning,
)
from orcapod.types import ColumnInfo
from orcapod.utils import arrow_utils
from orcapod.utils.warning_utils import outside_orcapod_stacklevel

if TYPE_CHECKING:
    import spiral as sp
else:
    from orcapod.utils.lazy_module import LazyModule

    sp = LazyModule("spiral")

logger = logging.getLogger(__name__)

# The largest batch a scan hands out. SpiralDB's default is 1,024 rows.
_SCAN_BATCH_ROWS = 8_192
_LIST_TIMEOUT_S = 60
_NO_WRITES = (
    "Orcapod does not write SpiralDB tables through spiral-sdk yet; it reads them. Writing "
    "is a follow-up of ITL-1186. Nothing was written. Store a job's results in another "
    "database, for example DeltaTableDatabase."
)
# Tables a warning was issued for in this process (``UnrecordedTableSchemaWarning``).
_warned_unrecorded: set[tuple[str, str]] = set()
_warning_here = local()  # ``keys``: what this thread is warning about right now


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _parse_table_name(query: str) -> str:
    """Extract the plain table name from a SELECT * FROM ... query string.

    Supports double-quoted identifiers (``"table_name"``) and unquoted bare
    identifiers (``table_name``). Always returns the plain table name — not
    a ``dataset.table`` qualified form.

    Args:
        query: SQL-style query string, e.g. ``'SELECT * FROM "my_table"'``.

    Returns:
        Plain table name string.

    Raises:
        ValueError: If no table name can be parsed from the query.
    """
    m = re.search(r'FROM\s+"([^"]+)"', query, re.IGNORECASE)
    if not m:
        m = re.search(r"FROM\s+(\w+)", query, re.IGNORECASE)
    if not m:
        raise ValueError(f"Cannot parse table name from query: {query!r}")
    return m.group(1)


@contextmanager
def _about_table(table: str) -> Iterator[None]:
    """Re-raise a schema or null error that does not name its table with ``table`` added.

    Args:
        table: The table, as ``"<project>/<dataset>/<table>"``.
    """
    try:
        yield
    except TableSchemaError as exc:
        if exc.table is not None:
            raise
        raise TableSchemaError(exc.reason, table=table) from exc
    except NullInNonNullableColumnError as exc:
        if exc.table is not None:
            raise
        raise NullInNonNullableColumnError(exc.column, table=table) from exc


# ---------------------------------------------------------------------------
# SpiralDBConnector
# ---------------------------------------------------------------------------


class SpiralDBTableNotFoundError(ValueError):
    """An exact table lookup found no matching table."""


@contextmanager
def _service_call(doing: str) -> Iterator[None]:
    """Note on a failing SDK call what Orcapod was doing: SpiralDB's errors often hold a
    status only.

    Args:
        doing: What the call does, and to what.
    """
    try:
        yield
    except Exception as exc:
        # The SDK's own error class is not named here: it may not be installed.
        if not isinstance(exc, (TableSchemaError, NullInNonNullableColumnError)):
            exc.add_note(f"SpiralDB: could not {doing}")
        raise


def _open_keyed_table(
    client: Any, project_id: str, dataset: str, name: str
) -> Any | None:
    """Return the latest snapshot of a keyed table, or ``None`` if there is none.

    SpiralDB has no error class for a missing table. Only its exact message for this
    name means "no such table"; a missing project, a refused sign-in or a transport
    failure has another and is raised.
    """
    with _service_call(f"open table {project_id}/{dataset}/{name}"):
        try:
            return client.keyed_table(project_id, dataset, name)
        except sp.SpiralError as exc:
            if str(exc).strip() != f"resource not found: {project_id}.{dataset}.{name}":
                raise
            return None


def _warn_unrecorded(table: str, table_id: str) -> None:
    """Warn once per table and process that its schema is not recorded.

    Args:
        table: The table, as ``"<project>/<dataset>/<table>"``.
        table_id: SpiralDB's id of it.
    """
    _warn_once(
        (table, table_id),
        f"SpiralDB table {table} has no row in its dataset's {SCHEMA_TABLE}: every column "
        "is read as nullable and without Arrow metadata. If an Orcapod release on "
        "pyspiral wrote it, migrate the dataset with scripts/migrate_spiral_schemas.py "
        "(its schema and hashes differ until then).",
    )


def _warn_once(about: tuple[str, str], message: str) -> None:
    """Issue an ``UnrecordedTableSchemaWarning``, usually once per ``about`` and process.

    Once it has been shown it is not shown again. Threads reading one table at the same
    time may each show it: no lock is held while warning, since a warning hook may wait
    for another thread that reads an unrecorded table. A warning turned into an error is
    not recorded as shown, so every reader raises it. A hook that reads the table itself
    is not warned again about it.

    Args:
        about: What the warning is about.
        message: The warning.
    """
    warning_now: set[tuple[str, str]] | None = getattr(_warning_here, "keys", None)
    if warning_now is None:
        warning_now = _warning_here.keys = set()
    if about in _warned_unrecorded or about in warning_now:
        return
    warning_now.add(about)
    try:
        warnings.warn(
            message,
            UnrecordedTableSchemaWarning,
            stacklevel=outside_orcapod_stacklevel(),
        )
    finally:
        warning_now.discard(about)
    _warned_unrecorded.add(about)


# ---------------------------------------------------------------------------
# The client
# ---------------------------------------------------------------------------


def require_same_process(pid: int) -> None:
    """Refuse a SpiralDB client made in another process: after a fork a call of it hangs,
    and a lock another thread held stays locked.

    Args:
        pid: The process that made the client.

    Raises:
        RuntimeError: If this is another process.
    """
    if pid != os.getpid():
        raise RuntimeError(
            "this SpiralDB client was made in another process, and cannot be used after "
            "a fork. Make a new one in a process started with 'spawn'"
        )


def _api_url(overrides: Mapping[str, str] | None) -> str | None:
    """Return the service address that saved ``overrides`` name, if any.

    Args:
        overrides: ``None``, or a mapping with the one key ``server.url``.

    Raises:
        ValueError: For any other key: spiral-sdk has no such option, and ignoring it
            could read another service than the one named.
    """
    options = dict(overrides or {})
    api_url = options.pop("server.url", None)
    if options:
        raise ValueError(
            f"SpiralDB overrides {sorted(options)} are not options of spiral-sdk; the "
            "only override is 'server.url'"
        )
    return api_url


def _new_client(overrides: Mapping[str, str] | None) -> Any:
    """Return a spiral-sdk client.

    Raises:
        SpiralDBSignInError: If spiral-sdk would have to ask a person to sign in.
        ImportError: If spiral-sdk is not installed, or another package provides the
            ``spiral`` module (``pyspiral``, which must not be installed beside it).
    """
    api_url = _api_url(overrides)
    try:
        client_class = sp.Client
    except AttributeError as exc:
        raise ImportError(
            "the installed 'spiral' module is not spiral-sdk (pyspiral has the same "
            "module name and must not be installed beside it): uninstall both, then "
            'install "orcapod[spiraldb]"'
        ) from exc
    require_sign_in()
    return client_class(api_url=api_url)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Follows no redirect, so a token is never sent to another address."""

    @override
    def redirect_request(
        self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str
    ) -> None:
        return None


def _http_get_json(url: str, token: str | None) -> dict[str, Any]:
    """Return the JSON object at ``url``.

    Args:
        url: The address.
        token: The bearer token; ``None`` sends none.
    """
    headers = {"Accept": "application/json", "User-Agent": "orcapod"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    opener = urllib.request.build_opener(_NoRedirect)
    request = urllib.request.Request(url, headers=headers)
    with opener.open(request, timeout=_LIST_TIMEOUT_S) as response:
        return json.load(response)


def _list_keyed_tables(client: Any, project_id: str, dataset: str) -> dict[str, str]:
    """Return a dataset's keyed tables: their names by table id.

    spiral-sdk offers no listing of keyed tables, so this is the documented endpoint
    (docs.spiraldb.com, "List Keyed tables"), called with the client's address and
    sign-in. These two are not public API of the SDK: the pinned version has them.

    Args:
        client: A spiral-sdk client.
        project_id: The project.
        dataset: The dataset; matched exactly by the service.
    """
    native = client._native  # no public accessor of the address and sign-in
    base = (
        f"{str(native.api_url).rstrip('/')}/api/keyed-tables/projects/"
        f"{urllib.parse.quote(project_id, safe='')}/tables"
    )
    names: dict[str, str] = {}
    token: str | None = None
    seen: set[str] = set()
    while True:
        query = {"dataset": dataset}
        if token is not None:
            query["pageToken"] = token
        page = _http_get_json(
            f"{base}?{urllib.parse.urlencode(query)}", native.access_token()
        )
        # A page can be empty before the last one.
        names.update(
            (t["id"], t["table"]) for t in page["items"] if t["dataset"] == dataset
        )
        following = page.get("nextPageToken")
        if following is None:
            return names
        if not following or following in seen:
            raise RuntimeError("SpiralDB's table listing does not advance")
        seen.add(following)
        token = following


def _literal(value: Any, arrow_type: pa.DataType) -> Any:
    """Return ``value`` as a scan literal of exactly ``arrow_type`` (the service refuses
    another type), or ``None`` when the SDK has no literal that filters as Orcapod does.

    Args:
        value: A condition value checked against the column.
        arrow_type: The column's type as SpiralDB stores it.
    """
    if pa.types.is_large_string(arrow_type):
        arrow_type = pa.string()
    elif pa.types.is_large_binary(arrow_type):
        arrow_type = pa.binary()
    if pa.types.is_float64(arrow_type):
        return sp.literal(value, arrow_type)
    if pa.types.is_float32(arrow_type) and (
        math.isnan(value) or pa.scalar(value, pa.float32()).as_py() == value
    ):
        return sp.literal(value, arrow_type)
    if pa.types.is_timestamp(arrow_type):
        # As a count of the unit: the SDK takes no datetime between two of them.
        return sp.literal(pa.scalar(value, type=arrow_type).value, arrow_type)
    if (
        pa.types.is_string(arrow_type)
        or pa.types.is_binary(arrow_type)
        or pa.types.is_boolean(arrow_type)
        or pa.types.is_integer(arrow_type)
        or pa.types.is_date(arrow_type)
    ):
        return sp.literal(value, arrow_type)
    return None


def _equals(array: Any, value: Any) -> Any:
    """Return where ``array`` equals ``value``, as SpiralDB compares: floats as 64-bit
    numbers, ``0.0`` not equal to ``-0.0`` and NaN equal to NaN; a null is no match.

    Args:
        array: The column.
        value: A condition value checked against the column.
    """
    compute: Any = pc  # pyarrow's stubs take no plain Python scalar
    if not pa.types.is_floating(array.type):
        mask = compute.equal(array, pa.scalar(value, type=array.type))
        return compute.fill_null(mask, False)
    doubles = array.cast(pa.float64())
    if math.isnan(value):
        return compute.fill_null(compute.is_nan(doubles), False)
    mask = compute.equal(doubles, pa.scalar(value, pa.float64()))
    if value == 0.0:
        negative = compute.less(compute.divide(1.0, doubles), 0.0)
        wanted = math.copysign(1.0, value) < 0
        mask = compute.and_(mask, compute.equal(negative, wanted))
    return compute.fill_null(mask, False)


class SpiralDBTable:
    """One SpiralDB table, looked up once (``SpiralDBConnector.lookup_table``).

    Every lookup of a table by name is a round trip to SpiralDB. A caller that needs
    several items of one table (a source's id, key, schema and version when it is built,
    and each run's check) takes them from one handle instead of looking the table up for
    each (ITL-1350). The handle is a snapshot: the id, key columns and version come with
    the lookup, and the schema and rows are those of that version. Look the table up
    again to see a newer version, or a table dropped and created again since.
    """

    def __init__(
        self, connector: SpiralDBConnector, table: Any, table_name: str
    ) -> None:
        """Wrap the keyed ``table`` that ``connector`` looked up as ``table_name``."""
        self._connector = connector
        self._table = table
        self._table_name = table_name
        self._readings: dict[str, _Reading] = {}

    def _reading(self, table: Any) -> _Reading:
        """Return how a snapshot of this table is read, worked out once per version."""
        self._connector._require_open()  # pyright: ignore[reportPrivateUsage]  # owns the handle
        version = str(table.version_id)
        if version not in self._readings:
            self._readings[version] = self._connector._reading(table, self._table_name)  # pyright: ignore[reportPrivateUsage]  # owns the handle
        return self._readings[version]

    @property
    def table_id(self) -> str:
        """SpiralDB's own id of the table (no round trip)."""
        return str(self._table.resource_id)

    @property
    def key_columns(self) -> list[str]:
        """The primary-key column names in declaration order (no round trip)."""
        return list(self._table.key_columns)

    def arrow_schema(self) -> pa.Schema:
        """The schema of the batches ``scan_batches`` yields (``get_arrow_schema``).

        Raises:
            TableSchemaError: If the table's row in the schema table cannot be read, or
                disagrees with the table.
        """
        return self._reading(self._table).schema

    def version(self) -> int:
        """The table's version at the lookup (``get_table_version``)."""
        self._connector._require_open()  # pyright: ignore[reportPrivateUsage]  # owns the handle
        return int(self._table.version_id)

    def _at(self, asof: int | None) -> Any:
        """Return the SDK's handle of this same table at version ``asof``."""
        self._connector._require_open()  # pyright: ignore[reportPrivateUsage]  # owns the handle
        if asof is None or str(asof) == str(self._table.version_id):
            return self._table
        with self._connector._service_call("open a version of", self._table_name):  # pyright: ignore[reportPrivateUsage]  # owns the handle
            return self._connector._client.keyed_table_by_id(  # pyright: ignore[reportPrivateUsage]  # owns the handle
                self.table_id, asof=asof
            )

    def scan_batches(
        self,
        *,
        where: Mapping[str, object] | None = None,
        limit: int | None = None,
        asof: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Yield prepared Arrow batches from this resolved table in key order."""
        connector = self._connector
        table = self._at(asof)
        reading = self._reading(table)
        predicate = None
        local: dict[str, object] = {}
        for column, value in (where or {}).items():
            column_type = reading.found.field(column).type
            literal = _literal(value, column_type)
            if literal is None and pa.types.is_float32(column_type):
                # No float32 is exactly this value. The SDK would round it, so 0.1 would
                # match the float32 nearest to it; a comparison of the values does not.
                return
            if literal is None:
                local[column] = value
                continue
            equals = sp.eq(sp.column(column), literal)
            predicate = equals if predicate is None else sp.and_(predicate, equals)
        with connector._service_call("read", self._table_name):  # pyright: ignore[reportPrivateUsage]  # owns the handle
            reader = table.scan(
                filter=predicate,
                # The limit counts rows that match every condition.
                limit=None if local else limit,
                batch_rows=_SCAN_BATCH_ROWS,
            ).to_reader()
        left = limit
        try:
            while left is None or left > 0:
                with connector._service_call("read", self._table_name):  # pyright: ignore[reportPrivateUsage]  # owns the handle
                    batch = next(reader, None)
                if batch is None:
                    return
                batch = reading.prepare(batch)
                for column, value in local.items():
                    batch = batch.filter(_equals(batch.column(column), value))
                if local and left is not None:
                    batch = batch.slice(0, left)
                    left -= batch.num_rows
                if batch.num_rows or not local:
                    yield batch
        finally:
            reader.close()

    def count_rows(self, *, asof: int | None = None, at_most: int | None = None) -> int:
        """Count the table's rows by reading one key column.

        SpiralDB offers no count of a keyed table, so this reads every key, or the first
        ``at_most``. A version never changes, so what was counted is remembered by the
        connector and those that share its client.

        Args:
            asof: Count this version; ``None`` for the version of the lookup.
            at_most: Stop at this many rows; ``None`` counts them all.

        Returns:
            The number of rows, or ``at_most`` when there are at least that many.
        """
        connector = self._connector
        table = self._at(asof)
        # Refuses what a read would: a schema table that is not complete.
        _ = self._reading(table)
        counts = connector._counts  # pyright: ignore[reportPrivateUsage]  # owns the handle
        known = (self.table_id, str(table.version_id))
        counted, exact = counts.get(known, (0, False))
        if not exact and (at_most is None or counted < at_most):
            with connector._service_call("count the rows of", self._table_name):  # pyright: ignore[reportPrivateUsage]  # owns the handle
                reader = table.scan(
                    [self.key_columns[0]], limit=at_most, batch_rows=_SCAN_BATCH_ROWS
                ).to_reader()
                try:
                    counted = sum(batch.num_rows for batch in reader)
                finally:
                    reader.close()
            exact = at_most is None or counted < at_most
            counts[known] = (counted, exact)
        return counted if at_most is None else min(counted, at_most)

    def row_count_estimate(self, asof: int | None = None) -> int:
        """Return the table's row count, exact, by reading every key (``count_rows``)."""
        return self.count_rows(asof=asof)


class _Reading:
    """How one snapshot is read: its schema, and each batch's preparation.

    Attributes:
        found: The snapshot's schema, as SpiralDB reports it.
        schema: The schema it is read with.
    """

    def __init__(
        self, found: pa.Schema, recorded: pa.Schema | None, table: str
    ) -> None:
        """Prepare reading a snapshot.

        Args:
            found: The snapshot's schema, as SpiralDB reports it.
            recorded: The schema its row records, or ``None``: as SpiralDB reports it.
            table: The table, as ``"<project>/<dataset>/<table>"``.
        """
        self.found = found
        if recorded is None:
            self.schema = pa.schema(
                [
                    f.with_type(
                        arrow_utils.normalize_to_large_types(f.type, keep_null=True)
                    )
                    for f in found
                ],
                metadata=cast("dict[bytes | str, bytes | str] | None", found.metadata),
            )
            self._paths = frozenset[tuple[str, ...]]()
        else:
            self.schema = recorded
            self._paths = schema_paths(recorded)
        self._table = table
        # The batch schema last found to have the snapshot's columns.
        self._accepted = found

    def prepare(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Return a scanned batch under ``schema``, its non-null columns checked.

        Raises:
            TableSchemaError: If the batch's columns are not the snapshot's.
            NullInNonNullableColumnError: If a non-nullable column holds a null.
        """
        if not batch.schema.equals(self._accepted, check_metadata=True):
            # By name, and only a batch with exactly the snapshot's columns.
            if sorted(batch.schema.names) == sorted(self.found.names):
                batch = batch.select(self.found.names)
            if compared_columns(batch.schema) != compared_columns(self.found):
                raise TableSchemaError(
                    "a batch's columns are not those of the version it was read from: "
                    f"{describe_difference(batch.schema, self.found)}",
                    table=self._table,
                )
            self._accepted = batch.schema
        with _about_table(self._table):
            check_no_nulls(batch, self._paths)
        if batch.schema.equals(self.schema, check_metadata=True):
            return batch
        return cast_changed_columns(batch, self.schema)


class SpiralDBConnector:
    """DBConnectorProtocol implementation backed by SpiralDB (spiral-sdk).

    Scoped to a single dataset within a SpiralDB project, and to its keyed tables. Auth
    is spiral-sdk's: ``SPIRAL_*`` environment variables, or a saved ``spiral login``.

    Args:
        project_id: SpiralDB project identifier (e.g. ``"my-project-123456"``).
        dataset: Dataset within the project. Defaults to ``"default"``.
        overrides: Optional: ``{"server.url": "https://api.spiraldb.dev"}``, the address
            of the service. Any other key is refused.
        _store: Open the dataset as a pipeline store (``spiraldb_store_tables``):
            tables named from their paths, rows through the store codec. Private until
            the store is switched on (ITL-1446, stage 5): such a connector is not saved.
    """

    def __init__(
        self,
        project_id: str,
        dataset: str = "default",
        overrides: dict[str, str] | None = None,
        *,
        _store: bool = False,
    ) -> None:
        self._project_id = project_id
        self._dataset = dataset
        self._overrides = overrides
        self._closed = False
        self._client = _new_client(overrides)
        self._pid = os.getpid()
        # Shared by every connector on this client (``share_client``).
        self._counts: dict[tuple[str, str], tuple[int, bool]] = {}
        client = self._client
        self._store: StoreTables | None = None
        if _store:
            self._store = StoreTables(
                client,
                project_id,
                dataset,
                lambda name: _open_keyed_table(client, project_id, dataset, name),
                lambda: _list_keyed_tables(client, project_id, dataset),
                lambda doing: _service_call(f"{doing} of {project_id}/{dataset}"),
            )
        self._schemas = SchemaTable(
            project_id,
            dataset,
            lambda: _open_keyed_table(client, project_id, dataset, SCHEMA_TABLE),
            lambda doing: _service_call(f"{doing} of {project_id}/{dataset}"),
        )

    def _require_open(self) -> None:
        """Raise RuntimeError if this connector has been closed."""
        if self._closed:
            raise RuntimeError("SpiralDBConnector is closed")
        require_same_process(self._pid)

    def share_client(self) -> SpiralDBConnector:
        """Return an independently closable connector in the same SDK client scope."""
        self._require_open()
        return copy(self)

    def _qualified(self, table_name: str) -> str:
        """Return ``"<project>/<dataset>/<table>"``, the table as errors name it."""
        return f"{self._project_id}/{self._dataset}/{table_name}"

    def _service_call(self, doing: str, table_name: str) -> Any:
        """Return a context that notes on a failing SDK call what it was doing."""
        return _service_call(f"{doing} {self._qualified(table_name)}")

    def _require_user_table(self, table_name: str) -> None:
        """Refuse the reserved name of the dataset's schema table."""
        if table_name == SCHEMA_TABLE:
            raise ValueError(
                f"{SCHEMA_TABLE!r} is reserved: it is where Orcapod keeps the schemas of "
                f"the tables of dataset {self._dataset!r}"
            )

    def table_name(self, path: tuple[str, ...]) -> str:
        """Return the name of the table at a full record path.

        A store names it from the path (``spiral_store_names``); otherwise the joined
        rule of the relational connectors (``table_names.joined_table_name``).
        """
        if self._store is not None:
            return self._store.table_name(path)
        return joined_table_name(path)

    def has_table(self, table_name: str) -> bool:
        """Return whether the table exists; a store opens it by name, with no listing."""
        self._require_open()
        if self._store is not None:
            return self._store.has_table(table_name)
        return table_name in self.get_table_names()

    def table_paths(
        self, prefix: tuple[str, ...], pending: Collection[tuple[str, ...]]
    ) -> list[tuple[str, ...]]:
        """Return the full paths of the tables under ``prefix``, stored or pending.

        A store gives the paths its tables record; otherwise the joined rule's form.
        """
        self._require_open()
        if self._store is not None:
            return self._store.table_paths(prefix, pending)
        names = {*self.get_table_names(), *(joined_table_name(p) for p in pending)}
        return joined_table_paths(names, prefix)

    def run_scope(self) -> AbstractContextManager[None]:
        """The scope of one run: a store holds one existence answer per table and one
        listing in it (``StoreTables.run_scope``); a plain connector holds nothing."""
        if self._store is not None:
            return self._store.run_scope()
        return nullcontext()

    def read_candidates(
        self,
        table_name: str,
        *,
        keys: Collection[bytes] | None = None,
        equal: Mapping[str, Any] | None = None,
    ) -> pa.Table | None:
        """Return the rows of a table that may match a lookup (ITL-1502).

        A store reads only the asked keys and pushes equality down to the scan
        (``StoreTables.read_candidates``); a plain connector reads the whole table.
        """
        self._require_open()
        if self._store is not None:
            return self._store.read_candidates(table_name, keys=keys, equal=equal)
        return whole_table(self.iter_batches(f'SELECT * FROM "{table_name}"'))

    def _refuse_in_store_mode(self, what: str) -> None:
        """Refuse an operation on raw tables in a store-mode connector."""
        if self._store is not None:
            raise ValueError(
                f"{what} reads raw SpiralDB tables; a store-mode connector reads its "
                "tables through the store codec only"
            )

    def get_table_names(self) -> list[str]:
        """Return all table names in this connector's dataset, sorted alphabetically.

        The dataset's schema table is not one of them. A table it records that the
        dataset no longer holds is warned about.

        Raises:
            TableSchemaError: If the dataset's schema table is not complete.

        Returns:
            Sorted list of plain table name strings (no ``dataset.`` prefix).
        """
        if self._store is not None:
            self._require_open()
            return self._store.table_names()
        self._require_open()
        with self._service_call("list the tables of", "*"):
            listed = _list_keyed_tables(self._client, self._project_id, self._dataset)
        with _about_table(f"{self._project_id}/{self._dataset}"):
            recorded = (
                (self._schemas.rows() or {}) if SCHEMA_TABLE in listed.values() else {}
            )
        gone = sorted(row.table_name for i, row in recorded.items() if i not in listed)
        if gone:
            _warn_once(
                (f"{self._project_id}/{self._dataset}", str(gone)),
                f"the {SCHEMA_TABLE} of SpiralDB dataset {self._project_id}/"
                f"{self._dataset} records tables that the dataset no longer holds: {gone}",
            )
        return sorted(name for name in listed.values() if name != SCHEMA_TABLE)

    def lookup_table(self, table_name: str) -> SpiralDBTable:
        """Look the table up once, for several items of its metadata (ITL-1350).

        Args:
            table_name: Plain table name (no dataset prefix).

        Returns:
            The table: its id and key columns, and its schema and version when asked.

        Raises:
            SpiralDBTableNotFoundError: If there is no such table.
            ValueError: If the name is the schema table's.
        """
        self._refuse_in_store_mode("lookup_table")
        self._require_open()
        self._require_user_table(table_name)
        missing = f"Table not found: {self._project_id}.{self._dataset}.{table_name}"
        if (
            not self._dataset
            or not table_name
            or "." in self._dataset
            or "." in table_name
        ):
            raise SpiralDBTableNotFoundError(missing)
        table = _open_keyed_table(
            self._client, self._project_id, self._dataset, table_name
        )
        if table is None:
            raise SpiralDBTableNotFoundError(missing)
        return SpiralDBTable(self, table, table_name)

    def _recorded_schema(
        self, table: Any, table_name: str, found: pa.Schema
    ) -> pa.Schema | None:
        """Return the schema recorded for an opened table; ``None``: as SpiralDB reports.

        Args:
            table: The SDK's handle.
            table_name: Plain table name.
            found: The handle's schema, as SpiralDB reports it.

        Raises:
            TableSchemaError: If the dataset's schema table cannot be read or is not
                complete, or the table's row disagrees with the table.
        """
        row = self._schemas.row(str(table.resource_id))
        if row is None:
            _warn_unrecorded(self._qualified(table_name), str(table.resource_id))
            return None
        if row.table_name != table_name:
            raise TableSchemaError(
                f"its row in {SCHEMA_TABLE} is of table {row.table_name!r}, which has "
                "this id: the table was renamed since it was recorded"
            )
        if row.key_columns != tuple(table.key_columns):
            raise TableSchemaError(
                f"its row in {SCHEMA_TABLE} has key columns {list(row.key_columns)}, "
                f"but the table has {list(table.key_columns)}"
            )
        if compared_columns(row.schema) != compared_columns(found):
            raise TableSchemaError(
                f"the columns of version {table.version_id} are not those of the table's "
                f"row in {SCHEMA_TABLE}: "
                f"{describe_difference(found, row.schema)}"
            )
        return row.schema

    def _reading(self, table: Any, table_name: str) -> _Reading:
        """Return how an opened snapshot of ``table_name`` is read.

        Raises:
            TableSchemaError: If the table's row cannot be read or disagrees with it.
        """
        self._require_open()
        qualified = self._qualified(table_name)
        with self._service_call("read the schema of", table_name):
            found = table.schema()
        with _about_table(qualified):
            recorded = self._recorded_schema(table, table_name, found)
        return _Reading(found, recorded, qualified)

    def get_pk_columns(self, table_name: str) -> list[str]:
        """Return primary-key column names in declaration order.

        Args:
            table_name: Plain table name (no dataset prefix).

        Returns:
            List of PK column names.
        """
        if self._store is not None:
            return [self._store.key_column]
        return self.lookup_table(table_name).key_columns

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        """Return column metadata with Arrow types.

        SpiralDB is Arrow-native; types are the table's own, normalized to their large
        variants at every nesting level (``arrow_utils.normalize_to_large_types``), the
        same types ``iter_batches`` gives. Only layouts change: widths and nested field
        names are kept, and hashes are unaffected.

        SpiralDB reports every top-level column and every struct field nullable. The
        columns and nested fields that the table's row in the dataset's schema table
        records as non-nullable (written by ``scripts/migrate_spiral_schemas.py``)
        are reported so; without a row, nullability is SpiralDB's, with a warning
        (``UnrecordedTableSchemaWarning``).

        Args:
            table_name: Plain table name (no dataset prefix).

        Returns:
            List of ColumnInfo objects.

        Raises:
            SpiralDBTableNotFoundError: If the table does not exist.
            TableSchemaError: If the table's row cannot be read or disagrees with it.
        """
        if self._store is not None:
            self._require_open()
            return self._store.column_info(table_name)
        return [
            ColumnInfo(name=f.name, arrow_type=f.type, nullable=f.nullable)
            for f in self.lookup_table(table_name).arrow_schema()
        ]

    def iter_batches(
        self,
        query: str,
        params: Any = None,
        batch_size: int = 1000,
    ) -> Iterator[pa.RecordBatch]:
        """Execute a full-table scan and yield results as Arrow RecordBatches.

        The table name is parsed from the query's FROM clause (double-quoted or
        unquoted). Only ``SELECT * FROM "table"`` patterns are supported; WHERE clauses
        and projections are silently ignored. The query must use a plain
        (non-qualified) table name — not ``"dataset.table"``.

        Args:
            query: SQL-style query. Must contain ``FROM "table_name"``.
            params: Accepted for protocol compliance. If not ``None``, a warning is
                logged and params are not used.
            batch_size: Accepted for protocol compliance: SpiralDB determines batch
                sizes. A warning is logged if a non-default value is passed.

        Yields:
            Arrow RecordBatch objects. Each batch has the schema
            ``get_arrow_schema`` gives: types in their large variants, and the metadata
            and nullability the table's row in the schema table records. Yields nothing
            for an empty table.

        Raises:
            TableSchemaError: If the table's row cannot be read or disagrees with it.
            NullInNonNullableColumnError: If a batch holds a null in a recorded
                non-nullable column (naming the table and the column).
        """
        self._require_open()
        if params is not None:
            logger.warning(
                "SpiralDBConnector does not support query parameters; "
                "ignoring params=%r",
                params,
            )
        if batch_size != 1000:
            logger.warning(
                "SpiralDBConnector does not support batch_size; "
                "batch sizing is controlled by the Spiral execution engine. "
                "Ignoring batch_size=%r",
                batch_size,
            )
        if self._store is not None:
            yield from self._store.scan(_parse_table_name(query))
            return
        yield from self.scan_batches(_parse_table_name(query))

    def scan_batches(
        self,
        table_name: str,
        *,
        where: Mapping[str, object] | None = None,
        limit: int | None = None,
        asof: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Scan a table, optionally restricted, and yield prepared Arrow batches.

        The restrictions are applied by SpiralDB, before any row reaches this
        process, except a condition on a column its literals do not cover (``float16``):
        that one is applied here, to the rows the other conditions select, and ``limit``
        after it, so rows are transferred that are not returned. Batches arrive in key order; their sizes are SpiralDB's. Each batch has
        the types, metadata and nullability ``iter_batches`` documents.

        Args:
            table_name: Plain table name (no dataset prefix).
            where: Column name to value. A row is read only if every named column
                equals its value.
            limit: Read at most this many rows, the first in key order.
            asof: Read the table as of this version (a ``get_table_version`` value).

        Yields:
            Arrow RecordBatch objects. Nothing for a table with no matching row.

        Raises:
            TableSchemaError: If the table's row cannot be read or disagrees with it.
            NullInNonNullableColumnError: If a batch holds a null in a recorded
                non-nullable column (naming the table and the column).
        """
        self._refuse_in_store_mode("scan_batches")
        yield from self.lookup_table(table_name).scan_batches(
            where=where, limit=limit, asof=asof
        )

    def get_arrow_schema(self, table_name: str) -> pa.Schema:
        """Return the schema of the batches ``scan_batches`` yields, without scanning.

        Args:
            table_name: Plain table name (no dataset prefix).

        Returns:
            The prepared Arrow schema: large-normalized types, and the metadata and
            nullability the table's row in the schema table records.

        Raises:
            TableSchemaError: If the table's row cannot be read or disagrees with it.
        """
        if self._store is not None:
            self._require_open()
            return self._store.arrow_schema(table_name)
        return self.lookup_table(table_name).arrow_schema()

    def get_spiral_table_id(self, table_name: str) -> str:
        """Return SpiralDB's own id of the table (for example ``table_W7C01nkLTmEJ``).

        The id stays the same when the table is renamed. A table dropped and created
        again under the same name gets a new one.

        Args:
            table_name: Plain table name (no dataset prefix).

        Returns:
            The table id.
        """
        return self.lookup_table(table_name).table_id

    def get_table_version(self, table_name: str) -> int:
        """Return the table's current version, usable as ``asof`` in ``scan_batches``.

        Args:
            table_name: Plain table name (no dataset prefix).

        Returns:
            The time of the table's last change, in microseconds; ``0`` for a table
            that was never written.
        """
        return self.lookup_table(table_name).version()

    def get_row_count_estimate(self, table_name: str, asof: int | None = None) -> int:
        """Return the table's row count, exact, by reading every key.

        SpiralDB offers no recorded count of a keyed table. The cost grows with the
        table; ``SpiralDBTable.count_rows`` takes a bound.

        Args:
            table_name: Plain table name (no dataset prefix).
            asof: Count the table as of this version (a ``get_table_version`` value,
                as ``scan_batches`` takes). If ``None`` (default), the current version.

        Returns:
            The number of rows.
        """
        return self.lookup_table(table_name).row_count_estimate(asof=asof)

    def delete_table(self, table_name: str) -> None:
        """Refuse to drop a table: Orcapod does not write SpiralDB yet.

        Raises:
            SpiralDBWriteNotSupportedError: Always (ITL-1186).
        """
        self._require_open()
        raise SpiralDBWriteNotSupportedError(_NO_WRITES)

    def create_table_if_not_exists(
        self,
        table_name: str,
        columns: list[ColumnInfo],
        pk_column: str,
    ) -> None:
        """Create a store table; refuse it on a plain connector.

        A store creates the table at a name it computed, unless it exists or is to be
        adopted (``spiraldb_store_tables``). Its columns are recorded by its first
        write.

        Args:
            table_name: A name ``table_name`` returned.
            columns: The columns (recorded by the first write, from its rows).
            pk_column: The key column: the store's ``__record_id``.

        Raises:
            SpiralDBWriteNotSupportedError: On a plain connector (ITL-1186).
            ValueError: If the key column is not the store's.
        """
        self._require_open()
        if self._store is None:
            raise SpiralDBWriteNotSupportedError(_NO_WRITES)
        self._store.require_key_column(pk_column)
        self._store.create(table_name)

    def upsert_records(
        self,
        table_name: str,
        records: pa.Table,
        id_column: str,
        skip_existing: bool = False,
    ) -> None:
        """Write rows to a store table; refuse them on a plain connector.

        Args:
            table_name: A name ``table_name`` returned, of a table ``create_table_if_not_exists``
                made.
            records: The rows, the key column included.
            id_column: The key column: the store's ``__record_id``.
            skip_existing: Leave out rows whose key the table holds.

        Raises:
            SpiralDBWriteNotSupportedError: On a plain connector (ITL-1186).
            ValueError: If the key column is not the store's, the rows hold no single data
                context, or their columns are not the table's.
        """
        self._require_open()
        if self._store is None:
            raise SpiralDBWriteNotSupportedError(_NO_WRITES)
        self._store.require_key_column(id_column)
        self._store.write(table_name, records, skip_existing)

    @property
    def keeps_field_metadata(self) -> bool:
        """Whether this connector keeps Arrow field metadata: it does, in the
        dataset's schema table (ITL-1186); schema evolution is refused by
        ``ConnectorArrowDatabase``."""
        return True

    def unstorable_column_reason(
        self, name: str, arrow_type: pa.DataType, *, primary_key: bool
    ) -> str | None:
        """Say why a store cannot keep a column; refuse every column on a plain connector.

        A store keeps what the store codec keeps (``spiral_store``).

        Args:
            name: The column's name.
            arrow_type: The column's Arrow type.
            primary_key: Whether the column is the table's primary key.

        Raises:
            SpiralDBWriteNotSupportedError: On a plain connector: Orcapod does not write
                its tables (ITL-1186). A job whose database is on one is refused when the
                database is attached, before it runs.
        """
        if self._store is None:
            raise SpiralDBWriteNotSupportedError(_NO_WRITES)
        return self._store.unstorable_column_reason(name, arrow_type, primary_key)

    def unstorable_table_name_reason(self, table_name: str) -> str | None:
        """Say why SpiralDB cannot hold a table of this name: it holds every name.

        A plain connector's names are letters, digits and ``_``; a store's are names it
        computed (``spiral_store_names``), within SpiralDB's 255 characters by
        construction.

        Args:
            table_name: The table name.

        Returns:
            Always ``None``.
        """
        return None

    def validate_records(self, records: pa.Table) -> None:
        """Check rows before a database buffers them; refuse them on a plain connector.

        A store runs the store codec on them (the last row of each key, as the database
        keeps), so a row it cannot keep is refused before anything is written.

        Args:
            records: The rows.

        Raises:
            SpiralDBWriteNotSupportedError: On a plain connector (ITL-1186).
            UnstorableInSpiralError: If the store codec refuses the rows.
        """
        if self._store is None:
            raise SpiralDBWriteNotSupportedError(_NO_WRITES)
        self._store.validate(records)

    def close(self) -> None:
        """Close this connector and release its SDK client. Idempotent."""
        if not self._closed:
            self._closed = True
            # spiral-sdk's client has no close method. Drop the owning references.
            del self._client
            del self._schemas

    def __enter__(self) -> SpiralDBConnector:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def to_config(self) -> dict[str, Any]:
        """Serialize connection configuration to a JSON-compatible dict.

        Returns:
            Dict with ``connector_type``, ``project_id``, ``dataset``, and
            ``overrides`` keys.

        Raises:
            RuntimeError: If this connector has been closed.
        """
        if self._store is not None:
            raise ValueError(
                "a store-mode SpiralDB connector is not saved yet: the store is not "
                "switched on (ITL-1446)"
            )
        self._require_open()
        return {
            "connector_type": "spiraldb",
            "project_id": self._project_id,
            "dataset": self._dataset,
            "overrides": self._overrides,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> SpiralDBConnector:
        """Reconstruct a SpiralDBConnector from a config dict.

        Args:
            config: Dict with ``connector_type``, ``project_id``, and optionally
                ``dataset`` and ``overrides`` keys.

        Returns:
            A new, open SpiralDBConnector instance.

        Raises:
            ValueError: If ``connector_type`` is not ``"spiraldb"``.
        """
        if config.get("connector_type") != "spiraldb":
            raise ValueError(
                f"Expected connector_type 'spiraldb', "
                f"got {config.get('connector_type')!r}"
            )
        return cls(
            project_id=config["project_id"],
            dataset=config.get("dataset", "default"),
            overrides=config.get("overrides"),
        )
