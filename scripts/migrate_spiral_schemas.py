"""Move the schemas of SpiralDB tables written by Orcapod on pyspiral to spiral-sdk.

Orcapod on pyspiral kept each table's non-nullable columns and Arrow metadata in the table's
key-value metadata, which spiral-sdk does not read. This tool records each table's schema in
the dataset's schema table (``__orcapod_schemas__``), which Orcapod on spiral-sdk reads
(``orcapod.databases.spiraldb_schema_table``). It writes nothing else.

Three steps, each in an environment of its own (the two clients cannot be installed
together); ``RELEASING.md`` has the exact commands:

1. ``dry-run``, with the last Orcapod on pyspiral: writes the plan to a file (what each
   table is read as: schema, content hash, pipeline hash, source id) and prints its digest.
2. ``write --digest <digest>``, with Orcapod on spiral-sdk: refuses unless the plan has that
   digest and every table still is as it was planned, then writes one row per table and the
   completion marker last. Run again after an interruption, it continues the same plan.
3. ``verify``, with Orcapod on spiral-sdk alone: every source is what the plan says.

No step signs in: each uses the credentials that are there, names the principal, and stops
without any.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib
import json
import os
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_TABLE = "__orcapod_schemas__"
PLAN_FORMAT = "orcapod.spiral_migration"
PLAN_VERSION = 1
ROW_FORMAT = "orcapod.table_schema"
ROW_FORMAT_VERSION = 1
FIRST_ROWS = 20
CREATING = "creating"
REFUSED = "refused"


class MigrationError(Exception):
    """The step cannot go on; the message says why."""


def canonical(value: Any) -> str:
    """Return ``value`` as JSON in one form."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def plan_digest(plan: dict[str, Any]) -> str:
    """Return the SHA-256 (hex) of the plan's canonical bytes."""
    return hashlib.sha256(canonical(plan).encode()).hexdigest()


def spiral_module() -> Any:
    """Return the ``spiral`` module of this environment: pyspiral's or spiral-sdk's."""
    return importlib.import_module("spiral")


def require_credentials() -> None:
    """Stop unless the client can sign in without asking a person.

    The rule of ``orcapod.databases.spiraldb_sign_in``, stated again: the dry run's
    Orcapod does not have it.
    """
    env = os.environ
    if env.get("SPIRAL_TOKEN") or env.get("SPIRAL_WORKLOAD_ID"):
        return
    if env.get("SPIRAL_CLIENT_ID") and env.get("SPIRAL_PRIVATE_KEY"):
        return
    if given := env.get("SPIRAL_AUTH_FILE"):
        path = Path(given)
    elif sys.platform == "darwin":
        path = Path.home() / "Library/Application Support/pyspiral/auth.json"
    else:
        config = env.get("XDG_CONFIG_HOME") or Path.home() / ".config"
        path = Path(config) / "pyspiral/auth.json"
    try:
        saved = path.stat().st_size > 0
    except OSError:
        saved = False
    if not saved:
        raise MigrationError(
            f"no SpiralDB sign-in was found ({path}). Run 'spiral login' first"
        )


def principal_of(token: str) -> str:
    """Return the principal a token names; the token itself is never shown."""
    body = token.split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
    return str(claims.get("sub", "unknown"))


# ------------------------------------------------------------------------------ dry run


def nullable_paths(schema: Any) -> list[list[str]]:
    """Return the nullable paths of a schema, as the schema table holds them."""
    import pyarrow as pa

    out: list[list[str]] = []

    def walk(field: Any, path: list[str]) -> None:
        if field.nullable:
            out.append(path)
        t = field.type
        if pa.types.is_struct(t):
            for i in range(t.num_fields):
                walk(t.field(i), [*path, t.field(i).name])
        elif (
            pa.types.is_list(t)
            or pa.types.is_large_list(t)
            or pa.types.is_fixed_size_list(t)
        ):
            walk(t.value_field, [*path, "[]"])

    for field in schema:
        walk(field, [field.name])
    return sorted(out)


def unrepresentable(schema: Any) -> str | None:
    """Return why a schema cannot be recorded, or ``None``."""
    import pyarrow as pa

    def walk(t: Any, where: str) -> str | None:
        if isinstance(t, pa.BaseExtensionType):
            return f"{where} is an extension type"
        if pa.types.is_string_view(t) or pa.types.is_binary_view(t):
            return f"{where} is {t}, which spiral-sdk does not read"
        for i in range(t.num_fields):
            if (why := walk(t.field(i).type, f"{where}.{t.field(i).name}")) is not None:
                return why
        return None

    for field in schema:
        if (why := walk(field.type, field.name)) is not None:
            return why
    return None


def source_values(project: str, dataset: str, name: str) -> dict[str, Any]:
    """Return the identity of the default source on a table, or why there is none."""
    from orcapod.sources import SpiralDBTableSource

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            source = SpiralDBTableSource(project, name, dataset=dataset)
    except (ValueError, TypeError) as exc:
        return {"refused": type(exc).__name__}
    with source:
        tags, data = source.output_schema(all_info=True)
        return {
            "source_id": source.source_id,
            "content_hash": source.content_hash().to_string(),
            "pipeline_hash": source.pipeline_hash().to_string(),
            "tag_schema": {k: str(v) for k, v in tags.items()},
            "data_schema": {k: str(v) for k, v in data.items()},
        }


def first_rows_digest(connector: Any, name: str) -> str | None:
    """Return Orcapod's hash of a table's first rows (``FIRST_ROWS``) as it reads them;
    ``None`` for a table it cannot hash (a map column).

    Args:
        connector: The connector of the table's dataset.
        name: The table.
    """
    import pyarrow as pa

    from orcapod.contexts import get_default_context

    handle = connector.lookup_table(name)
    batches = list(handle.scan_batches(limit=FIRST_ROWS))
    rows = pa.Table.from_batches(batches, schema=handle.arrow_schema())
    if any(pa.types.is_map(field.type) for field in rows.schema):
        return None
    return get_default_context().arrow_hasher.hash_table(rows).to_string()


def describe_table(
    connector: Any, project: str, dataset: str, name: str
) -> dict[str, Any]:
    """Return one table's part of the plan (the pyspiral Orcapod)."""
    handle = connector.lookup_table(name)
    schema = handle.arrow_schema()
    if (why := unrepresentable(schema)) is not None:
        raise MigrationError(f"{project}/{dataset}/{name} cannot be recorded: {why}")
    metadata = handle._table.get_metadata()
    recorded = any(k.startswith("__orcapod_nullability__") for k in metadata)
    return {
        "row": {
            "table_id": handle.table_id,
            "table_name": name,
            "format": ROW_FORMAT,
            "format_version": ROW_FORMAT_VERSION,
            "nullability": "record"
            if recorded or "__arrow_metadata__" in metadata
            else "service",
            "captured_version": handle.version(),
            "schema": base64.b64encode(schema.serialize().to_pybytes()).decode(),
            "key_columns": handle.key_columns,
            "nullable_columns": nullable_paths(schema),
        },
        "source": source_values(project, dataset, name),
        "first_rows": first_rows_digest(connector, name),
    }


def dry_run(args: argparse.Namespace) -> None:
    """Write the plan and print what each dataset's schema table would hold."""
    from orcapod.databases.spiraldb_connector import SpiralDBConnector

    sp = spiral_module()

    if hasattr(sp, "keyed_tables") or not hasattr(sp, "Spiral"):
        raise MigrationError(
            "this environment has spiral-sdk; the dry run needs the last Orcapod on "
            "pyspiral (see RELEASING.md)"
        )
    client = sp.Spiral()
    token = client.authn.token()
    print(
        "principal:", "none" if token is None else principal_of(token.expose_secret())
    )
    project = client.project(args.project)
    tables: dict[str, list[str]] = {}
    schema_tables: dict[str, str] = {}
    for found in project.list_tables():
        if found.table == SCHEMA_TABLE:
            schema_tables[found.dataset] = found.id
        else:
            tables.setdefault(found.dataset, []).append(found.table)
    if missing := [d for d in args.dataset if d not in tables]:
        raise MigrationError(
            f"no table in dataset(s) {missing}; the project has {sorted(tables)}"
        )
    plan: dict[str, Any] = {
        "format": PLAN_FORMAT,
        "version": PLAN_VERSION,
        "project": args.project,
        "datasets": {},
    }
    for dataset in sorted(set(args.dataset)):
        connector = SpiralDBConnector(args.project, dataset)
        described = []
        for name in sorted(tables[dataset]):
            described.append(describe_table(connector, args.project, dataset, name))
            print(".", end="", flush=True)
        print()
        present = schema_tables.get(dataset)
        plan["datasets"][dataset] = {"schema_table": present, "tables": described}
        print(f"{dataset}: {len(described)} table rows and 1 completion marker")
        print(
            f"  {SCHEMA_TABLE}: "
            + ("absent, as expected" if present is None else f"EXISTS ({present})")
        )
        for table in described:
            row, source = table["row"], table["source"]
            note = f", no source: {source['refused']}" if "refused" in source else ""
            print(f"  {row['table_name']} (nullability: {row['nullability']}{note})")
    Path(args.plan).write_text(canonical(plan))
    print("wrote the plan to", args.plan)
    print("digest:", plan_digest(plan))


# --------------------------------------------------------------------- write and verify


def read_plan(path: str) -> dict[str, Any]:
    """Return the plan in a file."""
    try:
        plan = json.loads(Path(path).read_text())
    except (OSError, ValueError) as exc:
        raise MigrationError(f"the plan {path} cannot be read: {exc}") from exc
    if plan.get("format") != PLAN_FORMAT or plan.get("version") != PLAN_VERSION:
        raise MigrationError(f"{path} is not a plan this tool reads")
    return plan


def require_scope(plan: dict[str, Any], args: argparse.Namespace) -> None:
    """Refuse a plan for another project or other datasets than the arguments name."""
    if plan["project"] != args.project or set(plan["datasets"]) != set(args.dataset):
        raise MigrationError(
            f"the plan is for project {plan['project']!r}, datasets "
            f"{sorted(plan['datasets'])}; the arguments name {args.project!r}, "
            f"{sorted(set(args.dataset))}"
        )


def sdk_client() -> Any:
    """Return a spiral-sdk client and print its principal."""
    spiral = spiral_module()

    if not hasattr(spiral, "Client"):
        raise MigrationError(
            "this environment has pyspiral; this step needs Orcapod on spiral-sdk (see "
            "RELEASING.md)"
        )
    client = spiral.Client()
    print("principal:", principal_of(client._native.access_token()))
    return client


def planned_rows(tables: list[dict[str, Any]]) -> list[Any]:
    """Return the plan's rows, checked to say what the plan says."""
    from orcapod.databases.spiraldb_schema_table import SchemaRow

    rows = []
    for table in tables:
        planned = table["row"]
        row = SchemaRow(
            planned["table_id"],
            planned["table_name"],
            tuple(planned["key_columns"]),
            base64.b64decode(planned["schema"]),
            planned["nullability"],
            planned["captured_version"],
        )
        # Read as the stored row will be, and equal to the plan, byte for byte.
        if SchemaRow.from_stored(row.stored()) != row or row.content() != planned:
            raise MigrationError(
                f"the plan's row of {planned['table_name']} is invalid"
            )
        rows.append(row)
    return rows


def require_unchanged(
    client: Any,
    project: str,
    dataset: str,
    tables: list[dict[str, Any]],
    rows: list[Any],
) -> None:
    """Refuse unless every table of the dataset is as the plan found it."""
    from orcapod.databases.spiraldb_connector import _list_keyed_tables
    from orcapod.databases.spiraldb_schema_table import (
        compared_columns,
        describe_difference,
    )

    # Not the connector's listing: it refuses a dataset whose migration was interrupted.
    listed = _list_keyed_tables(client, project, dataset)
    names = sorted(name for name in listed.values() if name != SCHEMA_TABLE)
    planned = sorted(row.table_name for row in rows)
    if names != planned:
        raise MigrationError(
            f"{dataset}: tables were added or removed since the dry run: "
            f"{sorted(set(names) ^ set(planned))}"
        )
    for table, row in zip(tables, rows):
        handle = client.keyed_table(project, dataset, row.table_name)
        where = f"{dataset}/{row.table_name}"
        if handle.resource_id != row.table_id:
            raise MigrationError(f"{where} is another table than in the dry run")
        if int(handle.version_id) != table["row"]["captured_version"]:
            raise MigrationError(
                f"{where} was written since the dry run (version {handle.version_id}, "
                f"planned {row.captured_version})"
            )
        if list(handle.key_columns) != list(row.key_columns):
            raise MigrationError(
                f"{where} has key columns {list(handle.key_columns)}, planned "
                f"{list(row.key_columns)}"
            )
        found = handle.schema()
        if compared_columns(found) != compared_columns(row.schema):
            raise MigrationError(
                f"{where} differs from the plan: {describe_difference(found, row.schema)}"
            )


def note_path(args: argparse.Namespace) -> Path:
    """Return where the schema tables this plan's write created are noted."""
    return Path(f"{args.plan}.created.json")


def read_note(args: argparse.Namespace) -> dict[str, str]:
    """Return the schema tables this plan's write created: dataset to table id."""
    try:
        note = json.loads(note_path(args).read_text())
    except OSError:
        return {}
    return note["created"] if note["digest"] == args.digest else {}


def write_note(args: argparse.Namespace, dataset: str, value: str) -> None:
    """Note what this plan's write creates in a dataset: ``CREATING``, then the table id,
    or ``REFUSED``.

    The note is replaced as a whole, so it is never half written.
    """
    created = {**read_note(args), dataset: value}
    new = Path(f"{note_path(args)}.new")
    new.write_text(canonical({"digest": args.digest, "created": created}))
    os.replace(new, note_path(args))


def require_own_schema_table(
    client: Any, args: argparse.Namespace, dataset: str, planned: dict[str, Any]
) -> Any | None:
    """Return the dataset's schema table when this plan's write created it, ``None`` when
    there is none yet; refuse any other."""
    from orcapod.databases.spiraldb_connector import _open_keyed_table

    if planned["schema_table"] is not None:
        raise MigrationError(
            f"{dataset}: {SCHEMA_TABLE} existed at the dry run ({planned['schema_table']})"
        )
    table = _open_keyed_table(client, args.project, dataset, SCHEMA_TABLE)
    noted = read_note(args).get(dataset)
    if (
        table is not None
        and noted == CREATING
        and list(table.key_columns) == ["table_id"]
        and int(table.version_id) == 0
    ):
        # Stopped around the creation. An empty table looks the same whoever made it, so
        # a person says that it is this plan's.
        if args.adopt != table.resource_id:
            raise MigrationError(
                f"{dataset}: an earlier run stopped while creating {SCHEMA_TABLE}, and "
                f"there is an empty one ({table.resource_id}). If that run made it, and "
                f"nobody else, run again with --adopt {table.resource_id}"
            )
        write_note(args, dataset, table.resource_id)
        return table
    if noted == REFUSED or (table is not None and noted != table.resource_id):
        found = "none now" if table is None else table.resource_id
        raise MigrationError(
            f"{dataset}: {SCHEMA_TABLE} ({found}) was not created by this "
            f"plan's write ({note_path(args)} does not name it)"
        )
    return table


def open_rows(client: Any, project: str, dataset: str) -> tuple[dict[str, Any], Any]:
    """Return the schema table's rows by table id and its marker, as stored now."""
    from orcapod.databases.spiraldb_connector import _open_keyed_table
    from orcapod.databases.spiraldb_schema_table import read_rows, require_schema_table

    table = _open_keyed_table(client, project, dataset, SCHEMA_TABLE)
    if table is None or not require_schema_table(table):
        return {}, None
    return read_rows(table.scan().to_table().to_pylist())


def write(args: argparse.Namespace) -> None:
    """Write the planned rows, one by one, and the completion marker last."""
    import pyarrow as pa

    from orcapod.databases import spiraldb_schema_table as st

    plan = read_plan(args.plan)
    require_scope(plan, args)
    if plan_digest(plan) != args.digest:
        raise MigrationError(
            f"the plan's digest is {plan_digest(plan)}, not {args.digest}"
        )
    client = sdk_client()
    project = plan["project"]
    prepared = {}
    # Everything is checked before anything is written.
    for dataset, planned in plan["datasets"].items():
        tables = planned["tables"]
        rows = planned_rows(tables)
        require_unchanged(client, project, dataset, tables, rows)
        _ = require_own_schema_table(client, args, dataset, planned)
        stored, _ = open_rows(client, project, dataset)
        wanted = {row.table_id: row for row in rows}
        for table_id, row in stored.items():
            if table_id not in wanted:
                raise MigrationError(
                    f"{dataset}: the schema table has a row of table {table_id!r} "
                    f"({row.table_name!r}), which is not in the plan"
                )
            if row.content() != wanted[table_id].content():
                raise MigrationError(
                    f"{dataset}: the schema table's row of {row.table_name!r} differs "
                    "from the plan's"
                )
        prepared[dataset] = rows
    by = f"migrate_spiral_schemas, plan {args.digest[:12]}"

    def put(dataset: str, values: dict[str, Any]) -> None:
        handle = require_own_schema_table(
            client, args, dataset, plan["datasets"][dataset]
        )
        if handle is None:
            # Created once, exclusively; the note is there before and after.
            write_note(args, dataset, CREATING)
            try:
                handle = client.project(project).create_keyed_table(
                    dataset, SCHEMA_TABLE, st.KEY_SCHEMA, exist_ok=False
                )
            except Exception as exc:
                if "status 409" not in str(exc):
                    raise
                # Another's: never to be adopted, by this run or a later one.
                write_note(args, dataset, REFUSED)
                raise MigrationError(
                    f"{dataset}: another program created {SCHEMA_TABLE} just now"
                ) from exc
            write_note(args, dataset, handle.resource_id)
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        values = {**values, "written_at": now, "written_by": by}
        handle.append(pa.Table.from_pylist([values], schema=st.ROW_SCHEMA))

    written = 0
    for dataset, rows in prepared.items():
        present, marker = open_rows(client, project, dataset)
        for row in rows:
            if row.table_id in present:
                continue
            put(dataset, row.stored())
            written += 1
            present, marker = open_rows(client, project, dataset)
            if (
                row.table_id not in present
                or present[row.table_id].content() != row.content()
            ):
                raise MigrationError(
                    f"{dataset}: the row of {row.table_name!r} does not read back as "
                    "written"
                )
            print(f"{dataset}: wrote {row.table_name}", flush=True)
            if args.stop_after is not None and written >= args.stop_after:
                raise MigrationError("stopped as asked (--stop-after)")
        # Again just before the marker, and after it.
        require_unchanged(
            client, project, dataset, plan["datasets"][dataset]["tables"], rows
        )
        final = st.marker_values(st.Scope(project, dataset, args.digest), rows)
        if marker is None or any(marker[k] != v for k, v in final.items()):
            put(dataset, final)
        try:
            require_unchanged(
                client, project, dataset, plan["datasets"][dataset]["tables"], rows
            )
            present, marker = open_rows(client, project, dataset)
            st.require_complete(project, dataset, present, marker)
            if {i: r.content() for i, r in present.items()} != {
                r.table_id: r.content() for r in rows
            }:
                raise MigrationError(
                    f"{dataset}: the schema table is not what was planned"
                )
        except BaseException as exc:
            # Whatever happened, no valid marker is left: reads of the dataset are
            # refused until a write step ends well.
            put(dataset, {**final, "rows_digest": "withdrawn: not checked to the end"})
            raise MigrationError(
                f"{exc or type(exc).__name__}. The completion marker was withdrawn: run "
                "write again, or the dry run if the dataset changed"
            ) from exc
        print(f"{dataset}: complete, {len(rows)} tables")
    print(f"wrote {written} rows; now run verify")


def verify(args: argparse.Namespace) -> None:
    """Check with spiral-sdk alone that every source is what the plan says."""
    from orcapod.databases.spiraldb_connector import SpiralDBConnector

    plan = read_plan(args.plan)
    require_scope(plan, args)
    from orcapod.databases import spiraldb_schema_table as st

    client = sdk_client()
    project = plan["project"]
    wrong: list[str] = []
    checked = refused = 0
    for dataset, planned in plan["datasets"].items():
        tables = planned["tables"]
        connector = SpiralDBConnector(project, dataset)
        rows = planned_rows(tables)
        require_unchanged(client, project, dataset, tables, rows)
        present, marker = open_rows(client, project, dataset)
        st.require_complete(project, dataset, present, marker)
        if (
            present != {row.table_id: row for row in rows}
            or marker is None
            or marker["plan_digest"] != plan_digest(plan)
        ):
            raise MigrationError(f"{dataset}: the schema table is not the plan's")
        for row in rows:
            schema = connector.get_arrow_schema(row.table_name)
            if not schema.equals(row.schema, check_metadata=True):
                wrong.append(f"{dataset}/{row.table_name}: schema")
        for table in tables:
            name, expected = table["row"]["table_name"], table["source"]
            found = source_values(project, dataset, name)
            for key in sorted(set(expected) | set(found)):
                if expected.get(key) != found.get(key):
                    wrong.append(
                        f"{dataset}/{name}: {key}: planned {expected.get(key)!r}, found "
                        f"{found.get(key)!r}"
                    )
            rows_found = first_rows_digest(connector, name)
            if rows_found != table["first_rows"]:
                wrong.append(f"{dataset}/{name}: its first {FIRST_ROWS} rows")
            checked += 1
            refused += "refused" in expected
        print(f"{dataset}: checked {len(tables)} tables", flush=True)
    if wrong:
        raise MigrationError("not equal:\n  " + "\n  ".join(wrong))
    print(
        f"verified: {checked} tables; schemas equal, and the source ids, content hashes and "
        f"pipeline hashes of {checked - refused} sources ({refused} tables have none, "
        "before and after). Table ids, versions and the completion marker are the plan's, "
        f"and so are the first {FIRST_ROWS} rows of each table. Rows beyond them were not "
        "read."
    )


def main() -> int:
    """Run one step."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n")[0])
    steps = parser.add_subparsers(dest="step", required=True)
    for name, run in (("dry-run", dry_run), ("write", write), ("verify", verify)):
        step = steps.add_parser(name, help=run.__doc__)
        step.add_argument("--project", required=True)
        step.add_argument("--dataset", action="append", required=True)
        step.add_argument("--plan", default="spiral-migration-plan.json")
        step.set_defaults(run=run)
        if name == "write":
            step.add_argument("--digest", required=True)
            step.add_argument(
                "--adopt",
                help="the id of an empty schema table that an earlier, stopped run of "
                "this plan created",
            )
            step.add_argument("--stop-after", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        require_credentials()
        args.run(args)
    except (MigrationError, ValueError, ImportError) as exc:
        # ValueError: what Orcapod refuses, a damaged schema table among it.
        print("STOPPED:", exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
