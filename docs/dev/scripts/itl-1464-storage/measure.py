"""Measure Delta files, Parquet encodings and provenance layouts without emitting values."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path
from statistics import median
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import Literal, TypedDict, cast
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
from deltalake import DeltaTable

TAG_NAMES = ("group_key", "order_key")
DEFAULT_GROUP = 131_072
CHAIN_LENGTH = 5
Codec = Literal["snappy", "zstd"]


class ParquetSizes(TypedDict):
    """Size of the physical files and their column chunks."""

    bytes: int
    compressed: dict[str, int]
    rows: int
    row_groups: int
    files: int
    codecs: list[str]


class EncodingOptions(TypedDict):
    """Settings passed to both Parquet write entry points."""

    use_dictionary: bool
    column_encoding: dict[str, str] | None


def name_kind(name: str) -> str:
    """Identify the public column family without exposing a dynamic namespaced suffix."""
    return name.split("::", 1)[0]


def delta_files(root: Path) -> list[Path]:
    """List active Parquet files in addition order according to Delta logs."""
    active = {
        Path(unquote(urlparse(uri).path)) for uri in DeltaTable(str(root)).file_uris()
    }
    ordered = []
    for log in sorted((root / "_delta_log").glob("*.json")):
        for line in log.read_text().splitlines():
            added = json.loads(line).get("add")
            if added is not None:
                path = root / unquote(added["path"])
                if path in active:
                    ordered.append(path)
    if set(ordered) != active:
        raise ValueError("Active files are missing from the commit logs")
    return ordered


def parquet_sizes(files: list[Path]) -> ParquetSizes:
    """Total compressed leaf bytes by top-level ordinal, plus file overhead."""
    compressed: Counter[str] = Counter()
    rows = 0
    row_groups = 0
    total = 0
    codecs = set()
    for path in files:
        file = pq.ParquetFile(path)
        meta = file.metadata
        total += path.stat().st_size
        rows += meta.num_rows
        row_groups += meta.num_row_groups
        columns = {
            field.name: str(index) for index, field in enumerate(file.schema_arrow)
        }
        for group_index in range(meta.num_row_groups):
            group = meta.row_group(group_index)
            for col_index in range(group.num_columns):
                col = group.column(col_index)
                field = col.path_in_schema.split(".", 1)[0]
                compressed[columns[field]] += col.total_compressed_size
                codecs.add(col.compression)
    return {
        "bytes": total,
        "compressed": dict(compressed),
        "rows": rows,
        "row_groups": row_groups,
        "files": len(files),
        "codecs": sorted(codecs),
    }


def arrow_bytes(table: pa.Table) -> dict[str, int]:
    """Return each Arrow column's logical buffer bytes, including offsets and validity."""
    return {str(index): col.nbytes for index, col in enumerate(table.columns)}


def prefix_leaves(table: pa.Table) -> list[str]:
    """List BYTE_ARRAY leaves whose names identify tokens, digests or record IDs."""
    leaves = []
    for field in table.schema:
        kind = name_kind(field.name)
        if not (
            kind.startswith("_source_")
            or kind.startswith("_tag_")
            or "hash" in kind
            or kind.endswith("_id")
        ):
            continue
        dtype = field.type
        path = field.name
        if pa.types.is_large_list(dtype) or pa.types.is_list(dtype):
            path += ".list.element"
            dtype = dtype.value_type
        if (
            pa.types.is_string(dtype)
            or pa.types.is_large_string(dtype)
            or pa.types.is_binary(dtype)
            or pa.types.is_large_binary(dtype)
        ):
            leaves.append(path)
    return leaves


def encoding_options(table: pa.Table, dictionary: str) -> EncodingOptions:
    """Choose dictionary and prefix encoding for the selected columns."""
    dictionaries: bool | list[str] = dictionary == "all"
    encoding = None
    if dictionary == "prefix":
        prefixes = prefix_leaves(table)
        # The selective dictionary applies to scalar fields outside the prefix test.
        # Remaining nested fields use plain encoding in this variant.
        dictionaries = [
            field.name
            for field in table.schema
            if not any(
                leaf.startswith(field.name + ".") or leaf == field.name
                for leaf in prefixes
            )
            and not pa.types.is_nested(field.type)
        ]
        encoding = dict.fromkeys(prefixes, "DELTA_BYTE_ARRAY")
    # PyArrow accepts a list of columns, omitted in the installed stubs.
    return {"use_dictionary": cast("bool", dictionaries), "column_encoding": encoding}


def parquet_round(
    table: pa.Table,
    path: Path,
    codec: Codec,
    level: int | None,
    dictionary: str,
    row_group: int,
) -> ParquetSizes:
    """Write a Parquet candidate and measure its physical column-chunk sizes."""
    pq.write_table(
        table,
        path,
        compression=codec,
        compression_level=level,
        row_group_size=row_group,
        **encoding_options(table, dictionary),
    )
    return parquet_sizes([path])


def parquet_grid(table: pa.Table, path: Path) -> dict[str, ParquetSizes]:
    """Sweep codecs and dictionaries, then isolate row-group size at Zstd 3."""
    codecs: list[tuple[Codec, int | None]] = [
        ("snappy", None),
        ("zstd", 1),
        ("zstd", 3),
        ("zstd", 9),
    ]
    settings: list[tuple[Codec, int | None, str, int]] = [
        (codec, level, dictionary, DEFAULT_GROUP)
        for codec, level in codecs
        for dictionary in ("all", "off", "prefix")
    ]
    for dictionary in ("all", "prefix"):
        for row_group in (16_384, 1_048_576):
            settings.append(("zstd", 3, dictionary, row_group))
    return {
        f"{codec}-{level or 0}-{dictionary}-{row_group}": parquet_round(
            table, path, codec, level, dictionary, row_group
        )
        for codec, level, dictionary, row_group in settings
    }


def ipc_grid(table: pa.Table, path: Path) -> dict[str, dict[str, float | int]]:
    """Measure writes and warm reads into Arrow, including full decompression."""
    stats = {}
    for codec in (None, "lz4", "zstd"):
        writes, reads = [], []
        for _ in range(3):
            started = perf_counter()
            with pa.OSFile(str(path), "wb") as output:
                with ipc.new_file(
                    output, table.schema, options=ipc.IpcWriteOptions(compression=codec)
                ) as writer:
                    writer.write_table(table, max_chunksize=DEFAULT_GROUP)
            writes.append(perf_counter() - started)
            started = perf_counter()
            with pa.OSFile(str(path), "rb") as source:
                loaded = ipc.open_file(source).read_all()
            reads.append(perf_counter() - started)
            if loaded.num_rows != table.num_rows:
                raise ValueError("IPC row count changed")
        stats[codec or "plain"] = {
            "bytes": path.stat().st_size,
            "write_s": median(writes),
            "read_s": median(reads),
        }
    return stats


def list_tokens(table: pa.Table, tokens: list[str]) -> list[str]:
    """Select the list-valued provenance columns."""
    return [
        name
        for name in tokens
        if pa.types.is_list(table[name].type)
        or pa.types.is_large_list(table[name].type)
    ]


def pack_lookup(
    table: pa.Table, tokens: list[str], *, mode: str
) -> tuple[pa.Table, pa.Table]:
    """Replace a tuple of provenance values with one verified lookup foreign key."""
    source = table.select(tokens)
    if mode == "integer":
        # A different tuple per row is the normal case for per-row provenance.
        # Deduplicate identical tuples too, for equality with the content key case.
        values = zip(*(col.to_pylist() for col in source.columns), strict=True)
        identifiers: dict[str, int] = {}
        rows = [
            identifiers.setdefault(json.dumps(row), len(identifiers)) for row in values
        ]
        key = pa.array(rows, pa.int64())
    else:
        values = zip(*(col.to_pylist() for col in source.columns), strict=True)
        rows = [
            hashlib.blake2b(json.dumps(row).encode(), digest_size=16).digest()
            for row in values
        ]
        key = pa.array(rows, pa.binary(16))
    first: dict[int | bytes, int] = {}
    for index, identifier in enumerate(rows):
        first.setdefault(identifier, index)
    lookup = source.take(list(first.values())).append_column(
        "provenance_key", pc.take(key, list(first.values()))
    )
    data = table.drop(tokens).append_column("provenance_key", key)
    return data, lookup


def derive_tokens(
    table: pa.Table, tokens: list[str]
) -> tuple[pa.Table, pa.Table, dict[str, tuple[str, str]]]:
    """Store fixed source/column templates where the existing row ID supplies the middle."""
    templates: dict[str, tuple[str, str]] = {}
    for name in tokens:
        col = table[name]
        if name in list_tokens(table, tokens) or col.null_count or not len(col):
            continue
        prefix, _, suffix = str(col[0].as_py()).rsplit("::", 2)
        templates[name] = prefix, suffix
    lookup = pa.table(
        {
            "fixed_source": pa.array(
                [prefix for prefix, _ in templates.values()], pa.string()
            ),
            "column_suffix": pa.array(
                [suffix for _, suffix in templates.values()], pa.string()
            ),
        }
    )
    return table.drop(list(templates)), lookup, templates


def derived_rebuild(
    data: pa.Table, lookup: pa.Table, names: list[str], kind: str
) -> pa.Table:
    """Rebuild token text from an existing scalar record ID and the template sidecar."""
    if not names:
        return data
    if kind == "pdb_v1":
        record_name = next(
            col for col in data.column_names if col.startswith("_tag_record_id::")
        )
        record_ids = data[record_name].cast(pa.string())
    else:
        record_ids = pa.array(
            [cast("bytes", value).hex() for value in data["__record_id"].to_pylist()]
        )
    for index, name in enumerate(names):
        tokens = pc.call_function(
            "binary_join_element_wise",
            [
                lookup["fixed_source"][index],
                record_ids,
                lookup["column_suffix"][index],
                pa.scalar("::"),
            ],
        )
        data = data.append_column(
            name,
            cast("pa.ChunkedArray[pa.Scalar[pa.DataType]]", tokens).cast(
                pa.large_string()
            ),
        )
    return data


def layouts(
    table: pa.Table, scratch: Path, kind: str
) -> dict[str, dict[str, float | int]]:
    """Measure compressed sidecar layouts and verified warm rebuild costs."""
    tokens = [name for name in table.column_names if name.startswith("_source_")]
    lists = list_tokens(table, tokens)
    data_file = scratch / "data.parquet"
    lookup_file = scratch / "lookup.parquet"
    report = {}
    for mode in ("inline", "none", "derived", "integer", "hash", "list_hash"):
        lookup = None
        templates: dict[str, tuple[str, str]] = {}
        active_tokens = lists if mode == "list_hash" else tokens
        if mode == "none":
            data = table.drop(tokens)
        elif mode == "derived":
            data, lookup, templates = derive_tokens(table, tokens)
        elif mode in ("integer", "hash", "list_hash") and active_tokens:
            data, lookup = pack_lookup(table, active_tokens, mode=mode)
        else:
            data = table
        data_bytes = int(
            parquet_round(data, data_file, "zstd", 3, "prefix", DEFAULT_GROUP)["bytes"]
        )
        lookup_bytes = 0
        if lookup is not None and lookup.num_rows:
            lookup_bytes = int(
                parquet_round(lookup, lookup_file, "zstd", 3, "prefix", DEFAULT_GROUP)[
                    "bytes"
                ]
            )
        total_times, rebuild_times = [], []
        for _ in range(3):
            started = perf_counter()
            rebuilt = pq.read_table(data_file)
            sidecar = pq.read_table(lookup_file) if lookup_bytes else None
            read_done = perf_counter()
            if sidecar is not None and mode == "derived":
                rebuilt = derived_rebuild(rebuilt, sidecar, list(templates), kind)
            elif sidecar is not None:
                indices = pc.index_in(
                    rebuilt["provenance_key"],
                    value_set=sidecar["provenance_key"].combine_chunks(),
                )
                recovered = sidecar.select(active_tokens).take(indices)
                for name in active_tokens:
                    rebuilt = rebuilt.append_column(name, recovered[name])
            finished = perf_counter()
            if mode != "none" and not rebuilt.select(table.column_names).equals(table):
                raise ValueError(f"Token round trip failed for {kind} / {mode}")
            total_times.append(finished - started)
            rebuild_times.append(finished - read_done)
        report[mode] = {
            "data_bytes": data_bytes,
            "lookup_bytes": lookup_bytes,
            "chain_bytes": CHAIN_LENGTH * data_bytes + lookup_bytes,
            "read_s": median(total_times),
            "rebuild_s": median(rebuild_times),
        }
    return report


def measure(
    root: Path, scratch: Path, *, sizes_only: bool = False
) -> dict[str, object]:
    """Read a real Delta run and measure both physical tables."""
    output = {}
    tables = {}
    for kind in ("pdb_v1", "rdb_v1"):
        (directory,) = root.rglob(kind)
        files = delta_files(directory)
        if sizes_only:
            arrow: Counter[str] = Counter()
            for path in files:
                for batch in pq.ParquetFile(path).iter_batches():
                    arrow.update(arrow_bytes(pa.Table.from_batches([batch])))
            columns = pq.ParquetFile(files[0]).schema_arrow.names
        else:
            table = pq.read_table(files)
            tables[kind] = table
            arrow = Counter(arrow_bytes(table))
            columns = table.column_names
        output[kind] = {
            "actual": parquet_sizes(files),
            "logs": sum(
                p.stat().st_size
                for p in (directory / "_delta_log").iterdir()
                if p.is_file()
            ),
            "arrow": dict(arrow),
            "columns": [name_kind(name) for name in columns],
        }
    if sizes_only:
        return output
    pipeline = tables["pdb_v1"]
    tags = [name for name in TAG_NAMES if name in pipeline.column_names]
    sorted_pipeline = pipeline.sort_by([(name, "ascending") for name in tags])
    positions = pc.index_in(
        sorted_pipeline["__data_id"],
        value_set=tables["rdb_v1"]["__record_id"].combine_chunks(),
    )
    # A result can be reused by many pipeline rows. Sort by its first tagged occurrence.
    result_order = pc.unique(pc.drop_null(positions))
    if len(result_order) != tables["rdb_v1"].num_rows:
        raise ValueError("Unexpected result records orphaned from the pipeline table")
    sorted_tables = {
        "pdb_v1": sorted_pipeline,
        "rdb_v1": tables["rdb_v1"].take(result_order),
    }
    file = scratch / "bench.parquet"
    for kind, computed in tables.items():
        output[kind].update(
            computed=parquet_grid(computed, file),
            sorted=parquet_grid(sorted_tables[kind], file),
            layouts=layouts(computed, scratch, kind),
            ipc=ipc_grid(computed, scratch / "bench.arrow"),
        )
    return output


def main() -> None:
    """Write JSON measurements only, using scratch space beside the input by default."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--sizes-only", action="store_true")
    args = parser.parse_args()
    with TemporaryDirectory(
        prefix=".storage-measure-", dir=args.root.parent
    ) as directory:
        print(
            json.dumps(measure(args.root, Path(directory), sizes_only=args.sizes_only))
        )


if __name__ == "__main__":
    main()
