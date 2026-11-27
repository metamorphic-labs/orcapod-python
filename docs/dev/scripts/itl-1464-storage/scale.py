"""Write bounded batches with the exact persisted node schema and seeded synthetic rows."""

from __future__ import annotations

import argparse
import json
from binascii import hexlify
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import numpy as np
from numpy.typing import NDArray
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from deltalake import write_deltalake

from orcapod.databases.delta_statistics import statistics_configuration

from measure import delta_files, encoding_options, name_kind, parquet_sizes  # pyright: ignore[reportImplicitRelativeImport]  # Script entry point.
from workloads import project, read_profile, synthetic  # pyright: ignore[reportImplicitRelativeImport]  # Script entry point.

BATCH = 65_536


def uniform(keys: NDArray[Any], salt: int) -> NDArray[Any]:
    """Use SplitMix64 to generate reproducible high-entropy limbs from each key."""
    with np.errstate(over="ignore"):
        stream = np.uint64((0xD2B74407B1CE6E93 * salt) % (1 << 64))
        x = keys.astype(np.uint64) * np.uint64(0x9E3779B97F4A7C15) + stream
        x = (x ^ (x >> 30)) * np.uint64(0xBF58476D1CE4E5B9)
        x = (x ^ (x >> 27)) * np.uint64(0x94D049BB133111EB)
        return x ^ (x >> 31)


def text_or_binary(
    buffer: bytes | NDArray[Any], width: int, dtype: pa.DataType
) -> pa.Array[pa.Scalar[pa.DataType]]:
    """Make a variable-length Arrow column from uniformly sized data."""
    size = len(buffer) // width
    offsets = np.arange(size + 1, dtype=np.int64) * width
    # The validity buffer is absent. The PyArrow stubs omit the None case.
    buffers = cast(
        "list[pa.Buffer]",
        [
            None,
            pa.py_buffer(offsets.tobytes()),
            pa.py_buffer(buffer if isinstance(buffer, bytes) else buffer.tobytes()),
        ],
    )
    return pa.Array.from_buffers(dtype, size, buffers)


def digest(
    keys: NDArray[Any], prefix: bytes, salt: int
) -> pa.Array[pa.Scalar[pa.DataType]]:
    """Generate each prefixed 32-byte digest at the template's logical width."""
    size = len(keys)
    raw = np.empty((size, len(prefix) + 32), dtype=np.uint8)
    raw[:, : len(prefix)] = np.frombuffer(prefix, dtype=np.uint8)
    for limb in range(4):
        start = len(prefix) + 8 * limb
        raw[:, start : start + 8] = (
            uniform(keys, salt + limb).view(np.uint8).reshape(-1, 8)
        )
    return text_or_binary(raw.ravel(), raw.shape[1], pa.large_binary())


def uuid_bytes(keys: NDArray[Any]) -> NDArray[Any]:
    """Generate UUIDv7-shaped IDs with a monotone clock and a random suffix."""
    result = np.empty((len(keys), 16), dtype=np.uint8)
    clock = (np.uint64(1_700_000_000_000) + keys.astype(np.uint64)).astype(">u8")
    result[:, :6] = clock.view(np.uint8).reshape(-1, 8)[:, 2:]
    result[:, 6:8] = uniform(keys, 4).view(np.uint8).reshape(-1, 8)[:, :2]
    result[:, 8:] = uniform(keys, 5).view(np.uint8).reshape(-1, 8)
    result[:, 6] = (result[:, 6] & 0x0F) | 0x70
    result[:, 8] = (result[:, 8] & 0x3F) | 0x80
    return result


def join_token(
    prefix: str, ids: pa.Array[pa.Scalar[pa.DataType]], suffix: str
) -> pa.Array[pa.Scalar[pa.DataType]]:
    """Use Arrow to combine a source prefix, row ID and suffix."""
    values = [
        pa.scalar(prefix, pa.large_string()),
        ids,
        pa.scalar(suffix, pa.large_string()),
        pa.scalar("::", pa.large_string()),
    ]
    return cast(
        "pa.Array[pa.Scalar[pa.DataType]]",
        pc.call_function("binary_join_element_wise", values),
    )


def value_column(values: NDArray[Any], name: str) -> NDArray[Any]:
    """Apply the same numeric transforms as the measured pods."""
    match name:
        case "v0":
            return values * 1.25
        case "v1":
            return np.square(values)
        case "v2":
            return np.sin(values)
        case "v3":
            return np.cos(values)
        case "v4":
            return np.arctan(values)
        case "v5":
            return np.sqrt(np.abs(values))
        case "v6":
            return values * 2
        case "v7":
            return values * 100
        case "v8":
            return values * -0.5
        case "v9":
            return values + 1
        case _:
            raise ValueError(name)


def build_batches(
    template: pa.Table, shape: pa.Table, kind: str
) -> Iterator[pa.RecordBatch]:
    """Substitute seeded values into the real result or pipeline Parquet schema."""
    values = shape["value"].combine_chunks().to_numpy(zero_copy_only=False)
    groups = shape["group_key"].combine_chunks().to_numpy(zero_copy_only=False)
    integer_values = np.rint(values * 1000).astype(np.int64)
    # Reused values share an actual result ID, as in the function-result store.
    _, first, inverse = np.unique(
        integer_values, return_index=True, return_inverse=True
    )
    result_ids = first[inverse]
    rows = np.sort(first) if kind == "rdb_v1" else np.arange(len(values))
    for start in range(0, len(rows), BATCH):
        row = rows[start : start + BATCH]
        data = integer_values[row]
        uuid_buffer = uuid_bytes(result_ids[row]).ravel()
        uuid = text_or_binary(uuid_buffer, 16, pa.large_binary())
        uuid_text = text_or_binary(
            hexlify(uuid_buffer.tobytes()), 32, pa.large_string()
        )
        row_tag = pa.array([f"row_{index}" for index in row], pa.large_string())
        fields = []
        for field in template.schema:
            name = name_kind(field.name)
            dtype = field.type
            old = template[field.name][0].as_py()
            if name == "__data_id" or (name == "__record_id" and kind == "rdb_v1"):
                col = uuid
            elif name in {
                "__record_id",
                "__pipeline_base_entry_id",
                "__input_data_hash",
                "__output_data_hash",
            }:
                entropy_keys = (
                    row if name in {"__record_id", "__pipeline_base_entry_id"} else data
                )
                salt = {
                    "__record_id": 40,
                    "__pipeline_base_entry_id": 50,
                    "__input_data_hash": 60,
                    "__output_data_hash": 70,
                }[name]
                col = digest(entropy_keys, old[:-32], salt)
            elif name == "_tag_record_id":
                col = row_tag
            elif name.startswith("_source_"):
                prefix, _, suffix = old.rsplit("::", 2)
                col = join_token(
                    prefix, uuid_text if kind == "rdb_v1" else row_tag, suffix
                )
            elif name == "group_key":
                col = pa.array(groups[row], dtype)
            elif name == "order_key":
                col = pa.array(values[row], dtype)
            elif name in {"__recorded_at", "__pod_ts"}:
                col = pa.array(1_700_000_000_000_000 + row * 1000, dtype)
            elif name.startswith("v"):
                col = pa.array(value_column(values[row], name), dtype)
            else:
                col = pa.repeat(template[field.name][0], len(row))
            fields.append(col)
        yield pa.RecordBatch.from_arrays(fields, schema=template.schema)


def main() -> None:
    """Seed and write separate large physical tables into a local output directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("template", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("rows", type=int)
    calibration = parser.add_mutually_exclusive_group(required=True)
    calibration.add_argument("--calibrate-export", type=Path)
    calibration.add_argument("--profile-json", type=Path)
    parser.add_argument("--rewrite", action="store_true")
    parser.add_argument("--sample-rows", type=int, default=0)
    args = parser.parse_args()
    profile = read_profile(args.calibrate_export, args.profile_json)
    shape = synthetic(args.rows, profile, 1464)
    print(
        json.dumps({"profile": profile, "synthetic_profile": project(shape)[1]}),
        flush=True,
    )
    for kind in ("pdb_v1", "rdb_v1"):
        (root,) = args.template.rglob(kind)
        template = pq.ParquetFile(delta_files(root)[0]).read_row_group(0).slice(0, 1)
        batches = build_batches(template, shape, kind)
        reader = pa.RecordBatchReader.from_batches(template.schema, batches)
        write_deltalake(
            str(args.destination / kind),
            reader,
            configuration=statistics_configuration(template.schema),
        )
        print(f"wrote {kind}", flush=True)
        if args.sample_rows:
            reader = pa.RecordBatchReader.from_batches(
                template.schema,
                build_batches(template, shape.slice(0, args.sample_rows), kind),
            )
            write_deltalake(
                str(
                    args.destination.with_name(args.destination.name + "-sample") / kind
                ),
                reader,
            )
        if args.rewrite:
            file = args.destination / f"{kind}-zstd.parquet"
            with pq.ParquetWriter(
                file,
                template.schema,
                compression="zstd",
                compression_level=3,
                **encoding_options(template, "prefix"),
            ) as writer:
                for batch in build_batches(template, shape, kind):
                    writer.write_batch(batch)
            print(json.dumps({kind: parquet_sizes([file])}), flush=True)


if __name__ == "__main__":
    main()
