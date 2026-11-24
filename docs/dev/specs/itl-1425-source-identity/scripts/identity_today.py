"""Measure what enters a source's identity today, per kind (ITL-1425, section 1).

Run from ``docs/dev/specs/itl-1425-source-identity``:
``uv run python -m scripts.identity_today``.
"""

from __future__ import annotations

import warnings

from ._fixtures import same_rows_sources, scratch

warnings.simplefilter("ignore")


def main() -> None:
    folder = scratch()
    sources = same_rows_sources(folder)

    print("== Given source_id='subjects', record_id_columns=['subject'] ==")
    first = None
    for kind, src in sources.items():
        tbl = src.as_table(columns={"source": True, "system_tags": True})
        sys_cols = [c for c in tbl.column_names if c.startswith("_tag_")]
        src_cols = [c for c in tbl.column_names if c.startswith("_source_")]
        tag_s, data_s = src.arrow_schemas
        rec = {
            "class": type(src).__name__,
            "SOURCE_KIND": src.SOURCE_KIND,
            "source_id": src.source_id,
            "content_hash": src.content_hash().to_string()[:40],
            "pipeline_hash": src.pipeline_hash().to_string()[:40],
            "arrow tag schema": str(tag_s).replace("\n", "; "),
            "arrow data schema": str(data_s).replace("\n", "; "),
            "identity_structure[0]": src.identity_structure()[0],
            "system-tag columns": sys_cols,
            "system-tag values row 0": [tbl.column(c)[0].as_py() for c in sys_cols],
            "source-info token (weight)": tbl.column("_source_weight").to_pylist()[0]
            if "_source_weight" in src_cols
            else src_cols,
        }
        print(f"\n-- {kind}")
        for k, v in rec.items():
            print(f"   {k:28s} {v}")
        if first is None:
            first = rec
    print("\n== Agreement with arrow_table ==")
    for kind, src in list(sources.items())[1:]:
        a, b = sources["arrow_table"], src
        ta = a.as_table(columns={"source": True, "system_tags": True})
        tb = b.as_table(columns={"source": True, "system_tags": True})
        print(
            f"   {kind:13s} content_hash equal: {a.content_hash() == b.content_hash()}; "
            f"pipeline_hash equal: {a.pipeline_hash() == b.pipeline_hash()}; "
            f"arrow schemas equal: {a.arrow_schemas == b.arrow_schemas}; "
            f"table with system tags and tokens equal: {ta.equals(tb)}"
        )

    print("\n== Same content hash once the class name is set aside ==")
    for kind, src in sources.items():
        rest = src.identity_structure()[1:]
        print(
            f"   {kind:13s} identity without class name equal to arrow_table's: "
            f"{rest == sources['arrow_table'].identity_structure()[1:]}"
        )

    print("\n== Record ids do not depend on the source id ==")
    from orcapod.sources import ArrowTableSource

    from ._fixtures import KEYED, POSITIONAL, arrow_table

    for label, kwargs in (("keyed", dict(KEYED)), ("positional", dict(POSITIONAL))):
        kwargs.pop("source_id", None)
        ids = {}
        for sid in ("subjects", "csv_v1:subjects", "something_else"):
            t = ArrowTableSource(arrow_table(), source_id=sid, **kwargs).as_table(  # pyright: ignore[reportArgumentType]  # kwargs is one of the two TypedDicts
                columns={"system_tags": True, "source": True}
            )
            rid = next(c for c in t.column_names if c.startswith("_tag_record_id"))
            ids[sid] = (
                t.column(rid).to_pylist(),
                t.column("_source_weight")[0].as_py(),
            )
        record_ids = {sid: v[0] for sid, v in ids.items()}
        print(
            f"   {label:10s} record ids under three source ids equal: "
            f"{len({tuple(v) for v in record_ids.values()}) == 1} -> {record_ids['subjects']}"
        )
        for sid, (_, token) in ids.items():
            print(f"      token of row 0 under {sid!r}: {token}")
    print(
        "   across the six kinds (keyed): record-id column values equal:",
        all(
            src.as_table(columns={"system_tags": True})
            .column(
                next(
                    c
                    for c in src.as_table(columns={"system_tags": True}).column_names
                    if c.startswith("_tag_record_id")
                )
            )
            .to_pylist()
            == ['{"subject":"s1"}', '{"subject":"s2"}', '{"subject":"s3"}']
            for src in sources.values()
        ),
    )

    print("\n== CSV without a declared schema (types inferred) ==")
    inferred = same_rows_sources(scratch(), csv_schema=False)["csv"]
    print(
        "   arrow schemas:",
        [str(x).replace("\n", "; ") for x in inferred.arrow_schemas],
    )
    print(
        "   equal to arrow_table's:",
        inferred.arrow_schemas == sources["arrow_table"].arrow_schemas,
    )
    print(
        "   pipeline_hash equal:",
        inferred.pipeline_hash() == sources["arrow_table"].pipeline_hash(),
    )

    print("\n== Default source ids (no source_id given) ==")
    defaults = same_rows_sources(scratch(), source_id=None)
    for kind, src in defaults.items():
        sid = src.source_id
        if folder.as_posix() in sid or "/" in sid:
            sid = sid.split(":", 1)[0] + ":<location>"
        print(f"   {kind:13s} {sid}")


if __name__ == "__main__":
    main()
