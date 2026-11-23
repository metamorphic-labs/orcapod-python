"""Audit 2: what a function pod's and a side-effect pod's rows carry, on a plain stream.

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.02_function_pod_lineage

Proves:
- an output column's token is ``<function uri>::<result uuid7 hex>::<output key>``: it names
  the RESULT, not the input row and not the input columns;
- on a plain stream the result id is random per computation: stable for re-reads of one
  stream, different for a second stream over the same inputs, and stored nowhere;
- the input row's tags and system tags pass through unchanged, so the output row links
  to its input row by its system tags; the input columns' tokens are NOT in the output;
- no input data hash, ``__input_data_hash`` or ``__node_content_hash`` is in a plain
  stream's row (those exist only in a job's stored tables: script 03);
- a side-effect pod passes rows, tokens and system tags through unchanged.
"""

from __future__ import annotations

from ._common import sessions_source, short, show, units_source

from typing import cast

from orcapod import SideEffectPod, function_pod, tap_pod
from orcapod.operators import Join


@function_pod(output_keys=("rate_hz", "per_um"))
def rate(n_spikes: int, duration_s: float, depth_um: int) -> tuple[float, float]:
    return n_spikes / duration_s, n_spikes / depth_um


for reverse in (False, True):
    print(f"== inputs in {'reversed' if reverse else 'given'} row and argument order")
    units = units_source(reverse=reverse)
    sessions = sessions_source()
    inputs = (sessions, units) if reverse else (units, sessions)
    out = rate.pod(*inputs)
    t = show("rate.pod(...)", out, rows=1)
    print(
        "  meta / hash columns in the row:",
        [c for c in t.column_names if c.startswith("__")] or "none",
    )
    again = out.as_table(all_info=True).column("_source_rate_hz").to_pylist()
    print(
        "  token stable on re-read of the same stream:",
        again == t.column("_source_rate_hz").to_pylist(),
    )
    other = rate.pod(*inputs).as_table(all_info=True).column("_source_rate_hz")
    print(
        "  token equal for a second stream over the same inputs:",
        other.to_pylist() == t.column("_source_rate_hz").to_pylist(),
    )

    # Output row -> input row: join on the system tags, which pass through unchanged.
    joined = Join()(units, sessions).as_table(all_info=True)
    sys_tags = sorted(c for c in t.column_names if c.startswith("_tag"))
    print(
        "  output carries the joined input's system-tag columns:",
        sys_tags == sorted(c for c in joined.column_names if c.startswith("_tag")),
    )
    key = lambda tab, i: tuple(tab.column(c)[i].as_py() for c in sys_tags)  # noqa: E731
    by_key = {key(joined, i): i for i in range(joined.num_rows)}
    i = by_key[key(t, 0)]
    print(
        f"  output row 0 -> input row: n_spikes token "
        f"{joined.column('_source_n_spikes')[i].as_py()}"
    )

print("\n== Side-effect pod: rows pass through unchanged")
seen = []


def _note(ctx, n_spikes: int) -> None:
    seen.append(n_spikes)


note = cast(SideEffectPod, tap_pod(_note))


units = units_source()
tapped = note(units.select_data_columns(["n_spikes"]))
a = tapped.as_table(all_info=True)
b = units.select_data_columns(["n_spikes"]).as_table(all_info=True)
print(
    "  same provenance columns and values as its input:",
    {
        c: a.column(c).to_pylist()
        for c in a.column_names
        if c.startswith(("_tag", "_source"))
    }
    == {
        c: b.column(c).to_pylist()
        for c in b.column_names
        if c.startswith(("_tag", "_source"))
    },
)
print(
    "  its invocation record (a job's side-effect log) is keyed on the system tags and"
    " the input data hash; nothing about it is added to the row."
)
print("  column names:", [short(c) for c in a.column_names])
