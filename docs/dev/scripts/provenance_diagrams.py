"""Draw the provenance diagrams shown in the documentation.

The pictures are drawn by Orcapod itself (``job.diagram(highlight=...)``) from the
tutorial's synthetic dataset (``docs/tutorial/workshop_data.py``), over the pipelines of
the tutorial "Tracing a value". Each is the diagram's standalone SVG (the image a notebook shows: on its
own white background, so it reads on a light and a dark page alike).

Run it to rewrite the pictures after a change to the diagram::

    uv run python docs/dev/scripts/provenance_diagrams.py

``tests/test_docs/test_provenance_diagrams.py`` draws them again and compares them with the
committed files, so they never go stale.
"""

from __future__ import annotations

import base64
import re
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Any

REPOSITORY = Path(__file__).resolve().parents[3]
TUTORIAL = REPOSITORY / "docs" / "tutorial"

# The committed pictures, by file name (in docs/tutorial/).
PICTURES = (
    "provenance-diagram-plain.svg",
    "provenance-diagram-trace.svg",
    "provenance-diagram-missing-filter.svg",
    "provenance-diagram-missing-join.svg",
)

_IMAGE = re.compile(
    r'<img class="op-dag-picture"[^>]* src="data:image/svg\+xml;base64,([^"]+)"'
)


def _standalone_svg(diagram: object) -> str:
    """The standalone SVG of a diagram: the picture its notebook display shows."""
    html = diagram.to_html()  # pyright: ignore[reportAttributeAccessIssue]
    match = _IMAGE.search(html)
    if match is None:
        raise RuntimeError("the diagram's HTML holds no picture")
    return base64.b64decode(match.group(1)).decode("utf-8") + "\n"


def render() -> dict[str, str]:
    """Draw every picture.

    Returns:
        The SVG text of each picture, by file name.
    """
    sys.path.insert(0, str(TUTORIAL))
    from workshop_data import make_dataset  # pyright: ignore[reportMissingImports]

    from orcapod import PipelineJob, function_pod
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.sources import CSVSource
    from orcapod.types import col

    @function_pod(output_keys="rate_hz")
    def firing_rate(n_spikes: int, duration_s: float) -> float:
        return n_spikes / duration_s

    @function_pod(output_keys="mean_rate_hz")
    def mean_rate(rate_hz: list[float]) -> float:
        return sum(rate_hz) / len(rate_hz)

    with tempfile.TemporaryDirectory() as folder, warnings.catch_warnings():
        warnings.simplefilter("ignore")
        data_dir = make_dataset(Path(folder))
        database = InMemoryArrowDatabase()

        def units() -> CSVSource:
            return CSVSource(
                str(data_dir / "units.csv"),
                tag_columns=["subject_id", "session", "probe", "unit"],
                tag_only_columns=True,
                source_id="units",
                label="units",
            )

        def sessions() -> CSVSource:
            return CSVSource(
                str(data_dir / "sessions.csv"),
                tag_columns=["subject_id", "session"],
                tag_only_columns=True,
                source_id="sessions",
                label="sessions",
            )

        job = PipelineJob(name="workshop", database=database)
        with job:
            # As in the tutorial: a stream method (StreamProtocol does not declare it).
            rates: Any = firing_rate.pod(
                units().select_data_columns(["n_spikes"]),
                sessions().select_data_columns(["duration_s"]),
                label="rates",
            )
            mean_rate.pod(
                rates.group_by(["subject_id", "session"]).select_data_columns(
                    ["rate_hz"]
                ),
                label="session_means",
            )
        _ = job.run()
        # One firing rate: its path covers the sources, the Join and rates, not the
        # grouping after them; its result was reused (an equal input computed it).
        trace = job.nodes["rates"].trace(
            {"subject_id": "mouse_02", "session": "s1", "probe": "imec1", "unit": 2},
            "rate_hz",
        )

        imec0 = PipelineJob(name="imec0", database=database)
        with imec0:
            firing_rate.pod(
                units().filter({"probe": "imec0"}).select_data_columns(["n_spikes"]),
                sessions()
                .filter(col("subject_id") != "mouse_04")
                .select_data_columns(["duration_s"]),
                label="imec0_rates",
            )
        _ = imec0.run()
        node = imec0.nodes["imec0_rates"]
        return {
            "provenance-diagram-plain.svg": _standalone_svg(job.diagram()),
            "provenance-diagram-trace.svg": _standalone_svg(
                job.diagram(highlight=trace.highlight())
            ),
            "provenance-diagram-missing-filter.svg": _standalone_svg(
                imec0.diagram(highlight=node.why_missing("units", "row_4").highlight())
            ),
            "provenance-diagram-missing-join.svg": _standalone_svg(
                imec0.diagram(highlight=node.why_missing("units", "row_48").highlight())
            ),
        }


def main() -> None:
    """Write every picture into docs/tutorial/."""
    for name, svg in render().items():
        (TUTORIAL / name).write_text(svg, encoding="utf-8")
        print(f"wrote docs/tutorial/{name}")


if __name__ == "__main__":
    main()
