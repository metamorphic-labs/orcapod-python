"""Sum test time per test file from pytest JUnit XML reports (ITL-1181).

The organization's required pytest check uploads one report per Python version
(``test-results-py3.X/test-results.xml``); download them with
``gh run download <run id>`` and pass their paths. Prints the files by total time over
all reports, with each file's share and the running share, leaving out the files that
protect what CI must always check (golden hashes, the hash label rule, the documentation
samples and build, the changelog and design-issue checks, the hash site inventory).

    uv run python docs/dev/scripts/itl_1181_per_file_times.py test-results-py3.*/test-results.xml
"""

from __future__ import annotations

import collections
import sys
import xml.etree.ElementTree as ET

PROTECTED = (
    "tests/test_hashing/golden/",
    "test_hash_label_rule.py",
    "test_hash_site_inventory.py",
    "test_doc_samples.py",
    "test_mkdocs_build.py",
    "test_changelog_fragments.py",
    "test_design_issues.py",
)


def module_path(classname: str) -> str:
    """The test file of a JUnit ``classname`` (``tests.a.test_b.TestC`` to ``tests/a/test_b.py``)."""
    parts = classname.split(".")
    while parts and parts[-1][:1].isupper():
        _ = parts.pop()
    return "/".join(parts) + ".py"


def main(paths: list[str], top: int = 19) -> None:
    """Print the ``top`` slowest unprotected files over the reports at ``paths``."""
    seconds: collections.Counter[str] = collections.Counter()
    total = 0.0
    for path in paths:
        for case in ET.parse(path).getroot().iter("testcase"):
            duration = float(case.get("time") or 0)
            seconds[module_path(case.get("classname", ""))] += duration
            total += duration
    print(f"{len(paths)} report(s), {total / len(paths):.0f} s of test time per report")
    running = 0.0
    shown = 0
    for name, spent in seconds.most_common():
        if any(marker in name for marker in PROTECTED):
            continue
        running += spent
        shown += 1
        print(
            f"{shown:2d} {spent / len(paths):6.1f} s {100 * spent / total:4.1f} %"
            + f" (running {100 * running / total:5.1f} %) {name}"
        )
        if shown == top:
            break


if __name__ == "__main__":
    main(sys.argv[1:])
