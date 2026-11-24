"""ITL-1446: exhaustive check of publishing a table by its catalog row (section 5).

One record path, two sessions one after the other (the lease), each creating its own table
when nothing is published. A session: create, commit first rows, publish (a catalog row),
mark the table; then perhaps tombstone (a row) and drop. The catalog only gains rows. A
session may crash at every point.

A reader without a lease runs at any time: read the catalog; list the tables; read each
table's marker; on a marked table with no row, or a row whose table is gone, read the
catalog once more; then decide ``absent``, ``published`` or ``refuse``.

Variants: ``late`` (the crashed session's next request may land at any later time, so the
lease was cleared before the service was quiet), ``lose`` (a publication row disappears),
``noreread`` (the reader does not read again), ``markfirst`` (the marker precedes the row),
``dropfirst`` (the drop precedes the tombstone).

Run: ``uv run python docs/dev/specs/itl-1446/publication_model.py``
"""

from __future__ import annotations

from typing import NamedTuple

FS = frozenset[int]
NONE: FS = frozenset()


class S(NamedTuple):
    """The service, the running session, requests in flight, and one reader."""

    tables: FS = NONE  # tables that exist
    rows: FS = NONE  # tables whose first rows are committed
    marked: FS = NONE
    pub: FS = NONE
    tomb: FS = NONE
    session: int = 0
    step: int = 0  # 0 not started; index into the program; -1 ended
    flying: frozenset[tuple[str, int]] = frozenset()
    complete: FS = NONE  # marked by a session that was told so, and not deleted
    lost: FS = NONE  # publication rows taken away
    rphase: str = "idle"
    c_pub: FS = NONE
    c_tomb: FS = NONE
    listed: FS = NONE
    r_marked: FS = NONE
    r_todo: FS = NONE
    r_complete: FS = NONE  # complete before the read began
    late_seen: bool = False


def program(options: frozenset[str]) -> list[str]:
    """The session's requests, in order."""
    first = ["mark", "publish"] if "markfirst" in options else ["publish", "mark"]
    last = ["drop", "tombstone"] if "dropfirst" in options else ["tombstone", "drop"]
    return ["create", "commit", *first, *last]


def apply(s: S, op: str, i: int) -> S:
    """The service carries out one request of session ``i``."""
    one = frozenset({i})
    if op == "create":
        return s._replace(tables=s.tables | one)
    if op == "commit":
        return s._replace(rows=s.rows | (one & s.tables))
    if op == "publish":
        return s._replace(pub=s.pub | one)
    if op == "mark":
        return s._replace(marked=s.marked | (one & s.tables))
    if op == "tombstone":
        return s._replace(tomb=s.tomb | one, complete=s.complete - one)
    return s._replace(tables=s.tables - one, complete=s.complete - one)


def writer(s: S, options: frozenset[str]) -> list[S]:
    """What the session does next."""
    if s.session > 1:
        return []
    ops = program(options)
    over = s._replace(session=s.session + 1, step=0)
    if s.step == 0 and s.pub - s.tomb:
        return [over]  # already published, refused, or opened: creates nothing
    if s.step == 0 and s.marked - s.pub - s.tomb:
        return [over]  # a marked table without its row: refuses
    out = [over]  # ends or crashes here
    if s.step < len(ops):
        op, i = ops[s.step], s.session
        done = apply(s, op, i)._replace(step=s.step + 1)
        if s.step == 3:
            done = done._replace(complete=done.complete | {i})
        out.append(done)
        if "late" in options:
            out.append(over._replace(flying=s.flying | {(op, i)}, late_seen=True))
    return out


def reader(s: S, options: frozenset[str], bad: set[str]) -> list[S]:
    """What the reader does next; a wrong verdict is added to ``bad``."""
    again = "noreread" not in options
    if s.rphase == "idle":
        return [
            s._replace(rphase="list", c_pub=s.pub, c_tomb=s.tomb, r_complete=s.complete)
        ]
    if s.rphase == "list":
        return [s._replace(rphase="marks", listed=s.tables, r_todo=s.tables)]
    if s.rphase == "marks":
        if s.r_todo:
            t = min(s.r_todo)
            found = frozenset({t}) & s.marked & s.tables
            return [s._replace(r_todo=s.r_todo - {t}, r_marked=s.r_marked | found)]
        odd = (s.r_marked - s.c_pub - s.c_tomb) | (s.c_pub - s.c_tomb - s.listed)
        if odd and again:
            return [s._replace(rphase="judge", c_pub=s.pub, c_tomb=s.tomb)]
        return [s._replace(rphase="judge")]
    if s.rphase == "judge":
        live = s.c_pub - s.c_tomb
        refuse = bool(s.r_marked - s.c_pub - s.c_tomb) or len(live) > 1
        # A row's table is opened by its id now; gone means the catalog is read again.
        gone = live - s.tables
        if gone and (not again or gone - s.tomb):
            refuse = True
        if gone and not refuse:
            live -= gone
        verdict = "refuse" if refuse else "published" if live else "absent"
        if verdict == "refuse" and not s.lost and not s.late_seen:
            bad.add("a healthy store refused")
        if verdict == "absent" and s.r_complete & s.complete:
            bad.add("a completed table read as absent")
        if live - s.rows and verdict == "published":
            bad.add("a table without its first rows read as published")
        if verdict == "refuse" and s.late_seen and not s.lost:
            bad.add("(late only) refused, for a person to resolve")
        return [s._replace(rphase="done")]
    return []


def explore(*names: str) -> None:
    """Visit every reachable state of one variant and print what was found."""
    options = frozenset(names)
    visited, queue, bad = {S()}, [S()], set[str]()
    while queue:
        s = queue.pop()
        new = writer(s, options) + reader(s, options, bad)
        for op, i in s.flying:
            new.append(apply(s, op, i)._replace(flying=s.flying - {(op, i)}))
        if "lose" in options:
            new += [
                s._replace(pub=s.pub - {i}, lost=s.lost | {i})
                for i in s.pub & s.complete
                if not s.lost
            ]
        for n in new:
            if n not in visited:
                visited.add(n)
                queue.append(n)
    print(f"{', '.join(names) or 'base'}: {len(visited)} states; {sorted(bad) or 'ok'}")


if __name__ == "__main__":
    explore()
    explore("lose")
    explore("late")
    explore("late", "lose")
    explore("noreread")
    explore("markfirst")
    explore("dropfirst")
