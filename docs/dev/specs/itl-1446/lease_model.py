"""ITL-1446: exhaustive check of the named-table lease (section 4 of the design).

One table name; creating it succeeds for one caller only (atomic, measured live). A writer:
creates; on a lost reply looks the name up and owns the table only when its own token is in
it; writes; drops **its own table id**. Every interleaving, with a crash after any step.

Variants, each adding one thing:

* ``late``: a create whose reply was lost may land after the writer looked (a ghost lease).
* ``late_drop``: a drop may be sent again after it already succeeded.
* ``clear``: a person drops the current table's id while its holder is alive (forbidden).
* ``by_name``: drops go by name instead of by id (the rule the design rests on, removed).

Run: ``uv run python docs/dev/specs/itl-1446/lease_model.py``
"""

from __future__ import annotations

from typing import NamedTuple

HOLDING = ("hold", "wrote")


class State(NamedTuple):
    """Writers' phases, the table's (owner, id) or ``None``, next id, own ids, in flight."""

    phases: tuple[str, ...]
    table: tuple[int, int] | None
    next_id: int
    ids: tuple[int, ...]
    flying: frozenset[int]


def steps(
    s: State, *, late: bool, late_drop: bool, clear: bool, by_name: bool
) -> list[State]:
    """Every state one step away."""
    out: list[State] = []
    for i, phase in enumerate(s.phases):

        def to(new: str, i: int = i) -> tuple[str, ...]:
            return (*s.phases[:i], new, *s.phases[i + 1 :])

        own = (*s.ids[:i], s.next_id, *s.ids[i + 1 :])
        if phase == "start":
            if s.table is None:
                made = s._replace(table=(i, s.next_id), next_id=s.next_id + 1)
                out.append(made._replace(phases=to("hold"), ids=own))
                out.append(made._replace(phases=to("unsure")))  # reply lost
            else:
                out.append(s._replace(phases=to("refused")))
                out.append(s._replace(phases=to("unsure")))  # refusal lost
            if late:
                out.append(s._replace(phases=to("unsure"), flying=s.flying | {i}))
        elif phase == "unsure":
            if s.table is not None and s.table[0] == i:
                ids = (*s.ids[:i], s.table[1], *s.ids[i + 1 :])
                out.append(s._replace(phases=to("hold"), ids=ids))
            else:
                out.append(s._replace(phases=to("refused")))
        elif phase == "hold":
            out.append(s._replace(phases=to("wrote")))
        elif phase == "wrote" or (late_drop and phase == "done"):
            mine = s.table is not None and (by_name or s.table[1] == s.ids[i])
            out.append(s._replace(phases=to("done"), table=None if mine else s.table))
        if phase not in ("start", "refused", "done", "crashed"):
            out.append(s._replace(phases=to("crashed")))
        if i in s.flying:
            landed = s._replace(flying=s.flying - {i})
            if s.table is None:
                landed = landed._replace(table=(i, s.next_id), next_id=s.next_id + 1)
            out.append(landed)
    if clear and s.table is not None:
        out.append(s._replace(table=None))
    return out


def explore(writers: int, **variant: bool) -> None:
    """Visit every reachable state; report two holders at once, and a stuck lease."""
    options = {"late": False, "late_drop": False, "clear": False, "by_name": False}
    options.update(variant)
    start = State(("start",) * writers, None, 0, (-1,) * writers, frozenset())
    visited, queue, two, stuck = {start}, [start], False, False
    while queue:
        s = queue.pop()
        two = two or sum(p in HOLDING for p in s.phases) > 1
        ended = all(p in ("refused", "done", "crashed") for p in s.phases)
        stuck = stuck or (ended and not s.flying and s.table is not None)
        for new in steps(s, **options):
            if new not in visited:
                visited.add(new)
                queue.append(new)
    print(
        f"{writers} writers, {', '.join(variant) or 'base'}: {len(visited)} states, "
        + f"two holders possible: {two}, a lease left behind possible: {stuck}"
    )


if __name__ == "__main__":
    for count in (2, 3, 4):
        explore(count)
    explore(3, late=True)
    explore(3, late_drop=True)
    explore(3, late=True, late_drop=True)
    explore(2, clear=True)
    explore(3, late_drop=True, by_name=True)
