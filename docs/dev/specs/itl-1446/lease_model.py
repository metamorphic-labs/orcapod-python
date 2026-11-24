"""ITL-1446: exhaustive check of the claim-then-check lease (section 6.3).

Every interleaving of N writers. A writer: add its claim, read all claims, hold if its own
is the only one, else withdraw; a holder writes twice and releases. A writer may crash after
any step (its claim stays). Two variants show what the rule rests on: ``stale reads`` (the
check may miss the newest claim) and ``cleared while alive`` (a person deletes the claim of
a live holder).

Run: ``uv run python docs/dev/specs/itl-1446/lease_model.py``
"""

from __future__ import annotations

State = tuple[tuple[str, ...], frozenset[int], frozenset[int]]
HOLDING = ("hold", "wrote")


def steps(state: State, stale: bool, clear: bool) -> list[State]:
    """Every state one step away: writers' phases, claims stored, claims visible."""
    phases, claims, seen = state
    out: list[State] = []
    for i, phase in enumerate(phases):

        def to(
            new: str, c: frozenset[int] = claims, s: frozenset[int] = seen, i: int = i
        ) -> State:
            return ((*phases[:i], new, *phases[i + 1 :]), c, s)

        if phase == "start":
            # stored at once; visible at once unless reads may be stale
            out.append(to("claimed", claims | {i}, seen if stale else seen | {i}))
        elif phase == "claimed":
            out.append(to("hold" if not (seen - {i}) else "withdraw"))
        elif phase == "withdraw":
            out.append(to("refused", claims - {i}, seen - {i}))
        elif phase == "hold":
            out.append(to("wrote"))
        elif phase == "wrote":
            out.append(to("done", claims - {i}, seen - {i}))
        if phase not in ("start", "refused", "done", "crashed"):
            out.append(to("crashed"))
        if clear and i in claims:
            out.append((phases, claims - {i}, seen - {i}))
    if stale and claims - seen:
        out.append((phases, claims, claims))
    return out


def explore(writers: int, *, stale: bool = False, clear: bool = False) -> None:
    """Visit every reachable state and report whether two writers ever hold at once."""
    start: State = (("start",) * writers, frozenset(), frozenset())
    visited, queue, broken, all_refused = {start}, [start], False, False
    while queue:
        state = queue.pop()
        holders = sum(p in HOLDING for p in state[0])
        broken = broken or holders > 1
        all_refused = all_refused or set(state[0]) == {"refused"}
        for new in steps(state, stale, clear):
            if new not in visited:
                visited.add(new)
                queue.append(new)
    print(
        f"{writers} writers, stale reads {stale}, cleared while alive {clear}: "
        + f"{len(visited)} states, two holders possible: {broken}, "
        + f"all refused possible: {all_refused}"
    )


if __name__ == "__main__":
    for count in (2, 3, 4):
        explore(count)
    explore(2, stale=True)
    explore(2, clear=True)
