"""The one-writer lease and write sessions of a SpiralDB store (ITL-1452), on the fake.

Every request a client sends goes through a ``Wire``, so a test says exactly what happens at
each one: nothing is timed and nothing sleeps. Threads are handed on by events and by the
``Schedule`` of the interleaving test, never by waiting a while.
"""

from __future__ import annotations

import ast
import asyncio
import os
import threading
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import orcapod
from orcapod.databases import _spiral_lease as lease_module
from orcapod.databases._spiral_lease import (
    LEASE_TABLE_NAME,
    LeaseHeldError,
    LeaseLeftError,
    LeaseLostError,
    LeaseUncertainError,
    NestedWriteSessionError,
    NoWriteSessionError,
    async_write_session,
    clear_lease,
    current_capability,
    take_lease,
    without_capability,
    write_session,
)
from orcapod.databases.spiraldb_connector import SpiralDBConnector
from tests.test_databases import fake_pyspiral

WAIT = 30  # seconds before a hung test fails; no test waits this long


class Killed(BaseException):
    """The process died: raised by this and every later request of its client."""


class Wire:
    """Carries one client's requests; a test decides what happens to each.

    ``on(kind, ...)`` queues what happens to the next requests of that kind: a callable given
    the request to send. With nothing queued a request is sent.
    """

    def __init__(self) -> None:
        self.log: list[str] = []
        self.dead = False
        self._plans: dict[str, list[Callable[[Callable[[], Any]], Any]]] = {}

    def on(self, kind: str, *plans: Callable[[Callable[[], Any]], Any]) -> None:
        self._plans.setdefault(kind, []).extend(plans)

    def __call__(self, kind: str, request: Callable[[], Any]) -> Any:
        if self.dead:
            raise Killed
        self.log.append(kind)
        plans = self._plans.get(kind)
        return plans.pop(0)(request) if plans else request()


def send(request: Callable[[], Any]) -> Any:
    return request()


def lost_reply(request: Callable[[], Any]) -> Any:
    """The service does it (or refuses it); the client sees a timeout."""
    try:
        request()
    except Exception:
        pass
    raise TimeoutError("reply lost")


def lost_request(request: Callable[[], Any]) -> Any:
    raise TimeoutError("request lost")


def retried_by_client(request: Callable[[], Any]) -> Any:
    """The client repeats a request whose reply was lost, and sees the second reply."""
    request()
    return request()


class WiredProject:
    def __init__(self, project: Any, wire: Wire) -> None:
        self._project, self._wire = project, wire

    def create_table(self, *args: Any, **kwargs: Any) -> Any:
        return self._wire("create", lambda: self._project.create_table(*args, **kwargs))

    def list_tables(self) -> Any:
        return self._project.list_tables()

    def table(self, identifier: str) -> Any:
        return self._wire("lookup", lambda: self._project.table(identifier))


class WiredSpiral:
    def __init__(self, spiral: Any, wire: Wire) -> None:
        self._spiral, self._wire = spiral, wire

    @property
    def core(self) -> Any:
        return self

    def drop_table(self, table_id: str) -> None:
        self._wire("drop", lambda: self._spiral.core.drop_table(table_id=table_id))


class Client(SpiralDBConnector):
    """A connector whose lease requests go through its ``wire``."""

    def __init__(
        self,
        dataset: str = "store",
        wire: Wire | None = None,
        project: str = "p",
        endpoint: str | None = None,
    ) -> None:
        overrides = None if endpoint is None else {"server.url": endpoint}
        super().__init__(project, dataset, overrides)
        self.wire = wire or Wire()
        self.real_project = self._project
        self.real_spiral = self._spiral
        self._project: Any = WiredProject(self._project, self.wire)
        self._spiral: Any = WiredSpiral(self._spiral, self.wire)

    def holder(self) -> Any:
        """The lease table as another client sees it, or ``None``."""
        try:
            return self.real_project.table(f"{self._dataset}.{LEASE_TABLE_NAME}")
        except ValueError:
            return None

    def put(self, key: int, value: str) -> None:
        """Send one row of data, as a commit of a store does."""
        table = self.real_project.create_table(
            f"{self._dataset}.data", key_schema=[("k", pa.int64())], exist_ok=True
        )
        rows = pa.table({"k": [key], "v": [value]})
        self.wire("write", lambda: table.write(rows))

    def rows(self) -> dict[int, str]:
        table = self.real_project.table(f"{self._dataset}.data")
        stored = self.real_spiral.scan(table.select()).to_table().to_pydict()
        return dict(zip(stored["k"], stored["v"])) if "v" in stored else {}


@pytest.fixture(autouse=True)
def service(tmp_path: Path) -> Iterator[None]:
    with fake_pyspiral.install(tmp_path):
        yield


def commit(client: Client, key: int, value: str) -> None:
    with current_capability().commit(client):
        client.put(key, value)


def kill(client: Client) -> None:
    client.wire.dead = True


def test_nothing_uses_the_module_yet() -> None:
    """Inert until PR 8: no module of the library imports it."""
    root = Path(orcapod.__file__).parent
    users = [
        path.name
        for path in root.rglob("*.py")
        for node in ast.walk(ast.parse(path.read_text()))
        if isinstance(node, (ast.Import, ast.ImportFrom))
        and "_spiral_lease" in ast.unparse(node)
    ]
    assert users == []


# ------------------------------------------------------------------- the fake's own rules


def test_fake_exclusive_create_and_drop_by_id_as_measured() -> None:
    client = Client()
    project, core = client.real_project, client.real_spiral.core
    first = project.create_table("store.t", key_schema=[("a", pa.int64())])
    with pytest.raises(Exception, match=r"^409 Conflict: Table p\.store\.t already"):
        project.create_table("store.t", key_schema=[("b", pa.int64())])
    core.drop_table(table_id=first.table_id)
    second = project.create_table("store.t", key_schema=[("b", pa.int64())])
    assert first.table_id != second.table_id
    with pytest.raises(Exception, match="^404 Not Found: Resource"):
        core.drop_table(table_id=first.table_id)
    assert project.table("store.t").table_id == second.table_id
    with pytest.raises(ValueError, match="Table not found"):
        project.drop_table("store.absent")


def test_fake_create_has_one_winner_among_threads() -> None:
    start = threading.Barrier(5)

    def create(index: int) -> bool:
        project = Client().real_project
        start.wait(WAIT)
        try:
            project.create_table("store.t", key_schema=[(f"k{index}", pa.int64())])
        except Exception as exc:
            assert str(exc).startswith("409 ")
            return False
        return True

    with ThreadPoolExecutor(5) as pool:
        assert sorted(pool.map(create, range(5))) == [False] * 4 + [True]


# ------------------------------------------------------------------------------ the lease


def test_take_check_and_release() -> None:
    client = Client()
    lease = take_lease(client, "My Job/№ 7." + "x" * 60)
    assert client.holder().table_id == lease.table_id
    (owner,) = client.holder().key_schema.names
    kind, token, started, pid, job = owner.split("-")
    assert (kind, int(pid), len(job)) == ("own", os.getpid(), 40)
    assert uuid.UUID(token).version == 7
    assert started.endswith("Z")
    assert job.startswith("my_job_7_xxx")
    lease.check()
    lease.release()
    assert client.holder() is None
    assert client.wire.log == ["create", "lookup", "drop"]


def test_tokens_are_never_reused() -> None:
    owners = set()
    for _ in range(3):
        lease = take_lease(Client(), "j")
        owners.add(lease.owner.split("-")[1])
        lease.release()
    assert len(owners) == 3


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
@pytest.mark.filterwarnings("ignore:.*fork.*:DeprecationWarning")
def test_a_fork_makes_its_own_tokens() -> None:
    read, write = os.pipe()
    child = os.fork()
    if child == 0:
        os.write(write, lease_module._new_token().encode())
        os._exit(0)
    os.close(write)
    os.waitpid(child, 0)
    mine = lease_module._new_token()
    assert os.read(read, 100).decode()[12:] != mine[12:]
    os.close(read)


def test_a_second_run_is_refused_at_once_with_the_owner_and_the_id() -> None:
    first = take_lease(Client(), "first job")
    second = Client()
    with pytest.raises(LeaseHeldError) as refused:
        take_lease(second, "second job")
    assert refused.value.table_id == first.table_id
    assert first.owner in str(refused.value)
    assert first.table_id in str(refused.value)
    assert "does not show that" in str(refused.value)
    assert second.wire.log.count("create") == 1


def test_a_refusal_whose_holder_left_meanwhile_is_not_retried() -> None:
    lease = take_lease(Client(), "first")
    second = Client()

    def refused_then_released(request: Callable[[], Any]) -> Any:
        try:
            return request()
        finally:
            lease.release()

    second.wire.on("create", refused_then_released)
    with pytest.raises(LeaseHeldError, match="Run again"):
        take_lease(second, "second")
    assert second.holder() is None


def test_every_run_after_a_crash_is_refused_whatever_its_owner_text_says() -> None:
    """No age, process id or form of the owner text makes a lease expire."""
    client = Client()
    for owner in (
        "own-" + "0" * 32 + "-20200101T000000Z-999999-old",
        f"own-{'1' * 32}-20200101T000000Z-{os.getpid()}-this_pid",
        "not an owner text",
    ):
        table = client.real_project.create_table(
            f"store.{LEASE_TABLE_NAME}", key_schema=[(owner, pa.int64())]
        )
        for _ in range(3):
            with pytest.raises(LeaseHeldError) as refused:
                take_lease(Client(), "next")
            assert owner in str(refused.value)
        assert clear_lease(client, table.table_id)


@pytest.mark.parametrize("reply", [lost_reply, retried_by_client])
def test_a_create_that_landed_without_its_reply_is_recognised(
    reply: Callable[..., Any],
) -> None:
    client = Client()
    client.wire.on("create", reply)
    lease = take_lease(client, "j")
    assert client.holder().table_id == lease.table_id
    assert client.holder().key_schema.names == [lease.owner]
    lease.release()
    assert client.holder() is None


def test_a_lost_refusal_is_a_refusal() -> None:
    first = take_lease(Client(), "first")
    client = Client()
    client.wire.on("create", lost_reply)
    with pytest.raises(LeaseHeldError) as refused:
        take_lease(client, "j")
    assert refused.value.table_id == first.table_id


@pytest.mark.parametrize("create", [lost_request, lost_reply])
def test_a_lookup_that_fails_never_grants_the_lease(create: Callable[..., Any]) -> None:
    client = Client()
    client.wire.on("create", create)
    client.wire.on("lookup", lost_request)
    with pytest.raises(LeaseUncertainError, match="own-"):
        take_lease(client, "j")
    assert client.wire.log == ["create", "lookup"]


def test_a_late_create_is_a_lease_left_behind_never_a_second_holder() -> None:
    client = Client()
    late: list[Callable[[], Any]] = []

    def sent_late(request: Callable[[], Any]) -> Any:
        late.append(request)
        raise TimeoutError

    client.wire.on("create", sent_late)
    with pytest.raises(LeaseUncertainError) as uncertain:
        take_lease(client, "j")
    # It lands while another run holds the lease: refused by the service.
    other = take_lease(Client(), "other")
    with pytest.raises(Exception, match="^409"):
        late[0]()
    other.check()
    other.release()
    # It lands on a free name: every run is refused, until a person clears it.
    ghost = late[0]()
    (owner,) = client.holder().key_schema.names
    assert owner in str(uncertain.value)
    with pytest.raises(LeaseHeldError) as refused:
        take_lease(Client(), "next")
    assert refused.value.table_id == ghost.table_id
    assert clear_lease(Client(), ghost.table_id)
    take_lease(Client(), "next").release()


def test_an_interrupted_create_is_undone() -> None:
    client = Client()

    def interrupted(request: Callable[[], Any]) -> Any:
        request()
        raise KeyboardInterrupt

    client.wire.on("create", interrupted)
    with pytest.raises(KeyboardInterrupt):
        take_lease(client, "j")
    assert client.holder() is None


def test_an_interrupted_create_that_cannot_be_undone_says_so() -> None:
    client = Client()

    def interrupted(request: Callable[[], Any]) -> Any:
        request()
        raise KeyboardInterrupt

    client.wire.on("create", interrupted)
    client.wire.on("lookup", lost_request)
    with pytest.raises(KeyboardInterrupt) as raised:
        take_lease(client, "j")
    assert client.holder().key_schema.names[0] in raised.value.__notes__[0]


def test_drops_go_by_id_and_a_repeated_drop_spares_a_newer_lease() -> None:
    first = take_lease(Client(), "first")
    first.release()
    newer = take_lease(Client(), "newer")
    first.release()  # 404: success, and nothing is dropped
    newer.check()


@pytest.mark.parametrize("plans", [[lost_reply], [lost_request, lost_reply]])
def test_a_drop_without_a_clear_reply_is_settled_by_its_id_alone(
    plans: list[Any],
) -> None:
    client = Client()
    lease = take_lease(client, "j")
    client.wire.on("drop", *plans)
    lease.release()
    assert client.holder() is None
    # The drop is sent again until the id itself is answered; no look at the name decides.
    assert client.wire.log == ["create", *["drop"] * (len(plans) + 1)]


def test_a_name_that_looks_free_does_not_prove_the_lease_is_gone() -> None:
    client = Client()
    lease = take_lease(client, "j")
    client.wire.on("drop", *[lost_reply] * 3)
    with pytest.raises(LeaseLeftError):
        lease.release()
    assert client.holder() is None


def test_a_lost_drop_reply_after_which_a_new_lease_was_taken() -> None:
    client = Client()
    lease = take_lease(client, "j")
    newer = []

    def dropped_then_taken(request: Callable[[], Any]) -> Any:
        request()
        newer.append(take_lease(Client(), "newer"))
        raise TimeoutError

    client.wire.on("drop", dropped_then_taken)
    lease.release()
    newer[0].check()


def test_a_drop_that_cannot_be_settled_names_the_id() -> None:
    client = Client()
    lease = take_lease(client, "j")
    client.wire.on("drop", *[lost_request] * 3)
    with pytest.raises(LeaseLeftError) as left:
        lease.release()
    assert left.value.table_id == lease.table_id
    assert lease.table_id in str(left.value)


def test_clear_drops_exactly_the_id_it_is_given() -> None:
    client = Client()
    table = client.real_project.create_table(
        "store.data", key_schema=[("k", pa.int64())]
    )
    assert not clear_lease(client, table.table_id)  # no lease: never another table
    first = take_lease(Client(), "first")
    assert not clear_lease(client, table.table_id)
    assert client.real_project.table("store.data").table_id == table.table_id
    assert clear_lease(client, first.table_id)
    assert not clear_lease(client, first.table_id)
    second = take_lease(Client(), "second")
    assert not clear_lease(client, first.table_id)
    second.check()


# ------------------------------------------------------- every interleaving, with crashes


class Schedule:
    """Runs writers one request at a time, in the order a list of choices gives.

    A step is ``(writer, fault)``. Choices beyond the given ones take the first option, and
    the options of every step are recorded, so a caller can visit them all.
    """

    def __init__(self, choices: list[int], faults: int) -> None:
        self.choices, self.faults = choices, faults
        self.options: list[int] = []
        self.holding: set[int] = set()
        self.late: list[Callable[[], Any]] = []
        self._turn = threading.Condition()
        self._waiting: dict[int, tuple[str, Callable[[], Any]] | None] = {}
        self._replies: dict[int, Callable[[], Any]] = {}
        self._ended: set[int] = set()

    def run(self, bodies: list[Callable[[Wire], None]]) -> None:
        errors: list[BaseException] = []

        def writer(index: int) -> None:
            wire = Wire()
            wire.__class__ = type(
                "Scheduled",
                (Wire,),
                {"__call__": lambda _, kind, request: self._ask(index, kind, request)},
            )
            try:
                bodies[index](wire)
            except (Killed, LeaseHeldError, LeaseUncertainError, LeaseLeftError):
                pass
            except BaseException as exc:
                errors.append(exc)
            finally:
                with self._turn:
                    self._ended.add(index)
                    self._turn.notify_all()

        threads = [
            threading.Thread(target=writer, args=(i,)) for i in range(len(bodies))
        ]
        for thread in threads:
            thread.start()
        while self._step(len(bodies)):
            pass
        for thread in threads:
            thread.join(WAIT)
            assert not thread.is_alive()
        if errors:
            raise errors[0]

    def _ask(self, index: int, kind: str, request: Callable[[], Any]) -> Any:
        with self._turn:
            if self._replies.get(index) is _die:
                raise Killed
            self._waiting[index] = (kind, request)
            self._turn.notify_all()
            assert self._turn.wait_for(lambda: index in self._replies, WAIT)
            reply = self._replies[index]
            if reply is not _die:
                del self._replies[index]
        return reply()

    def _step(self, count: int) -> bool:
        with self._turn:
            assert self._turn.wait_for(
                lambda: len(self._ended) + len(self._waiting) == count, WAIT
            )
            if not self._waiting:
                for request in self.late:  # what is still in flight lands now
                    _quietly(request)
                return False
            options: list[tuple[int, str]] = []
            for index, waiting in sorted(self._waiting.items()):
                assert waiting is not None
                options.append((index, "send"))
                if self.faults:
                    options += [(index, "kill"), (index, "kill after")]
                    if waiting[0] in ("create", "drop"):
                        options += [(index, "lose reply"), (index, "late")]
            if self.late:
                options.append((-1, "land"))
            taken = (
                self.choices[len(self.options)]
                if len(self.options) < len(self.choices)
                else 0
            )
            self.options.append(len(options))
            index, fault = options[taken]
            if fault == "land":
                _quietly(self.late.pop(0))
                return True
            waiting = self._waiting.pop(index)
            assert waiting is not None
            kind, request = waiting
            if fault != "send":
                self.faults -= 1
            if fault in ("kill", "kill after"):
                if fault == "kill after":
                    _quietly(request)
                self.holding.discard(index)
                self._replies[index] = _die
            elif fault == "lose reply":
                _quietly(request)
                self._replies[index] = _time_out
            elif fault == "late":
                self.late.append(request)
                self._replies[index] = _time_out
            else:
                # Sent here, in the scheduler's thread: one request at a time.
                self._replies[index] = _outcome(request)
            self._turn.notify_all()
            return True


def _die() -> Any:
    raise Killed


def _time_out() -> Any:
    raise TimeoutError


def _quietly(request: Callable[[], Any]) -> None:
    try:
        request()
    except Exception:
        pass


def _outcome(request: Callable[[], Any]) -> Callable[[], Any]:
    try:
        value = request()
    except Exception as exc:
        error = exc

        def fail() -> Any:
            raise error

        return fail
    return lambda: value


def scheduled_writer(
    schedule: Schedule, dataset: str, index: int
) -> Callable[[Wire], None]:
    """Take the lease, commit once, release; never beside another holder."""

    def write(wire: Wire) -> None:
        client = Client(dataset, wire)
        with write_session(client, job=f"w{index}") as capability:
            try:
                schedule.holding.add(index)
                assert schedule.holding == {index}, "two holders"

                def data() -> None:
                    mine = capability._session.lease
                    assert client.holder().table_id == mine.table_id

                with capability.commit(client):
                    client.wire("write", data)
                assert schedule.holding == {index}, "two holders"
            finally:
                schedule.holding.discard(index)

    return write


@pytest.mark.parametrize(
    ("writers", "faults", "fewest"), [(2, 0, 20), (3, 0, 200), (2, 1, 500)]
)
def test_never_two_holders_in_any_interleaving(
    writers: int, faults: int, fewest: int, tmp_path: Path
) -> None:
    """Every order of the requests; with ``faults``, one fault at every possible place.

    A fault is a crash before or after a request, a lost reply, or a create or drop that
    lands at any later point. Each writer takes the lease, commits once and releases.
    """
    choices: list[int] = []
    runs = 0
    while True:
        runs += 1
        dataset = f"run{runs}"
        schedule = Schedule(choices, faults)

        schedule.run([scheduled_writer(schedule, dataset, i) for i in range(writers)])
        # the next unvisited branch
        taken = choices + [0] * (len(schedule.options) - len(choices))
        while taken and taken[-1] + 1 == schedule.options[len(taken) - 1]:
            taken.pop()
        if not taken:
            break
        choices = [*taken[:-1], taken[-1] + 1]
    assert runs >= fewest


# --------------------------------------------------------------- crashes between two steps

STEPS = ["create", "lookup", "write", "drop"]


@pytest.mark.parametrize("step", STEPS)
@pytest.mark.parametrize("when", ["before", "after"])
def test_a_crash_at_each_step_leaves_the_lease_or_nothing(step: str, when: str) -> None:
    """A run is take, check, commit, release; its process dies before or after each."""
    client = Client()

    def die(request: Callable[[], Any]) -> Any:
        if when == "after":
            request()
        kill(client)
        raise Killed

    client.wire.on(step, die)
    with pytest.raises(Killed):
        with write_session(client, job="crash") as capability:
            with capability.commit(client):
                client.put(1, "a")
    held = (step, when) not in (("create", "before"), ("drop", "after"))
    other = Client()
    assert (other.holder() is not None) is held
    if held:
        for _ in range(2):  # it never expires
            with pytest.raises(LeaseHeldError) as refused:
                with write_session(other, job="next"):
                    raise AssertionError
            assert refused.value.table_id == other.holder().table_id
        assert clear_lease(other, other.holder().table_id)
    with write_session(other, job="next"):
        commit(other, 2, "b")
    expected = {2: "b"}
    if step == "drop" or (step, when) == ("write", "after"):
        expected[1] = "a"
    assert other.rows() == expected


# ------------------------------------------------------------------- sessions (section 4.5)


def test_a_session_takes_writes_and_releases() -> None:
    client = Client()
    with write_session(client, job="j") as capability:
        assert current_capability() is capability
        capability.require(client)
        commit(client, 1, "a")
    assert client.rows() == {1: "a"}
    assert client.holder() is None
    assert client.wire.log == ["create", "lookup", "write", "drop"]
    with pytest.raises(NoWriteSessionError):
        current_capability()


def test_an_unchanged_rerun_takes_the_lease_too() -> None:
    client = Client()
    with write_session(client, job="j"):
        pass
    assert client.wire.log == ["create", "drop"]


def test_a_run_that_fails_releases() -> None:
    client = Client()
    with pytest.raises(ZeroDivisionError):
        with write_session(client, job="j"):
            commit(client, 1, "a")
            raise ZeroDivisionError
    assert client.holder() is None
    assert client.rows() == {1: "a"}


def test_a_write_by_hand_is_refused_before_anything_is_sent() -> None:
    client = Client()
    with write_session(client, job="j") as capability:
        pass
    with pytest.raises(NoWriteSessionError, match=r"store\.writing\(\)"):
        current_capability()
    for attempt in (
        lambda: capability.require(client),
        lambda: capability.commit(client).__enter__(),
        lambda: capability.add_final_flush(print),
    ):
        with pytest.raises(NoWriteSessionError):
            attempt()
    assert client.wire.log == ["create", "drop"]


def test_reads_need_no_session_and_are_never_refused() -> None:
    client, reader = Client(), Client()
    with write_session(client, job="j"):
        commit(client, 1, "a")
        with without_capability():
            assert reader.rows() == {1: "a"}
            assert reader.get_table_names() == ["data", LEASE_TABLE_NAME]
    assert reader.wire.log == []


def test_a_dataset_the_session_did_not_lease_is_refused() -> None:
    with write_session(Client("a"), job="j") as capability:
        with pytest.raises(NoWriteSessionError, match="p/b"):
            capability.require(Client("b"))
        with pytest.raises(NoWriteSessionError, match="q/a"):
            with capability.commit(Client("a", project="q")):
                raise AssertionError


def test_a_nested_session_is_refused_before_any_request() -> None:
    client, other = Client("a"), Client("b")
    with write_session(client, job="j"):
        for dataset in (client, other):
            with pytest.raises(NestedWriteSessionError):
                with write_session(dataset, job="nested"):
                    raise AssertionError
    assert other.wire.log == []


def test_user_code_has_no_authority_and_a_job_it_starts_on_the_dataset_is_refused() -> (
    None
):
    client = Client()
    with write_session(client, job="outer"):
        with without_capability():
            with pytest.raises(NoWriteSessionError):
                current_capability()
            with pytest.raises(LeaseHeldError):
                with write_session(client, job="nested"):
                    raise AssertionError
            with write_session(Client("elsewhere"), job="nested"):
                pass
        commit(client, 1, "a")
    assert client.rows() == {1: "a"}


def test_a_users_thread_has_no_authority() -> None:
    outcome: list[object] = []

    def users() -> None:
        try:
            current_capability()
        except NoWriteSessionError as exc:
            outcome.append(exc)

    with write_session(Client(), job="j"), without_capability():
        for target in (users, lambda: lease_module.copy_context().run(users)):
            thread = threading.Thread(target=target)
            thread.start()
            thread.join(WAIT)
        users()
    assert len(outcome) == 3


def test_a_users_generator_has_no_authority_at_any_step() -> None:
    def users() -> Iterator[bool]:
        while True:
            yield lease_module._located.get() is None

    with write_session(Client(), job="j") as capability:
        generator = users()
        for _ in range(3):
            with without_capability():
                assert next(generator)
            assert current_capability() is capability


def test_the_locator_alone_authorizes_nothing() -> None:
    """A capability is checked for its state, lease and dataset wherever it was found."""
    client = Client()
    with write_session(client, job="j") as capability:
        stale = lease_module.WriteCapability(capability._session, capability.role)
        stale.generation = "table_of_an_earlier_lease"
        token = lease_module._located.set(stale)
        with pytest.raises(NoWriteSessionError):
            commit(client, 1, "x")
        stale.generation, stale.revoked = capability.generation, True
        with pytest.raises(NoWriteSessionError):
            commit(client, 1, "x")
        lease_module._located.reset(token)
    assert client.wire.log == ["create", "drop"]


def test_works_capability_ends_with_the_work() -> None:
    """A thread or copied context the work left behind has no authority while the run goes on."""
    client = Client()
    with ThreadPoolExecutor(1) as pool, write_session(client, job="j") as capability:
        left = capability.submit(pool, lease_module.copy_context).result(WAIT)
        pool.submit(lambda: None).result(WAIT)  # the first future's callbacks have run
        assert left.run(current_capability) is not capability
        with pytest.raises(NoWriteSessionError, match="ended"):
            left.run(commit, client, 1, "orphan")
        commit(client, 2, "run")
    assert client.rows() == {2: "run"}


def test_closing_admits_only_what_is_already_running() -> None:
    client = Client()
    running, closing = threading.Event(), threading.Event()
    run: list[Any] = []

    def work() -> None:
        running.set()
        assert closing.wait(WAIT)
        for attempt in (
            lambda: run[0].require(client),
            lambda: run[0].submit(pool, print),
            lambda: run[0].commit(client).__enter__(),
            lambda: run[0].add_final_flush(print),
        ):
            with pytest.raises(NoWriteSessionError):
                attempt()
        current_capability().submit(pool, commit, client, 1, "subwork")
        commit(client, 2, "work")

    with ThreadPoolExecutor(2) as pool:
        with write_session(client, job="j") as capability:
            run.append(capability)
            session = capability._session
            drain = session._drain

            def noted() -> None:
                closing.set()
                drain()

            session._drain = noted
            done = capability.submit(pool, work)
            assert running.wait(WAIT)
        done.result(0)
    assert client.rows() == {1: "subwork", 2: "work"}
    assert client.wire.log[-1] == "drop"


def test_a_lost_flush_capability_cannot_be_used_after_the_flush() -> None:
    client, kept = Client(), []
    with write_session(client, job="j") as capability:
        capability.add_final_flush(lambda: kept.append(current_capability()))
    with pytest.raises(NoWriteSessionError):
        kept[0].require(client)


def test_another_run_on_the_same_connector_object_is_refused() -> None:
    client = Client()
    refused: list[BaseException | None] = []

    def second_run() -> None:
        with write_session(client, job="second"):
            raise AssertionError

    with write_session(client, job="first") as capability:
        with ThreadPoolExecutor(1) as pool:
            refused.append(pool.submit(second_run).exception(WAIT))
            refused.append(pool.submit(commit, client, 1, "x").exception(WAIT))
        capability.require(client)
        commit(client, 2, "mine")
    assert isinstance(refused[0], LeaseHeldError)
    assert isinstance(refused[1], NoWriteSessionError)
    assert client.rows() == {2: "mine"}


def test_orcapods_threads_write_with_the_session() -> None:
    client = Client()
    with ThreadPoolExecutor(4) as pool:
        with write_session(client, job="j") as capability:
            for key in range(8):
                capability.submit(pool, commit, client, key, "t")
    assert client.rows() == dict.fromkeys(range(8), "t")
    assert client.wire.log[-1] == "drop"


def test_release_waits_for_a_thread_that_is_still_committing() -> None:
    client = Client()
    sending, go = threading.Event(), threading.Event()

    def slow(request: Callable[[], Any]) -> Any:
        sending.set()
        assert go.wait(WAIT)
        return request()

    client.wire.on("write", slow)
    with ThreadPoolExecutor(1) as pool, ThreadPoolExecutor(1) as runner:

        def run() -> None:
            with write_session(client, job="j") as capability:
                capability.submit(pool, commit, client, 1, "late")
                assert sending.wait(WAIT)

        ended = runner.submit(run)
        assert sending.wait(WAIT)
        with pytest.raises(LeaseHeldError):
            take_lease(Client(), "other")
        assert not ended.done()
        go.set()
        ended.result(WAIT)
    assert client.wire.log == ["create", "lookup", "write", "drop"]


def test_work_that_outlives_the_run_is_refused() -> None:
    client = Client()
    with ThreadPoolExecutor(1) as pool:
        with write_session(client, job="j") as capability:
            context = lease_module.copy_context()  # a thread Orcapod did not register
        for attempt in (
            lambda: capability.require(client),
            lambda: commit(client, 1, "x"),
            lambda: capability.submit(pool, print),
        ):
            with pytest.raises(NoWriteSessionError, match="closed"):
                context.run(attempt)
    assert client.wire.log == ["create", "drop"]


def test_the_final_flush_runs_after_the_drain_and_before_release() -> None:
    client = Client()
    client.wire.on("write", lambda request: request())
    with ThreadPoolExecutor(1) as pool:
        with write_session(client, job="j") as capability:
            refused: list[BaseException] = []

            def flush() -> None:
                assert client.rows() == {1: "thread"}
                for attempt in (
                    lambda: capability.require(client),
                    lambda: capability.submit(pool, print),
                ):
                    with pytest.raises(NoWriteSessionError) as error:
                        attempt()
                    refused.append(error.value)
                commit(client, 2, "flush")

            capability.add_final_flush(flush)
            capability.submit(pool, commit, client, 1, "thread")
    assert len(refused) == 2
    assert client.rows() == {1: "thread", 2: "flush"}
    assert client.wire.log[-2:] == ["write", "drop"]


@pytest.mark.parametrize("run_fails", [False, True])
def test_a_flush_that_fails_is_reported_and_the_others_run(run_fails: bool) -> None:
    client, flushed = Client(), []

    def refuse() -> None:
        raise ValueError("3 rows dropped")

    with pytest.raises((ValueError, ZeroDivisionError)) as raised:
        with write_session(client, job="j") as capability:
            capability.add_final_flush(refuse)
            capability.add_final_flush(lambda: flushed.append(1))
            if run_fails:
                raise ZeroDivisionError
    if run_fails:
        assert "3 rows dropped" in raised.value.__notes__[0]
    else:
        assert str(raised.value) == "3 rows dropped"
    assert flushed == [1]
    assert client.holder() is None


def test_a_commit_that_failed_may_still_land_so_the_lease_stays() -> None:
    client = Client()
    landing: list[Callable[[], Any]] = []

    def sent_late(request: Callable[[], Any]) -> Any:
        landing.append(request)
        raise TimeoutError

    client.wire.on("write", sent_late)
    with pytest.raises(LeaseLeftError) as left:
        with write_session(client, job="j"):
            with pytest.raises(TimeoutError):
                commit(client, 1, "late")
    assert left.value.table_id == client.holder().table_id
    landing[0]()  # it does, and nobody else could write
    assert client.rows() == {1: "late"}


def test_a_failed_commit_that_is_settled_does_not_keep_the_lease() -> None:
    client = Client()
    client.wire.on("write", lost_request)
    with write_session(client, job="j") as capability:
        failed: list[lease_module.Commit] = []
        with pytest.raises(TimeoutError):
            with capability.commit(client) as sent:
                failed.append(sent)
                client.put(1, "a")
        assert not failed[0].settled
        commit(client, 1, "a")  # a later success does not settle the earlier request
        assert not failed[0].settled
        failed[0].settle()
    assert client.holder() is None


def test_a_check_that_fails_sends_nothing_and_may_be_repeated() -> None:
    client = Client()
    client.wire.on("lookup", lost_request)
    with write_session(client, job="j"):
        with pytest.raises(TimeoutError):
            commit(client, 1, "a")
        assert "write" not in client.wire.log
        commit(client, 1, "a")
    assert client.holder() is None


# ---------------------------------------------------------------- a cleared live lease (L2)


@pytest.mark.parametrize("second_run", [False, True])
def test_a_cleared_live_lease_is_detected_at_the_next_commit(second_run: bool) -> None:
    client, other = Client(), Client()
    with pytest.raises(LeaseLostError):
        with write_session(client, job="first"):
            commit(client, 1, "first")
            assert clear_lease(other, other.holder().table_id)
            if second_run:
                take_lease(other, "second")
            with pytest.raises(LeaseLostError):
                commit(client, 2, "never")
            # Lost for good: no later commit looks again, nothing takes the lease back.
            log = list(client.wire.log)
            with pytest.raises(LeaseLostError):
                commit(client, 3, "never")
            assert client.wire.log == log
    assert client.rows() == {1: "first"}
    assert (other.holder() is not None) is second_run  # the newer lease is untouched


def test_the_documented_window_a_commit_past_its_check_lands() -> None:
    """4.3: the check is detection. There is no fencing, and this asserts that."""
    client, other = Client(), Client()

    def cleared_while_sending(request: Callable[[], Any]) -> Any:
        assert clear_lease(other, other.holder().table_id)
        with write_session(other, job="second"):
            commit(other, 1, "second")
        return request()

    client.wire.on("write", cleared_while_sending)
    with pytest.raises(LeaseLostError):
        with write_session(client, job="first"):
            with current_capability().commit(client), without_capability():
                client.put(1, "first")
            commit(client, 2, "never")
    assert other.rows() == {1: "first"}  # the old holder overwrote the new one's row
    assert other.holder() is None


# --------------------------------------------------------- other datasets, other addresses


def test_one_dataset_through_two_connectors_is_one_lease() -> None:
    first, second = Client(), Client()
    with write_session(first, job="j"):
        commit(first, 1, "a")
        commit(second, 2, "b")
    assert (first.wire.log + second.wire.log).count("create") == 1
    assert first.rows() == {1: "a", 2: "b"}


def test_one_service_under_two_addresses_is_refused() -> None:
    """No alias is adopted: not between runs, and not within one."""
    first = Client(endpoint="https://one")
    second = Client(endpoint="https://u:pw@two")
    with write_session(first, job="j") as capability:
        with pytest.raises(NoWriteSessionError) as inside:
            capability.require(second)
        with without_capability(), pytest.raises(LeaseHeldError) as between:
            with write_session(second, job="j"):
                raise AssertionError
    for refused in (inside, between):
        assert "pw" not in str(refused.value)  # an endpoint can hold secrets


@pytest.mark.parametrize("order", [["a", "b"], ["b", "a"]])
def test_two_datasets_are_two_sessions_that_never_wait(order: list[str]) -> None:
    """A run holds one dataset; what user code opens on another is its own."""
    outer, inner = order
    with write_session(Client(outer), job="outer"), without_capability():
        with write_session(Client(inner), job="inner"), without_capability():
            for name in order:
                with pytest.raises(LeaseHeldError):
                    take_lease(Client(name), "third")
    assert Client("a").holder() is None
    assert Client("b").holder() is None


# ------------------------------------------------------------------- interrupted and forked


def test_an_interrupted_drain_keeps_the_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    client = Client()
    with pytest.raises(KeyboardInterrupt) as raised:
        with write_session(client, job="j") as capability:

            def interrupted(*_: Any) -> Any:
                raise KeyboardInterrupt

            monkeypatch.setattr(capability._session, "_drain", interrupted)
    assert client.holder().table_id in raised.value.__notes__[0]
    assert "drop" not in client.wire.log


def test_an_interrupted_release_names_what_stays() -> None:
    client = Client()

    def interrupted(request: Callable[[], Any]) -> Any:
        raise KeyboardInterrupt

    client.wire.on("drop", interrupted)
    with pytest.raises(KeyboardInterrupt) as raised:
        with write_session(client, job="j"):
            pass
    assert client.holder().table_id in raised.value.__notes__[0]


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
@pytest.mark.filterwarnings("ignore:.*fork.*:DeprecationWarning")
@pytest.mark.parametrize("ends", ["normally", "raising"])
def test_a_forked_child_cannot_write_and_never_releases(ends: str) -> None:
    client = Client()
    parent = os.getpid()
    with ThreadPoolExecutor(1) as pool, write_session(client, job="j") as capability:
        read, write = os.pipe()
        child = os.fork()
        if child == 0:
            code = 1
            try:
                refused = 0
                for attempt in (
                    lambda: capability.require(client),
                    lambda: commit(client, 1, "child"),
                    lambda: capability.submit(pool, print),
                    lambda: capability.add_final_flush(print),
                ):
                    try:
                        attempt()
                    except NoWriteSessionError:
                        refused += 1
                # leave the body, as a child that unwinds does
                capability._session.close(None if ends == "normally" else SystemExit())
                code = 0 if refused == 4 else 2
            finally:
                os._exit(code)
        os.close(write)
        os.close(read)
        assert os.waitstatus_to_exitcode(os.waitpid(child, 0)[1]) == 0
        assert os.getpid() == parent
        assert client.holder() is not None
        commit(client, 2, "parent")
    assert client.rows() == {2: "parent"}
    assert client.holder() is None


# ------------------------------------------------------------------------------- async runs


def test_an_async_session_writes_from_tasks_and_threads() -> None:
    client = Client()

    async def run() -> None:
        with ThreadPoolExecutor(2) as pool:
            async with async_write_session(client, job="j") as capability:

                async def work(key: int) -> None:
                    await asyncio.wrap_future(
                        current_capability().submit(pool, commit, client, key, "a")
                    )

                for key in range(4):
                    capability.create_task(work(key))

    asyncio.run(run())
    assert client.rows() == dict.fromkeys(range(4), "a")
    assert client.holder() is None
    assert client.wire.log[-1] == "drop"


def test_awaited_user_code_and_its_tasks_have_no_authority() -> None:
    client = Client()

    async def user() -> None:
        with pytest.raises(NoWriteSessionError):
            current_capability()

        async def task() -> None:
            current_capability()

        with pytest.raises(NoWriteSessionError):
            await asyncio.create_task(task())

    async def run() -> None:
        async with async_write_session(client, job="j") as capability:
            with without_capability():
                await user()
            capability.require(client)

    asyncio.run(run())


def test_a_second_async_run_in_the_same_loop_is_refused() -> None:
    client = Client()

    async def second() -> None:
        async with async_write_session(client, job="second"):
            raise AssertionError

    async def run() -> None:
        inside, done = asyncio.Event(), asyncio.Event()

        async def first() -> None:
            async with async_write_session(client, job="first"):
                inside.set()
                await done.wait()

        running = asyncio.create_task(first())
        await inside.wait()
        with pytest.raises(LeaseHeldError):
            await second()
        done.set()
        await running

    asyncio.run(run())
    assert client.holder() is None


def test_a_cancelled_run_waits_for_the_commit_its_thread_is_sending() -> None:
    client = Client()
    sending, go = threading.Event(), threading.Event()

    def slow(request: Callable[[], Any]) -> Any:
        sending.set()
        assert go.wait(WAIT)
        return request()

    client.wire.on("write", slow)

    async def run() -> None:
        with ThreadPoolExecutor(1) as pool:

            async def job() -> None:
                async with async_write_session(client, job="j") as capability:
                    await asyncio.wrap_future(
                        capability.submit(pool, commit, client, 1, "a")
                    )

            running = asyncio.create_task(job())
            await asyncio.to_thread(sending.wait, WAIT)
            for _ in range(3):  # cancelled, and again while it drains
                running.cancel()
                await asyncio.sleep(0)
                await asyncio.sleep(0)
            assert not running.done()
            with pytest.raises(LeaseHeldError):
                take_lease(Client(), "other")
            go.set()
            with pytest.raises(asyncio.CancelledError):
                await running

    asyncio.run(run())
    assert client.rows() == {1: "a"}
    assert client.wire.log == ["create", "lookup", "write", "drop"]


def test_a_cancelled_task_is_waited_for() -> None:
    client = Client()

    async def run() -> None:
        async with async_write_session(client, job="j") as capability:
            started, cleaned = asyncio.Event(), asyncio.Event()

            async def work() -> None:
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    await cleaned.wait()
                    commit(client, 1, "cleanup")

            task = capability.create_task(work())
            await started.wait()
            task.cancel()
            asyncio.get_running_loop().call_soon(cleaned.set)

    asyncio.run(run())
    assert client.rows() == {1: "cleanup"}
    assert client.holder() is None


@pytest.mark.parametrize("outcome", ["taken", "refused"])
def test_a_run_cancelled_while_it_takes_the_lease(outcome: str) -> None:
    client = Client()
    blocker = take_lease(Client(), "other") if outcome == "refused" else None
    asking, go = threading.Event(), threading.Event()

    def slow(request: Callable[[], Any]) -> Any:
        asking.set()
        assert go.wait(WAIT)
        return request()

    client.wire.on("create", slow)

    async def run() -> None:
        async def job() -> None:
            async with async_write_session(client, job="j"):
                raise AssertionError

        running = asyncio.create_task(job())
        await asyncio.to_thread(asking.wait, WAIT)
        running.cancel()
        await asyncio.sleep(0)
        go.set()
        with pytest.raises(asyncio.CancelledError):
            await running

    asyncio.run(run())
    if blocker is not None:
        blocker.check()
    else:
        assert client.holder() is None


def test_a_run_cancelled_while_it_releases() -> None:
    client = Client()
    dropping, go = threading.Event(), threading.Event()

    def slow(request: Callable[[], Any]) -> Any:
        dropping.set()
        assert go.wait(WAIT)
        return request()

    client.wire.on("drop", slow)

    async def run() -> None:
        async def job() -> None:
            async with async_write_session(client, job="j"):
                pass

        running = asyncio.create_task(job())
        await asyncio.to_thread(dropping.wait, WAIT)
        running.cancel()
        await asyncio.sleep(0)
        go.set()
        with pytest.raises(asyncio.CancelledError):
            await running

    asyncio.run(run())
    assert client.holder() is None


def test_an_async_final_flush_and_a_lost_lease() -> None:
    client, other = Client(), Client()

    async def run() -> None:
        async with async_write_session(client, job="j") as capability:
            capability.add_final_flush(lambda: commit(client, 1, "flush"))
            clear_lease(other, other.holder().table_id)

    with pytest.raises(LeaseLostError):
        asyncio.run(run())
    assert client.wire.log.count("write") == 0


def test_a_nested_async_session_is_refused() -> None:
    client = Client()

    async def run() -> None:
        async with async_write_session(client, job="j"):
            with pytest.raises(NestedWriteSessionError):
                async with async_write_session(Client("b"), job="nested"):
                    raise AssertionError

    asyncio.run(run())


def test_a_synchronous_session_refuses_a_task() -> None:
    async def nothing() -> None:
        pass

    with write_session(Client(), job="j") as capability:
        with pytest.raises(NoWriteSessionError, match="asyncio"):
            capability.create_task(nothing())
    with pytest.raises(NoWriteSessionError):
        capability.create_task(nothing())


def test_a_task_that_outlives_the_run_is_refused() -> None:
    client = Client()

    async def run() -> None:
        go = asyncio.Event()

        async def stray() -> None:
            await go.wait()
            with pytest.raises(NoWriteSessionError, match="closed"):
                commit(client, 1, "late")

        async with async_write_session(client, job="j"):
            task = asyncio.create_task(stray())  # not registered with the session
        go.set()
        await task

    asyncio.run(run())
    assert client.wire.log == ["create", "drop"]
