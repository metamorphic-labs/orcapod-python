"""The one-writer lease of a SpiralDB store, and the write session that holds it.

Private, and used by no job yet (ITL-1452, part of ITL-1446). The design is sections 4 and 8
of the ITL-1446 specification.

SpiralDB has no data-commit conflict: of two writers of one key the last commit wins, silently.
So a run holds a **lease** on each dataset it may write: the table ``orcapod_lease_v1``, created
exclusively. The owner is the name of that table's one key column, so it is part of the atomic
create, and SpiralDB's own id of the table is the lease.

* Nothing makes a lease expire: no age, process id or host. A lease left behind is cleared by a
  person, with ``clear_lease`` and the table id the refusal names.
* Leases are dropped by table id, never by name, so a late or repeated drop cannot touch a
  newer lease. Only SpiralDB's answer about that id (done, or 404) says it is gone.
* The check before a commit is detection, not fencing: if a live holder's lease is cleared, a
  commit already past its check still lands.

A **session** (``write_session``, ``async_write_session``) is one run's lease of one dataset.
What may write is an explicit ``WriteCapability``, checked at every admission for its process,
its session's state, the lease it was made for, and the dataset. The run has one, and each piece
of work registered with ``submit`` or ``create_task`` has its own, revoked when the work ends.
A context variable only *locates* the caller's capability (``current_capability``); it is taken
away around user code (``without_capability``).

Held back until decided (second review of the design): leases of several datasets in one run,
one dataset reached under two addresses, any automatic clearing, and any claim that a timeout
proves a request can no longer land. A second address of a leased dataset is simply refused.
"""

from __future__ import annotations

import asyncio
import os
import re
import threading
import time
from collections.abc import AsyncIterator, Callable, Coroutine, Iterator
from concurrent.futures import Executor, Future
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar, copy_context
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, NamedTuple, TypeVar

import pyarrow as pa

from orcapod.databases.spiraldb_connector import SpiralDBTableNotFoundError

if TYPE_CHECKING:
    from orcapod.databases.spiraldb_connector import SpiralDBConnector

T = TypeVar("T")

LEASE_TABLE_NAME = "orcapod_lease_v1"
# How often one release sends its drop before it gives up. A repeated drop of one id is safe.
_DROP_ATTEMPTS = 3
_JOB_LABEL_LENGTH = 40


class SpiralLeaseError(RuntimeError):
    """Base of the errors about a SpiralDB store's lease."""


class LeaseHeldError(SpiralLeaseError):
    """The dataset's lease belongs to another run.

    Attributes:
        table_id: The holder's lease table id, or ``None`` if it was gone when looked up.
    """

    def __init__(self, dataset: str, owner: str | None, table_id: str | None) -> None:
        self.table_id = table_id
        if table_id is None:
            detail = "it was held when this run started and released since. Run again."
        else:
            detail = f"held by {owner!r}, lease table id {table_id!r}. {_how_to_clear(table_id)}"
        super().__init__(
            f"SpiralDB dataset {dataset} is in use by another run: {detail}"
        )


class LeaseUncertainError(SpiralLeaseError):
    """Whether this run holds the lease could not be found out, so it does not."""

    def __init__(self, dataset: str, owner: str) -> None:
        super().__init__(
            f"The lease of SpiralDB dataset {dataset} could not be taken: the reply to the "
            "request was lost and the lease table could not be read back as this run's. If "
            f"a later run is refused by the owner {owner!r}, the request landed late: clear "
            "that lease with its table id."
        )


class LeaseLostError(SpiralLeaseError):
    """The lease table is no longer the one this run created."""

    def __init__(self, dataset: str, table_id: str) -> None:
        super().__init__(
            f"This run's lease of SpiralDB dataset {dataset} (table id {table_id!r}) is "
            "gone: it was cleared while the run was alive. Nothing more is written. A "
            "commit of this run that was already being sent may have landed."
        )


class LeaseLeftError(SpiralLeaseError):
    """A lease may have been left behind; a person clears it.

    Attributes:
        table_id: The id of the lease table.
    """

    def __init__(self, dataset: str, table_id: str, reason: str) -> None:
        self.table_id = table_id
        super().__init__(
            f"The lease of SpiralDB dataset {dataset} may be left behind: {reason}. "
            + _how_to_clear(table_id)
        )


class NoWriteSessionError(SpiralLeaseError):
    """A write was attempted without the authority of an open write session."""


class NestedWriteSessionError(SpiralLeaseError):
    """A write session was opened inside another one."""


def _how_to_clear(table_id: str) -> str:
    """Return the instruction for clearing the lease ``table_id``."""
    return (
        f"If that run is gone, clear its lease with clear_lease({table_id!r}), after making "
        "sure that its process has ended and that no request of it can still reach "
        "SpiralDB. A timeout in the client does not show that."
    )


def _dataset_label(connector: SpiralDBConnector) -> str:
    """Return the dataset as errors name it (never the endpoint: it can hold secrets)."""
    return f"{connector._project_id}/{connector._dataset}"  # pyright: ignore[reportPrivateUsage]  # the lease is the connector's own


def _dataset_key(connector: SpiralDBConnector) -> tuple[str, str, str]:
    """Return ``(endpoint, project, dataset)``."""
    overrides = connector._overrides or {}  # pyright: ignore[reportPrivateUsage]  # the lease is the connector's own
    return (
        overrides.get("server.url", ""),
        connector._project_id,  # pyright: ignore[reportPrivateUsage]  # the lease is the connector's own
        connector._dataset,  # pyright: ignore[reportPrivateUsage]  # the lease is the connector's own
    )


# --------------------------------------------------------------------------- the lease


def _new_token() -> str:
    """Return a new UUIDv7 as 32 hex digits, different in a forked copy of this process.

    The random bits come from the operating system for every token: a generator held in
    process memory would give a parent and its fork the same token.
    """
    raw = bytearray((time.time_ns() // 1_000_000).to_bytes(6, "big") + os.urandom(10))
    raw[6] = 0x70 | (raw[6] & 0x0F)
    raw[8] = 0x80 | (raw[8] & 0x3F)
    return raw.hex()


def _owner_text(job: str) -> str:
    """Return a new owner text: ``own-<token>-<utc start>-<pid>-<job>``."""
    label = re.sub(r"[^a-z0-9_]+", "_", job.lower())[:_JOB_LABEL_LENGTH]
    started = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"own-{_new_token()}-{started}-{os.getpid()}-{label}"


class _Holder(NamedTuple):
    """The lease table found under the name: its owner text and its table id."""

    owner: str
    table_id: str


def _current_holder(connector: SpiralDBConnector) -> _Holder | None:
    """Look the lease table up by name.

    Returns:
        Its owner and id, or ``None`` if there is no such table. Any other failure is
        raised: it is never read as "absent".
    """
    try:
        table = connector.lookup_table(LEASE_TABLE_NAME)
    except SpiralDBTableNotFoundError:
        return None
    return _Holder(",".join(table.key_columns), table.table_id)


class Lease:
    """The lease of one dataset, held by this run."""

    def __init__(self, connector: SpiralDBConnector, owner: str, table_id: str) -> None:
        """Hold the lease table ``table_id`` of ``connector``'s dataset."""
        self.connector = connector
        self.owner = owner
        self.table_id = table_id

    @property
    def dataset(self) -> str:
        """The dataset as errors name it."""
        return _dataset_label(self.connector)

    def check(self) -> None:
        """Raise ``LeaseLostError`` unless the lease table is still this run's."""
        holder = _current_holder(self.connector)
        if holder is None or holder.table_id != self.table_id:
            raise LeaseLostError(self.dataset, self.table_id)

    def release(self) -> None:
        """Drop the lease table, or raise ``LeaseLeftError``."""
        _drop_lease_table(self.connector, self.table_id)


def take_lease(connector: SpiralDBConnector, job: str) -> Lease:
    """Take the lease of ``connector``'s dataset: one claim, refused at once or held.

    One claim has one token. However often the client's transport delivers its create, the
    replies are settled against that token (``_settle_create``).

    Args:
        connector: The dataset to lease.
        job: A name shown to a run that is refused.

    Returns:
        The lease.

    Raises:
        LeaseHeldError: If another run holds it.
        LeaseUncertainError: If the reply was lost and the lease cannot be shown to be
            this run's.
    """
    owner = _owner_text(job)
    try:
        table = connector._project.create_table(  # pyright: ignore[reportPrivateUsage]  # the lease is the connector's own
            f"{connector._dataset}.{LEASE_TABLE_NAME}",  # pyright: ignore[reportPrivateUsage]  # the lease is the connector's own
            key_schema=pa.schema([(owner, pa.int64())]),
            exist_ok=False,
        )
        return Lease(connector, owner, str(table.table_id))
    except Exception as exc:
        return _settle_create(connector, owner, exc)
    except BaseException as interrupt:
        _undo_interrupted_create(connector, owner, interrupt)
        raise


def _settle_create(connector: SpiralDBConnector, owner: str, error: Exception) -> Lease:
    """Find out, by one look at the name, what a failed create did.

    A 409 and a lost reply are settled the same way, because the client's own retry turns
    a lost reply into a 409.
    """
    dataset = _dataset_label(connector)
    refused = _has_status(error, 409)
    try:
        holder = _current_holder(connector)
    except Exception as lookup_error:
        lookup_error.add_note(f"The create had failed with: {error!r}")
        raise LeaseUncertainError(dataset, owner) from lookup_error
    if holder is None:
        if refused:
            raise LeaseHeldError(dataset, None, None) from error
        raise LeaseUncertainError(dataset, owner) from error
    if holder.owner == owner:
        return Lease(connector, owner, holder.table_id)
    raise LeaseHeldError(dataset, holder.owner, holder.table_id) from error


def _undo_interrupted_create(
    connector: SpiralDBConnector, owner: str, interrupt: BaseException
) -> None:
    """Drop the lease an interrupted create may have made; note it if that fails."""
    try:
        holder = _current_holder(connector)
        if holder is not None and holder.owner == owner:
            _drop_lease_table(connector, holder.table_id)
    except Exception as exc:
        interrupt.add_note(
            f"A lease of SpiralDB dataset {_dataset_label(connector)} with the owner "
            f"{owner!r} may be left behind ({exc!r})."
        )


def _drop_lease_table(connector: SpiralDBConnector, table_id: str) -> None:
    """Drop the lease table with exactly this id; already gone is success.

    Raises:
        LeaseLeftError: If it could not be settled whether the table is gone.
    """
    error: Exception | None = None
    for _ in range(_DROP_ATTEMPTS):
        try:
            connector._spiral.core.drop_table(table_id=table_id)  # pyright: ignore[reportPrivateUsage]  # the lease is the connector's own
            return
        except Exception as exc:
            if _has_status(exc, 404):
                return
            error = exc
    raise LeaseLeftError(
        _dataset_label(connector), table_id, f"dropping it failed ({error!r})"
    ) from error


def clear_lease(connector: SpiralDBConnector, table_id: str) -> bool:
    """Drop a lease a run left behind. Called by a person, never by Orcapod.

    The caller has made sure that the holder's process has ended and that no request of it
    is in flight; clearing a live holder's lease lets a second run write.

    Args:
        connector: The dataset whose lease is cleared.
        table_id: The lease table id the refusal named.

    Returns:
        Whether the dataset's lease had that id. If not, nothing is dropped.

    Raises:
        LeaseLeftError: If it could not be settled whether the table is gone.
    """
    holder = _current_holder(connector)
    if holder is None or holder.table_id != table_id:
        return False
    _drop_lease_table(connector, table_id)
    return True


def _has_status(error: Exception, status: int) -> bool:
    """Whether ``error`` is SpiralDB's reply ``status``.

    pyspiral 0.16.33 raises a plain ``Exception`` whose text starts with the status from its
    native calls (seen live: ``409 Conflict: ...``, ``404 Not Found: ...``) and
    ``SpiralHTTPError`` with a ``code`` from its Python calls.
    """
    return getattr(error, "code", None) == status or str(error).startswith(f"{status} ")


# ------------------------------------------------------------------------- the session


class _State(Enum):
    """What a session still admits."""

    OPEN = "open"  # every live capability
    DRAINING = "draining"  # registered work that is still running, only
    FLUSHING = "flushing"  # the final flush, only
    CLOSED = "closed"  # nothing


class _Role(Enum):
    """Whose capability it is."""

    RUN = "run"
    WORK = "work"
    FLUSH = "flush"


_ADMITTED = {
    _State.OPEN: {_Role.RUN, _Role.WORK},
    _State.DRAINING: {_Role.WORK},
    _State.FLUSHING: {_Role.FLUSH},
    _State.CLOSED: set[_Role](),
}


class Commit:
    """One commit to a store table, made under a checked lease."""

    def __init__(self) -> None:
        """Start with an unknown outcome."""
        self._settled = False

    def settle(self) -> None:
        """Say that no request of this commit can still reach SpiralDB.

        Called for a commit that failed, by a writer that knows: it was never sent, or
        SpiralDB answered it with a refusal.
        """
        self._settled = True

    @property
    def settled(self) -> bool:
        """Whether no request of this commit can still reach SpiralDB."""
        return self._settled


class _Session:
    """One run's lease of one dataset, and what is running under it."""

    def __init__(self, connector: SpiralDBConnector, job: str, is_async: bool) -> None:
        """Describe the session; nothing is leased yet."""
        self.connector = connector
        self.is_async = is_async
        self.dataset_key = _dataset_key(connector)
        self.pid = os.getpid()
        self.final_flushes: list[Callable[[], None]] = []
        self._job = job
        self._changed = threading.Condition()
        self._state = _State.OPEN
        self._running = 0
        self._commits: list[Commit] = []
        self._lease: Lease | None = None
        self._lost: LeaseLostError | None = None

    @property
    def lease(self) -> Lease:
        """The lease; only of an opened session."""
        assert self._lease is not None
        return self._lease

    # ---- admission

    def admit(self, capability: WriteCapability, *, work: bool) -> None:
        """Check ``capability``, and in the same step count new running work.

        Args:
            capability: What the caller acts with.
            work: Whether something starts running that ends with ``end``.

        Raises:
            NoWriteSessionError: If the capability does not allow it now.
        """
        # The process first: after a fork a lock may be held by a thread that is gone.
        if os.getpid() != self.pid:
            raise NoWriteSessionError(
                "A forked process cannot write with its parent's write session."
            )
        with self._changed:
            if self._state is _State.CLOSED:
                raise NoWriteSessionError(
                    "The write session is closed: the run it belongs to has ended."
                )
            if (
                capability.revoked
                or capability.role not in _ADMITTED[self._state]
                or capability.generation != self.lease.table_id
            ):
                raise NoWriteSessionError(
                    "The write session no longer admits this work: what it belongs to "
                    "has ended."
                )
            if work:
                self._running += 1

    def end(self, revoke: WriteCapability | None = None) -> None:
        """Count one piece of running work less, and revoke its capability."""
        with self._changed:
            if revoke is not None:
                revoke.revoked = True
            self._running -= 1
            self._changed.notify_all()

    def start_commit(self) -> Commit:
        """Check the lease and return a commit of unknown outcome."""
        if self._lost is not None:
            raise self._lost
        try:
            self.lease.check()
        except LeaseLostError as exc:
            self._lost = exc
            raise
        commit = Commit()
        with self._changed:
            self._commits.append(commit)
        return commit

    # ---- open and close

    def open(self) -> None:
        """Take the lease."""
        self._lease = take_lease(self.connector, self._job)

    def close(self, error: BaseException | None) -> None:
        """Close admission, wait for the run's work, flush, and release.

        Args:
            error: What the run raised. Failures here become notes on it; without one,
                the first failure is raised.
        """
        if os.getpid() != self.pid:
            return  # a forked child ends its copy of the run: the lease is the parent's
        failures: list[Exception] = []
        try:
            with self._changed:
                self._state = _State.DRAINING
            self._drain()
            failures += self._flush()
            with self._changed:
                self._state = _State.CLOSED
            self._release()
        except LeaseLeftError as exc:
            failures.append(exc)
        except BaseException as interrupt:
            with self._changed:
                self._state = _State.CLOSED
            interrupt.add_note(str(self._left("closing the write session failed")))
            raise
        if self._lost is not None and self._lost is not error:
            failures = [f for f in failures if f is not self._lost]
            failures.insert(0, self._lost)
        if error is not None:
            for failure in failures:
                error.add_note(f"Closing the write session: {failure}")
        elif failures:
            first, *rest = failures
            for failure in rest:
                first.add_note(f"Closing the write session: {failure}")
            raise first

    def _drain(self) -> None:
        """Wait until nothing is running; admit nothing but the final flush from then on."""
        with self._changed:
            self._changed.wait_for(lambda: self._running == 0)
            self._state = _State.FLUSHING

    def _flush(self) -> list[Exception]:
        """Run every final flush, with a capability of its own; return what failed."""
        failures: list[Exception] = []
        capability = WriteCapability(self, _Role.FLUSH)
        token = _located.set(capability)
        try:
            for flush in self.final_flushes:
                try:
                    flush()
                except Exception as exc:
                    failures.append(exc)
        finally:
            _located.reset(token)
            with self._changed:
                capability.revoked = True
                # a commit a flush handed to another thread
                self._changed.wait_for(lambda: self._running == 0)
        return failures

    def _release(self) -> None:
        """Drop the lease table, unless a request of this run may still land."""
        if not all(commit.settled for commit in self._commits):
            raise self._left(
                "a commit failed and its request may still reach the service"
            )
        self.lease.release()

    def _left(self, reason: str) -> LeaseLeftError:
        """Return the error that the lease stays, for ``reason``."""
        return LeaseLeftError(self.lease.dataset, self.lease.table_id, reason)


class WriteCapability:
    """The right to write to one leased dataset: a run's, or one piece of its work's.

    Attributes:
        generation: The table id of the lease it was made for.
        role: Whose it is.
        revoked: Whether what it belonged to has ended.
    """

    def __init__(self, session: _Session, role: _Role) -> None:
        """Make a capability of ``session``'s current lease."""
        self._session = session
        self.generation = session.lease.table_id
        self.role = role
        self.revoked = False

    def require(self, connector: SpiralDBConnector) -> None:
        """Raise unless the holder may write to ``connector``'s dataset now."""
        self._require_dataset(connector)
        self._session.admit(self, work=False)

    @contextmanager
    def commit(self, connector: SpiralDBConnector) -> Iterator[Commit]:
        """Run one commit: check the lease, then let the caller send it.

        The session does not close before the body has ended. If the body raises, the
        commit's request may still land: unless the caller settles it, the session leaves
        its lease behind.

        Args:
            connector: The dataset the commit goes to.

        Yields:
            The commit.

        Raises:
            NoWriteSessionError: If this capability does not allow it.
            LeaseLostError: If the lease was cleared. Every later commit raises it too.
        """
        self._require_dataset(connector)
        self._session.admit(self, work=True)
        try:
            commit = self._session.start_commit()
            yield commit
            commit.settle()
        finally:
            self._session.end()

    def submit(
        self, executor: Executor, function: Callable[..., T], /, *args: Any
    ) -> Future[T]:
        """Run Orcapod's own ``function`` on ``executor`` with a capability of its own.

        The session does not close before the function has ended, also if its future, or
        a task awaiting it, is cancelled while it runs.
        """
        self._session.admit(self, work=True)
        capability = WriteCapability(self._session, self._work_role)
        try:
            context = copy_context()
            context.run(_located.set, capability)
            future = executor.submit(context.run, function, *args)
        except BaseException:
            self._session.end(revoke=capability)
            raise
        future.add_done_callback(lambda _: self._session.end(revoke=capability))
        return future

    def create_task(self, coroutine: Coroutine[Any, Any, T]) -> asyncio.Task[T]:
        """Run Orcapod's own ``coroutine`` as a task with a capability of its own.

        The session does not close before the task has ended, also if it is cancelled.
        """
        try:
            if not self._session.is_async:
                raise NoWriteSessionError(
                    "A synchronous write session cannot wait for an asyncio task."
                )
            self._session.admit(self, work=True)
        except BaseException:
            coroutine.close()
            raise
        capability = WriteCapability(self._session, self._work_role)
        try:
            context = copy_context()
            context.run(_located.set, capability)
            task = asyncio.get_running_loop().create_task(coroutine, context=context)
        except BaseException:
            self._session.end(revoke=capability)
            coroutine.close()
            raise
        task.add_done_callback(lambda _: self._session.end(revoke=capability))
        return task

    def add_final_flush(self, flush: Callable[[], None]) -> None:
        """Call ``flush`` once everything else of the run has ended, before release."""
        self._session.admit(self, work=True)
        try:
            self._session.final_flushes.append(flush)
        finally:
            self._session.end()

    @property
    def _work_role(self) -> _Role:
        """The role of work this capability starts."""
        return _Role.FLUSH if self.role is _Role.FLUSH else _Role.WORK

    def _require_dataset(self, connector: SpiralDBConnector) -> None:
        """Raise unless ``connector``'s dataset is the leased one."""
        if _dataset_key(connector) != self._session.dataset_key:
            raise NoWriteSessionError(
                "The write session holds no lease of SpiralDB dataset "
                f"{_dataset_label(connector)} at that address."
            )


# Where the caller's capability is found. It authorizes nothing: ``_Session.admit`` does.
_located: ContextVar[WriteCapability | None] = ContextVar(
    "orcapod_spiral_write_capability", default=None
)


def current_capability() -> WriteCapability:
    """Return the caller's capability.

    Raises:
        NoWriteSessionError: If it has none.
    """
    capability = _located.get()
    if capability is None:
        raise NoWriteSessionError(
            "A SpiralDB store is written only inside a run or store.writing()."
        )
    return capability


@contextmanager
def without_capability() -> Iterator[None]:
    """Run user code without the run's capability: each call, await and generator step.

    User code may then open a session of its own on another dataset.
    """
    token = _located.set(None)
    try:
        yield
    finally:
        _located.reset(token)


def _new_session(connector: SpiralDBConnector, job: str, is_async: bool) -> _Session:
    """Return a session to open, unless the caller has one already."""
    if _located.get() is not None:
        raise NestedWriteSessionError(
            "A write session cannot be opened inside another one."
        )
    return _Session(connector, job, is_async)


@contextmanager
def write_session(
    connector: SpiralDBConnector, *, job: str
) -> Iterator[WriteCapability]:
    """Hold the lease of ``connector``'s dataset for the body: one run.

    Args:
        connector: The dataset the run writes.
        job: A name shown to a run that is refused.

    Yields:
        The run's capability.

    Raises:
        LeaseHeldError: If the dataset is in use.
        LeaseUncertainError: If the lease could not be shown to be this run's.
        NestedWriteSessionError: If the caller already has a capability.
    """
    session = _new_session(connector, job, is_async=False)
    session.open()
    capability = WriteCapability(session, _Role.RUN)
    token = _located.set(capability)
    try:
        try:
            yield capability
        except BaseException as exc:
            session.close(exc)
            raise
        session.close(None)
    finally:
        _located.reset(token)


@asynccontextmanager
async def async_write_session(
    connector: SpiralDBConnector, *, job: str
) -> AsyncIterator[WriteCapability]:
    """``write_session`` for an asynchronous run.

    Cancelling the run does not interrupt taking, draining or releasing: a thread that is
    committing goes on after its task is cancelled, so the lease is kept until it ends.
    The cancellation is raised afterwards.
    """
    session = _new_session(connector, job, is_async=True)
    error, cancelled = await _uncancelled(session.open)
    if error is not None:
        if cancelled is not None:
            raise cancelled from error
        raise error
    if cancelled is not None:
        await _close_async(session, cancelled)
        raise cancelled
    capability = WriteCapability(session, _Role.RUN)
    token = _located.set(capability)
    try:
        try:
            yield capability
        except BaseException as exc:
            await _close_async(session, exc)
            raise
        await _close_async(session, None)
    finally:
        _located.reset(token)


async def _close_async(session: _Session, raised: BaseException | None) -> None:
    """Close ``session`` without being interrupted by a cancellation."""
    if os.getpid() != session.pid:
        return  # a forked child: see ``_Session.close``
    error, cancelled = await _uncancelled(lambda: session.close(raised))
    if cancelled is not None and raised is None:
        if error is not None:
            cancelled.add_note(f"Closing the write session: {error}")
        raise cancelled
    if error is not None:
        raise error


async def _uncancelled(
    function: Callable[[], None],
) -> tuple[BaseException | None, asyncio.CancelledError | None]:
    """Run ``function`` in a thread and wait for its end, whatever is cancelled.

    Returns:
        What it raised, and the cancellation that arrived meanwhile.
    """
    loop = asyncio.get_running_loop()
    ended: asyncio.Future[BaseException | None] = loop.create_future()
    context = copy_context()

    def run() -> None:
        error: BaseException | None = None
        try:
            context.run(function)
        except BaseException as exc:
            error = exc
        try:
            loop.call_soon_threadsafe(ended.set_result, error)
        except RuntimeError:
            pass  # the loop was closed: nobody waits any more

    # A daemon: if the loop is stopped, waiting for its tasks must not keep the process
    # alive. The lease then stays, as after a crash.
    threading.Thread(target=run, name="orcapod-spiral-session", daemon=True).start()
    cancelled: asyncio.CancelledError | None = None
    while not ended.done():
        try:
            await asyncio.wait({ended})
        except asyncio.CancelledError as exc:
            cancelled = exc
    return ended.result(), cancelled
