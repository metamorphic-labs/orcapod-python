from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Generator
from contextlib import contextmanager
from functools import partial
from typing import TYPE_CHECKING, Any

from orcapod.protocols import core_protocols as cp

if TYPE_CHECKING:
    from orcapod.pipeline.pod_invocation import PodInvocation


class BasicTrackerManager:
    def __init__(self) -> None:
        self._active_trackers: list[cp.TrackerProtocol] = []
        self._active = True

    def set_active(self, active: bool = True) -> None:
        """Set the active state of the tracker manager."""
        self._active = active

    def register_tracker(self, tracker: cp.TrackerProtocol) -> None:
        """Register a new tracker in the system."""
        if tracker not in self._active_trackers:
            self._active_trackers.append(tracker)

    def deregister_tracker(self, tracker: cp.TrackerProtocol) -> None:
        """Remove a tracker from the system."""
        if tracker in self._active_trackers:
            self._active_trackers.remove(tracker)

    def get_active_trackers(self) -> list[cp.TrackerProtocol]:
        """Get the list of active trackers."""
        if not self._active:
            return []
        return [t for t in self._active_trackers if t.is_active()]

    def record_operator_pod_invocation(
        self,
        pod: cp.OperatorPodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        """Record the invocation of a pod in the tracker."""
        from orcapod.pipeline.pod_invocation import OperatorInvocation

        self._record(partial(OperatorInvocation, pod, upstreams, label))

    def record_function_pod_invocation(
        self,
        pod: cp.FunctionPodProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None:
        """Record the invocation of a data function to the tracker."""
        from orcapod.pipeline.pod_invocation import FunctionInvocation

        self._record(partial(FunctionInvocation, pod, (input_stream,), label))

    def record_side_effect_pod_invocation(
        self,
        pod: cp.SideEffectPodProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None:
        """Record a side-effect pod invocation in all active trackers."""
        from orcapod.pipeline.pod_invocation import SideEffectInvocation

        self._record(partial(SideEffectInvocation, pod, (input_stream,), label))

    def _record(self, make_invocation: Callable[[], PodInvocation]) -> None:
        """Prepare every active tracker's recording before committing in order."""
        trackers = self.get_active_trackers()
        if not trackers:
            return
        invocation = make_invocation()
        recordings = [tracker.prepare_invocation(invocation) for tracker in trackers]
        for record in recordings:
            record()

    @contextmanager
    def no_tracking(self) -> Generator[None, Any, None]:
        original_state = self._active
        self.set_active(False)
        try:
            yield
        finally:
            self.set_active(original_state)


class AutoRegisteringContextBasedTracker(ABC):
    def __init__(
        self, tracker_manager: cp.TrackerManagerProtocol | None = None
    ) -> None:
        self._tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER
        self._active = False

    def set_active(self, active: bool = True) -> None:
        if active:
            self._tracker_manager.register_tracker(self)
        else:
            self._tracker_manager.deregister_tracker(self)
        self._active = active

    def is_active(self) -> bool:
        return self._active

    @abstractmethod
    def prepare_invocation(self, invocation: PodInvocation) -> Callable[[], None]: ...

    @abstractmethod
    def record_operator_pod_invocation(
        self,
        pod: cp.OperatorPodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None: ...

    @abstractmethod
    def record_function_pod_invocation(
        self,
        pod: cp.FunctionPodProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None: ...

    def __enter__(self):
        self.set_active(True)
        return self

    def __exit__(self, exc_type, exc_val, ext_tb):
        self.set_active(False)


DEFAULT_TRACKER_MANAGER = BasicTrackerManager()
