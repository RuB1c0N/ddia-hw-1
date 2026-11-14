import asyncio
from asyncio import sleep
from collections import Counter, deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TypeVar

from .contracts import (
    LatencyBudget,
    Outcome,
    RequestError,
    Response,
    RetryBudget,
    Sender,
    StopReason,
    _Attempt,
    _duration,
    _positive_int,
)


_cancelling: set[asyncio.Task[Response]] = set()


def _finish_cancel(task: asyncio.Task[Response]) -> None:
    _cancelling.discard(task)
    if not task.cancelled():
        task.exception()


async def _send_once(sender: Sender, budget: LatencyBudget, deadline: float) -> _Attempt:
    clock = asyncio.get_running_loop().time
    started_at = clock()
    if budget.subrequest_latency is not None:
        deadline = min(deadline, started_at + budget.subrequest_latency)
    if started_at >= deadline:
        return _Attempt()

    task = asyncio.create_task(sender.send())
    try:
        while (remaining := deadline - clock()) > 0:
            done, _ = await asyncio.wait({task}, timeout=remaining)
            if done and clock() < deadline:
                return _Attempt(task.result())
        return _Attempt()
    finally:
        if not task.done():
            _cancelling.add(task)
            task.add_done_callback(_finish_cancel)
            task.cancel()
        elif not task.cancelled():
            task.exception()


class DefaultClient:
    async def request(self, sender: Sender, budget: LatencyBudget) -> Response:
        clock = asyncio.get_running_loop().time
        deadline = clock() + budget.latency
        result = await _send_once(sender, budget, deadline)
        if result.response is not None and result.outcome is Outcome.SUCCESS:
            return result.response
        reason = StopReason.LATENCY if clock() >= deadline else StopReason.MAX_RETRIES
        raise RequestError(reason, 1, result)


async def _sleep_until(deadline: float) -> None:
    clock = asyncio.get_running_loop().time
    while (remaining := deadline - clock()) > 0:
        await sleep(remaining)


@dataclass(frozen=True, slots=True)
class ExponentialBackoff:
    initial_delay: float = 0.1
    max_delay: float = 1.0

    def __post_init__(self) -> None:
        _duration("initial_delay", self.initial_delay)
        _duration("max_delay", self.max_delay)
        if self.max_delay < self.initial_delay:
            raise ValueError("max_delay must be at least initial_delay")


@dataclass(frozen=True, slots=True)
class _Route:
    sender: Sender
    observe: Callable[[Outcome], None] | None = None


_ERROR_REASONS = {
    Outcome.FAST_ERROR: StopReason.FAST_ERRORS,
    Outcome.FAILURE: StopReason.FAILURES,
    Outcome.TIMEOUT: StopReason.TIMEOUTS,
}


async def _retry(
    select: Callable[[], _Route | None],
    budget: RetryBudget,
    backoff: ExponentialBackoff | None,
) -> Response:
    clock = asyncio.get_running_loop().time
    deadline = clock() + budget.latency
    errors: Counter[Outcome] = Counter()
    limits = {
        Outcome.FAST_ERROR: budget.fast_errors,
        Outcome.FAILURE: budget.failures,
        Outcome.TIMEOUT: budget.timeouts,
    }
    attempts = 0
    last = None
    delay = backoff.initial_delay if backoff else 0.0
    while clock() < deadline:
        route = select()
        if route is None:
            raise RequestError(StopReason.NO_AVAILABLE_REPLICAS, attempts, last)
        attempts += 1
        last = await _send_once(route.sender, budget, deadline)
        if route.observe is not None:
            route.observe(last.outcome)
        if clock() >= deadline:
            break
        if last.response is not None and last.outcome is Outcome.SUCCESS:
            return last.response

        errors[last.outcome] += 1
        limit = limits[last.outcome]
        if limit is not None and errors[last.outcome] >= limit:
            raise RequestError(_ERROR_REASONS[last.outcome], attempts, last)
        if attempts >= budget.max_retries:
            raise RequestError(StopReason.MAX_RETRIES, attempts, last)
        if backoff is not None and last.outcome is not Outcome.FAST_ERROR:
            if clock() + delay >= deadline:
                await _sleep_until(deadline)
                break
            await sleep(delay)
            delay = min(delay * 2, backoff.max_delay)
    raise RequestError(StopReason.LATENCY, attempts, last)


class RetryClient:
    def __init__(self, backoff: ExponentialBackoff | None = None):
        self._backoff = backoff

    async def request(self, sender: Sender, budget: RetryBudget) -> Response:
        route = _Route(sender)
        return await _retry(lambda: route, budget, self._backoff)


Endpoint = TypeVar("Endpoint", bound=Sender)


def _pool(senders: Sequence[Endpoint]) -> tuple[Endpoint, ...]:
    pool = tuple(senders)
    if not pool or len({id(sender) for sender in pool}) != len(pool):
        raise ValueError("provide an ordered pool of distinct senders")
    return pool


class _RoundRobin:
    def __init__(self):
        self._cursor = 0

    def select(self, size: int, eligible: Callable[[int], bool] | None = None) -> int | None:
        for _ in range(size):
            index = self._cursor % size
            self._cursor = (index + 1) % size
            if eligible is None or eligible(index):
                return index
        return None


class RoundRobinClient:
    def __init__(self, backoff: ExponentialBackoff | None = None):
        self._backoff = backoff
        self._rotation = _RoundRobin()

    async def request(self, senders: Sequence[Sender], budget: RetryBudget) -> Response:
        pool = _pool(senders)

        def select() -> _Route:
            index = self._rotation.select(len(pool))
            assert index is not None
            return _Route(pool[index])

        return await _retry(select, budget, self._backoff)


class HedgingClient:
    def __init__(self, hedging_delay: float):
        _duration("hedging_delay", hedging_delay, allow_zero=True)
        self._hedging_delay = hedging_delay

    async def request(self, senders: Sequence[Sender], budget: LatencyBudget) -> Response:
        pool = _pool(senders)
        clock = asyncio.get_running_loop().time
        started_at = clock()
        deadline = started_at + budget.latency
        completed: asyncio.Queue[asyncio.Task[_Attempt]] = asyncio.Queue()
        tasks: list[asyncio.Task[_Attempt]] = []

        def launch(sender: Sender) -> None:
            task = asyncio.create_task(_send_once(sender, budget, deadline))
            tasks.append(task)
            task.add_done_callback(completed.put_nowait)

        async def receive(until: float) -> _Attempt | None:
            while clock() < until:
                phase = asyncio.timeout_at(until)
                try:
                    async with phase:
                        task = await completed.get()
                except TimeoutError:
                    if not phase.expired():
                        raise
                else:
                    return task.result()
            return None

        last = None
        launch(pool[0])
        try:
            hedge_at = started_at + self._hedging_delay
            if len(pool) > 1 and hedge_at < deadline:
                last = await receive(hedge_at)
                if last and last.response is not None and last.outcome is Outcome.SUCCESS:
                    return last.response
                if clock() < deadline:
                    for sender in pool[1:]:
                        launch(sender)
            for _ in range(len(tasks) - int(last is not None)):
                result = await receive(deadline)
                if result is None:
                    break
                last = result
                if last.response is not None and last.outcome is Outcome.SUCCESS:
                    return last.response
            await _sleep_until(deadline)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        raise RequestError(StopReason.LATENCY, len(tasks), last)


@dataclass(frozen=True, slots=True)
class Replica:
    key: str
    sender: Sender

    def __post_init__(self) -> None:
        if not isinstance(self.key, str) or not self.key.strip():
            raise ValueError("replica key must be a nonempty string")

    async def send(self) -> Response:
        return await self.sender.send()


@dataclass(slots=True)
class _Circuit:
    history: deque[bool]
    opens_until: float | None = None
    errors: int = 0

    def record(self, failed: bool) -> None:
        if len(self.history) == self.history.maxlen:
            self.errors -= self.history[0]
        self.history.append(failed)
        self.errors += failed


class CircuitBreakingClient:
    def __init__(
        self,
        window: int,
        failure_threshold: float,
        cooldown: float,
        backoff: ExponentialBackoff | None = None,
    ):
        _positive_int("window", window)
        _duration("failure_threshold", failure_threshold, allow_zero=True)
        _duration("cooldown", cooldown)
        if failure_threshold > 1:
            raise ValueError("failure_threshold must be at most 1")
        self._window = window
        self._threshold = failure_threshold
        self._cooldown = cooldown
        self._backoff = backoff
        self._circuits: dict[str, _Circuit] = {}
        self._rotation = _RoundRobin()

    async def request(self, senders: Sequence[Replica], budget: RetryBudget) -> Response:
        pool = _pool(senders)
        if len({replica.key for replica in pool}) != len(pool):
            raise ValueError("replica keys must be distinct")
        if len({id(replica.sender) for replica in pool}) != len(pool):
            raise ValueError("replica senders must be distinct")
        clock = asyncio.get_running_loop().time

        def eligible(index: int) -> bool:
            circuit = self._circuits.get(pool[index].key)
            return circuit is None or circuit.opens_until is None or clock() >= circuit.opens_until

        def select() -> _Route | None:
            index = self._rotation.select(len(pool), eligible)
            if index is None:
                return None
            replica = pool[index]
            circuit = self._circuits.get(replica.key)
            if circuit is None or circuit.opens_until is not None:
                circuit = _Circuit(deque(maxlen=self._window))
                self._circuits[replica.key] = circuit

            def observe(outcome: Outcome) -> None:
                if circuit.opens_until is not None:
                    return
                circuit.record(outcome is not Outcome.SUCCESS)
                if len(circuit.history) == self._window and circuit.errors / self._window > self._threshold:
                    circuit.opens_until = clock() + self._cooldown

            return _Route(replica, observe)

        return await _retry(select, budget, self._backoff)
