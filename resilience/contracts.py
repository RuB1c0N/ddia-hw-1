from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from math import isfinite
from typing import Protocol, TypeVar


def _duration(name: str, value: float, *, allow_zero: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    if not isfinite(value) or value < 0 or (value == 0 and not allow_zero):
        raise ValueError(f"{name} must be finite and {'nonnegative' if allow_zero else 'positive'}")


def _positive_int(name: str, value: int) -> None:
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be a positive integer")


class Outcome(Enum):
    SUCCESS = 2
    FAST_ERROR = 4
    FAILURE = 5
    TIMEOUT = 0


@dataclass(frozen=True, slots=True)
class Response:
    status: int
    body: bytes = b""

    def __post_init__(self) -> None:
        if type(self.status) is not int or self.status // 100 not in (2, 4, 5):
            raise ValueError("status must be 2xx, 4xx or 5xx")

    @property
    def outcome(self) -> Outcome:
        return Outcome(self.status // 100)


class Sender(Protocol):
    async def send(self) -> Response: ...


@dataclass(frozen=True, slots=True)
class LatencyBudget:
    latency: float
    subrequest_latency: float | None = None

    def __post_init__(self) -> None:
        _duration("latency", self.latency)
        if self.subrequest_latency is not None:
            _duration("subrequest_latency", self.subrequest_latency)


@dataclass(frozen=True, slots=True)
class RetryBudget(LatencyBudget):
    max_retries: int = 3
    fast_errors: int | None = None
    failures: int | None = None
    timeouts: int | None = None

    def __post_init__(self) -> None:
        LatencyBudget.__post_init__(self)
        _positive_int("max_retries", self.max_retries)
        for name in ("fast_errors", "failures", "timeouts"):
            limit = getattr(self, name)
            if limit is not None and (type(limit) is not int or limit < 0):
                raise ValueError(f"{name} must be a nonnegative integer")


Budget = TypeVar("Budget", bound=LatencyBudget, contravariant=True)
Endpoint = TypeVar("Endpoint", bound=Sender, contravariant=True)


class SingleClient(Protocol[Budget]):
    async def request(self, sender: Sender, budget: Budget) -> Response: ...


class MultiClient(Protocol[Endpoint, Budget]):
    async def request(self, senders: Sequence[Endpoint], budget: Budget) -> Response: ...


class StopReason(str, Enum):
    LATENCY = "latency"
    MAX_RETRIES = "max_retries"
    FAST_ERRORS = "fast_errors"
    FAILURES = "failures"
    TIMEOUTS = "timeouts"
    NO_AVAILABLE_REPLICAS = "no_available_replicas"


@dataclass(frozen=True, slots=True)
class _Attempt:
    response: Response | None = None

    @property
    def outcome(self) -> Outcome:
        return self.response.outcome if self.response is not None else Outcome.TIMEOUT


class RequestError(Exception):
    def __init__(self, reason: StopReason, attempts: int, last: _Attempt | None):
        self.reason = reason
        self.attempts = attempts
        self.last_outcome = last.outcome if last else None
        self.last_response = last.response if last else None
        super().__init__(f"{reason.value} after {attempts} attempt(s)")
