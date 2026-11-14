import asyncio
import unittest
from collections import deque
from unittest.mock import AsyncMock, call, patch

from resilience import (
    CircuitBreakingClient,
    DefaultClient,
    ExponentialBackoff,
    HedgingClient,
    LatencyBudget,
    Outcome,
    Replica,
    RequestError,
    Response,
    RetryBudget,
    RetryClient,
    RoundRobinClient,
    StopReason,
)


OK = Response(200, b"payload")
BAD_REQUEST = Response(400)
UNAVAILABLE = Response(503)


class ScriptedSender:
    def __init__(self, *steps, name="server", order=None):
        self.steps = deque(steps)
        self.name = name
        self.order = order
        self.calls = 0
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def send(self) -> Response:
        self.calls += 1
        self.started.set()
        if self.order is not None:
            self.order.append(self.name)
        if not self.steps:
            raise AssertionError(f"unexpected attempt on {self.name}")
        step = self.steps.popleft()
        try:
            if step is None:
                return await asyncio.Future()
            if isinstance(step, Response):
                return step
            return await step()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class DefaultTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_original_payload(self):
        sender = ScriptedSender(OK)
        self.assertIs(await DefaultClient().request(sender, LatencyBudget(1)), OK)
        self.assertEqual(sender.calls, 1)

    async def test_http_errors_are_never_retried(self):
        for response in (BAD_REQUEST, UNAVAILABLE):
            with self.subTest(status=response.status):
                sender = ScriptedSender(response, OK)
                with self.assertRaises(RequestError) as raised:
                    await DefaultClient().request(sender, LatencyBudget(1))
                self.assertEqual(raised.exception.reason, StopReason.MAX_RETRIES)
                self.assertIs(raised.exception.last_response, response)
                self.assertEqual(sender.calls, 1)

    async def test_deadline_cancels_hanging_sender(self):
        sender = ScriptedSender(None)
        with self.assertRaises(RequestError) as raised:
            await DefaultClient().request(sender, LatencyBudget(0.04))
        self.assertEqual(raised.exception.reason, StopReason.LATENCY)
        self.assertEqual(raised.exception.last_outcome, Outcome.TIMEOUT)
        await asyncio.wait_for(sender.cancelled.wait(), 0.5)

    async def test_client_stops_waiting_during_transport_cleanup(self):
        cleaning = asyncio.Event()
        release = asyncio.Event()
        finished = asyncio.Event()

        async def delayed_cleanup():
            try:
                await asyncio.Future()
            finally:
                cleaning.set()
                await release.wait()
                finished.set()

        sender = ScriptedSender(delayed_cleanup)
        try:
            with self.assertRaises(RequestError):
                await asyncio.wait_for(DefaultClient().request(sender, LatencyBudget(0.04)), 0.5)
            await asyncio.wait_for(cleaning.wait(), 0.5)
            self.assertFalse(finished.is_set())
        finally:
            release.set()
        await asyncio.wait_for(finished.wait(), 0.5)


class RetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_retries_all_three_failure_types(self):
        sender = ScriptedSender(BAD_REQUEST, UNAVAILABLE, None, OK)
        result = await RetryClient().request(sender, RetryBudget(1, 0.03, max_retries=4))
        self.assertIs(result, OK)
        self.assertEqual(sender.calls, 4)
        self.assertTrue(sender.cancelled.is_set())

    async def test_each_error_budget_can_stop_retries(self):
        for name, outcome in (("fast_errors", BAD_REQUEST), ("failures", UNAVAILABLE), ("timeouts", None)):
            with self.subTest(budget=name):
                sender = ScriptedSender(outcome, outcome, OK)
                with self.assertRaises(RequestError) as raised:
                    await RetryClient().request(sender, RetryBudget(1, 0.03, **{name: 2}))
                self.assertEqual(raised.exception.reason, StopReason(name))
                self.assertEqual(raised.exception.attempts, 2)
                self.assertEqual(sender.calls, 2)

    async def test_max_retries_includes_first_attempt(self):
        sender = ScriptedSender(BAD_REQUEST, UNAVAILABLE, OK)
        with self.assertRaises(RequestError) as raised:
            await RetryClient().request(sender, RetryBudget(1, max_retries=2))
        self.assertEqual(raised.exception.reason, StopReason.MAX_RETRIES)
        self.assertEqual(sender.calls, 2)

    async def test_overall_deadline_limits_an_attempt(self):
        sender = ScriptedSender(BAD_REQUEST, None, OK)
        with self.assertRaises(RequestError) as raised:
            await RetryClient().request(sender, RetryBudget(0.06, 1))
        self.assertEqual(raised.exception.reason, StopReason.LATENCY)
        self.assertEqual(sender.calls, 2)


class BackoffTests(unittest.IsolatedAsyncioTestCase):
    async def test_delay_doubles_after_5xx_and_timeout_and_stops_at_cap(self):
        sender = ScriptedSender(UNAVAILABLE, None, UNAVAILABLE, UNAVAILABLE, OK)
        client = RetryClient(ExponentialBackoff(0.1, 0.25))
        with patch("resilience.clients.sleep", new_callable=AsyncMock) as pause:
            result = await client.request(sender, RetryBudget(2, 0.03, max_retries=5))
        self.assertIs(result, OK)
        self.assertEqual(pause.await_args_list, [call(0.1), call(0.2), call(0.25), call(0.25)])

    async def test_4xx_does_not_wait_or_advance_backoff(self):
        sender = ScriptedSender(BAD_REQUEST, UNAVAILABLE, BAD_REQUEST, UNAVAILABLE, OK)
        with patch("resilience.clients.sleep", new_callable=AsyncMock) as pause:
            await RetryClient(ExponentialBackoff()).request(sender, RetryBudget(2, max_retries=5))
        self.assertEqual(pause.await_args_list, [call(0.1), call(0.2)])

    async def test_backoff_shares_overall_deadline(self):
        sender = ScriptedSender(UNAVAILABLE, OK)
        with self.assertRaises(RequestError) as raised:
            await RetryClient(ExponentialBackoff(0.3)).request(sender, RetryBudget(0.05))
        self.assertEqual(raised.exception.reason, StopReason.LATENCY)
        self.assertEqual(sender.calls, 1)

    async def test_exhausted_budget_never_schedules_sleep(self):
        sender = ScriptedSender(UNAVAILABLE, OK)
        with patch("resilience.clients.sleep", new_callable=AsyncMock) as pause:
            with self.assertRaises(RequestError) as raised:
                await RetryClient(ExponentialBackoff()).request(sender, RetryBudget(1, failures=1))
        self.assertEqual(raised.exception.reason, StopReason.FAILURES)
        pause.assert_not_awaited()


class RoundRobinTests(unittest.IsolatedAsyncioTestCase):
    async def test_wraps_to_first_replica(self):
        order = []
        a = ScriptedSender(UNAVAILABLE, OK, name="A", order=order)
        b = ScriptedSender(BAD_REQUEST, name="B", order=order)
        c = ScriptedSender(None, name="C", order=order)
        client = RoundRobinClient()
        self.assertIs(await client.request([a, b, c], RetryBudget(1, 0.03, max_retries=4)), OK)
        self.assertEqual(order, ["A", "B", "C", "A"])

    async def test_rotation_continues_with_fresh_request_senders(self):
        order = []
        client = RoundRobinClient()
        for _ in range(3):
            pool = [ScriptedSender(OK, name=name, order=order) for name in "AB"]
            await client.request(pool, RetryBudget(1))
        self.assertEqual(order, ["A", "B", "A"])

    async def test_shares_error_budget_across_replicas(self):
        a, b, c = ScriptedSender(UNAVAILABLE), ScriptedSender(UNAVAILABLE), ScriptedSender(OK)
        with self.assertRaises(RequestError) as raised:
            await RoundRobinClient().request([a, b, c], RetryBudget(1, failures=2))
        self.assertEqual(raised.exception.reason, StopReason.FAILURES)
        self.assertEqual([a.calls, b.calls, c.calls], [1, 1, 0])

    async def test_accepts_same_backoff_policy_as_single_client(self):
        a, b = ScriptedSender(UNAVAILABLE), ScriptedSender(OK)
        client = RoundRobinClient(ExponentialBackoff(0.2))
        with patch("resilience.clients.sleep", new_callable=AsyncMock) as pause:
            self.assertIs(await client.request([a, b], RetryBudget(1)), OK)
        pause.assert_awaited_once_with(0.2)


class HedgingTests(unittest.IsolatedAsyncioTestCase):
    async def test_fast_primary_does_not_send_hedges(self):
        primary, backup = ScriptedSender(OK), ScriptedSender(OK)
        self.assertIs(await HedgingClient(0.2).request([primary, backup], LatencyBudget(1)), OK)
        self.assertEqual([primary.calls, backup.calls], [1, 0])

    async def test_delayed_fanout_runs_in_parallel_and_cancels_losers(self):
        primary = ScriptedSender(None, name="A")
        second = ScriptedSender(None, name="B")
        third_response = Response(201, b"C")
        release = asyncio.Event()

        async def respond():
            await release.wait()
            return third_response

        third = ScriptedSender(respond, name="C")
        task = asyncio.create_task(HedgingClient(0.08).request([primary, second, third], LatencyBudget(1)))
        self.addAsyncCleanup(self._cleanup_request, task)
        await asyncio.wait_for(primary.started.wait(), 0.5)
        self.assertEqual([second.calls, third.calls], [0, 0])
        await asyncio.wait_for(second.started.wait(), 0.5)
        await asyncio.wait_for(third.started.wait(), 0.5)
        release.set()
        self.assertIs(await task, third_response)
        self.assertEqual([primary.calls, second.calls, third.calls], [1, 1, 1])
        await asyncio.wait_for(primary.cancelled.wait(), 0.5)
        await asyncio.wait_for(second.cancelled.wait(), 0.5)

    async def _cleanup_request(self, task):
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def test_early_primary_failure_and_failed_hedge_allow_success(self):
        a, b, c = ScriptedSender(UNAVAILABLE), ScriptedSender(BAD_REQUEST), ScriptedSender(OK)
        self.assertIs(await HedgingClient(0.5).request([a, b, c], LatencyBudget(1)), OK)
        self.assertEqual([a.calls, b.calls, c.calls], [1, 1, 1])

    async def test_no_hedges_when_delay_exceeds_deadline(self):
        a, b = ScriptedSender(None), ScriptedSender(OK)
        with self.assertRaises(RequestError) as raised:
            await HedgingClient(0.2).request([a, b], LatencyBudget(0.05))
        self.assertEqual(raised.exception.reason, StopReason.LATENCY)
        self.assertEqual([a.calls, b.calls], [1, 0])

    async def test_all_http_failures_wait_for_deadline(self):
        a, b = ScriptedSender(UNAVAILABLE), ScriptedSender(BAD_REQUEST)
        clock = asyncio.get_running_loop().time
        started_at = clock()
        with self.assertRaises(RequestError) as raised:
            await HedgingClient(0.01).request([a, b], LatencyBudget(0.06))
        self.assertEqual(raised.exception.reason, StopReason.LATENCY)
        self.assertGreaterEqual(clock() - started_at, 0.05)
        self.assertEqual([a.calls, b.calls], [1, 1])


class CircuitBreakingTests(unittest.IsolatedAsyncioTestCase):
    async def test_open_replica_is_skipped_without_spending_attempt(self):
        order = []
        client = CircuitBreakingClient(window=3, failure_threshold=0.5, cooldown=1)
        for _ in range(2):
            a = ScriptedSender(BAD_REQUEST, name="A", order=order)
            with self.assertRaises(RequestError):
                await client.request([Replica("a", a)], RetryBudget(1, max_retries=1))

        a = ScriptedSender(UNAVAILABLE, name="A", order=order)
        b = ScriptedSender(UNAVAILABLE, OK, name="B", order=order)
        self.assertIs(await client.request(
            [Replica("a", a), Replica("b", b)], RetryBudget(1, max_retries=3)
        ), OK)
        self.assertEqual(order, ["A", "A", "A", "B", "B"])

    async def test_uses_full_sliding_window_and_strict_threshold(self):
        client = CircuitBreakingClient(window=2, failure_threshold=0.5, cooldown=1)
        with self.assertRaises(RequestError) as raised:
            await client.request([Replica("a", ScriptedSender(UNAVAILABLE))], RetryBudget(1, max_retries=1))
        self.assertIs(raised.exception.reason, StopReason.MAX_RETRIES)
        self.assertIs(await client.request([Replica("a", ScriptedSender(OK))], RetryBudget(1)), OK)

        a = ScriptedSender(BAD_REQUEST, UNAVAILABLE)
        with self.assertRaises(RequestError) as raised:
            await client.request([Replica("a", a)], RetryBudget(1))
        self.assertIs(raised.exception.reason, StopReason.NO_AVAILABLE_REPLICAS)
        self.assertEqual(a.calls, 2)

    async def test_cooldown_expires_with_fresh_sender_and_cleared_history(self):
        client = CircuitBreakingClient(window=2, failure_threshold=0.5, cooldown=0.05)
        first = ScriptedSender(None, UNAVAILABLE)
        with self.assertRaises(RequestError) as raised:
            await client.request([Replica("a", first)], RetryBudget(1, 0.03))
        self.assertEqual(raised.exception.reason, StopReason.NO_AVAILABLE_REPLICAS)
        fresh = ScriptedSender(BAD_REQUEST, OK)
        with self.assertRaises(RequestError) as raised:
            await client.request([Replica("a", fresh)], RetryBudget(1))
        self.assertEqual(raised.exception.attempts, 0)
        await asyncio.sleep(0.08)
        self.assertIs(await client.request([Replica("a", fresh)], RetryBudget(1)), OK)
        self.assertEqual(fresh.calls, 2)

    async def test_all_open_returns_without_another_subrequest(self):
        client = CircuitBreakingClient(window=1, failure_threshold=0.5, cooldown=1)
        a, b = ScriptedSender(UNAVAILABLE), ScriptedSender(BAD_REQUEST)
        with self.assertRaises(RequestError) as raised:
            await client.request([Replica("a", a), Replica("b", b)], RetryBudget(1))
        self.assertEqual(raised.exception.reason, StopReason.NO_AVAILABLE_REPLICAS)
        self.assertEqual(raised.exception.attempts, 2)
        self.assertEqual([a.calls, b.calls], [1, 1])
