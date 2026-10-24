import asyncio
import os
import random
import time
from collections import Counter, deque
from dataclasses import dataclass

import httpx

from tools.common import app_urls


@dataclass(frozen=True)
class LoadConfig:
    urls: tuple[str, ...]
    target_rps: float = 30
    ramp_seconds: float = 120
    missing_rate: float = 0.03
    concurrency: int = 20
    duration: float = 0

    def __post_init__(self):
        if not self.urls or not 1 <= self.target_rps <= 100:
            raise ValueError("Set at least one URL and LOAD_TARGET_RPS in 1..100")
        if self.ramp_seconds < 0 or not 0 <= self.missing_rate <= 0.2:
            raise ValueError("Invalid ramp duration or LOAD_ERROR_RATE (0..0.2)")
        if not 1 <= self.concurrency <= 100 or self.duration < 0:
            raise ValueError("Invalid LOAD_CONCURRENCY (1..100) or LOAD_DURATION_SECONDS")

    def rate_at(self, elapsed: float) -> float:
        progress = min(1.0, elapsed / self.ramp_seconds) if self.ramp_seconds else 1.0
        return min(self.target_rps, 3 + (self.target_rps - 3) * progress)


class LoadRunner:
    def __init__(self, config: LoadConfig):
        self.config = config
        self.ids = {url: deque(maxlen=200) for url in config.urls}
        self.statuses: Counter = Counter()
        self.routes: Counter = Counter()
        self.skipped = 0

    async def request(self, client: httpx.AsyncClient, sequence: int) -> None:
        url = self.config.urls[sequence % len(self.config.urls)]
        operation = (sequence // len(self.config.urls)) % 3
        known = self.ids[url]
        method = ("POST", "GET", "PUT")[operation]
        payload = {"name": "background-load", "value": sequence % 1000}
        if method == "POST" or not known:
            method, path, body = "POST", "/items", payload
        else:
            item_id = -1 if random.random() < self.config.missing_rate else random.choice(known)
            path = f"/items/{item_id}"
            body = payload if method == "PUT" else None
        self.routes[(url, method)] += 1
        try:
            response = await client.request(method, url + path, json=body)
            self.statuses[str(response.status_code)] += 1
            if method == "POST" and response.status_code == 201:
                known.append(response.json()["id"])
        except httpx.HTTPError:
            self.statuses["transport_error"] += 1

    async def run(self, transport: httpx.AsyncBaseTransport | None = None) -> None:
        limits = httpx.Limits(max_connections=self.config.concurrency)
        async with httpx.AsyncClient(timeout=5, limits=limits, transport=transport) as client:
            deadline = time.monotonic() + 180
            pending = set(self.config.urls)
            while pending:
                for url in tuple(pending):
                    try:
                        if (await client.get(f"{url}/health")).is_success:
                            pending.remove(url)
                    except httpx.HTTPError:
                        pass
                if time.monotonic() >= deadline:
                    raise RuntimeError("Load generator readiness timeout")
                if pending:
                    await asyncio.sleep(1)
            active: set[asyncio.Task] = set()
            started = next_request = last_log = time.monotonic()
            sequence = 0
            try:
                while not self.config.duration or time.monotonic() - started < self.config.duration:
                    now = time.monotonic()
                    rate = self.config.rate_at(now - started)
                    if now < next_request:
                        await asyncio.sleep(next_request - now)
                        continue
                    if len(active) < self.config.concurrency:
                        task = asyncio.create_task(self.request(client, sequence))
                        active.add(task)
                        task.add_done_callback(active.discard)
                    else:
                        self.skipped += 1
                    sequence += 1
                    next_request = max(next_request + 1 / rate, now)
                    if now - last_log >= 10:
                        print(
                            f"target_rps={rate:.1f} completed={sum(self.statuses.values())} "
                            f"statuses={dict(self.statuses)} skipped={self.skipped}",
                            flush=True,
                        )
                        last_log = now
            finally:
                if active:
                    await asyncio.gather(*active)
            print(
                f"Load stopped: statuses={dict(self.statuses)} skipped={self.skipped}", flush=True
            )


def main() -> None:
    config = LoadConfig(
        urls=tuple(app_urls()),
        target_rps=float(os.getenv("LOAD_TARGET_RPS", "30")),
        ramp_seconds=float(os.getenv("LOAD_RAMP_SECONDS", "120")),
        missing_rate=float(os.getenv("LOAD_ERROR_RATE", "0.03")),
        concurrency=int(os.getenv("LOAD_CONCURRENCY", "20")),
        duration=float(os.getenv("LOAD_DURATION_SECONDS", "0")),
    )
    asyncio.run(LoadRunner(config).run())


if __name__ == "__main__":
    main()
