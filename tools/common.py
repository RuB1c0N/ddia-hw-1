import os
import time
from collections.abc import Iterable

import httpx


def app_urls() -> list[str]:
    urls = [
        url.strip().rstrip("/")
        for url in os.getenv("APP_URLS", "http://app-1:8000,http://app-2:8000").split(",")
        if url.strip()
    ]
    if not urls:
        raise ValueError("APP_URLS must contain at least one URL")
    return urls


def wait_ready(client: httpx.Client, urls: Iterable[str], timeout: float = 180) -> None:
    pending = set(urls)
    deadline = time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        for url in tuple(pending):
            try:
                if client.get(url).is_success:
                    pending.remove(url)
            except httpx.HTTPError:
                pass
        if pending:
            time.sleep(1)
    if pending:
        raise RuntimeError(f"Readiness timeout for {', '.join(sorted(pending))}")


def checked(response: httpx.Response) -> dict | list:
    if not response.is_success:
        raise RuntimeError(
            f"HTTP {response.status_code}: {response.request.method} {response.request.url.path}"
        )
    return response.json()
