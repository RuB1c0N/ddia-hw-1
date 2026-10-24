from collections.abc import Callable
from dataclasses import dataclass
from time import perf_counter

from fastapi import APIRouter, FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
)
from starlette.exceptions import HTTPException


@dataclass(frozen=True)
class Identity:
    pod: str
    datacenter: str
    deployment: str


class EndpointMetrics:
    def __init__(self, identity: Identity) -> None:
        self.identity = identity
        self.registry = CollectorRegistry()
        self.labels = {
            "pod": identity.pod,
            "datacenter": identity.datacenter,
            "deployment": identity.deployment,
        }
        dimensions = ("pod", "datacenter", "deployment", "endpoint", "metric_type")
        self.requests = Counter(
            "http_requests",
            "Completed business requests by HTTP status.",
            (*dimensions, "status_code"),
            registry=self.registry,
        )
        self.duration = Histogram(
            "http_request_duration_seconds",
            "Business handler latency in seconds.",
            dimensions,
            registry=self.registry,
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
        )
        self.route_type = self._make_route_type()

    def _make_route_type(self) -> type[APIRoute]:
        metrics = self

        class MeasuredRoute(APIRoute):
            def get_route_handler(self) -> Callable:
                original = super().get_route_handler()

                async def measured(request: Request) -> Response:
                    started = perf_counter()
                    status = 500
                    try:
                        response = await original(request)
                        status = response.status_code
                        return response
                    except HTTPException as exc:
                        status = exc.status_code
                        raise
                    except RequestValidationError:
                        status = 422
                        raise
                    finally:
                        endpoint = f"{request.method} {self.path}"
                        common = {**metrics.labels, "endpoint": endpoint}
                        metrics.duration.labels(**common, metric_type="latency").observe(
                            perf_counter() - started
                        )
                        metrics.requests.labels(
                            **common, metric_type="traffic", status_code=str(status)
                        ).inc()

                return measured

        return MeasuredRoute

    def router(self, **kwargs) -> APIRouter:
        return APIRouter(route_class=self.route_type, **kwargs)

    def handlers(self, app: FastAPI) -> list[dict[str, str]]:
        return sorted(
            [
                {"method": method, "path": route.path, "endpoint": f"{method} {route.path}"}
                for route in app.routes
                if isinstance(route, self.route_type)
                for method in sorted(route.methods or ())
                if method not in {"HEAD", "OPTIONS"}
            ],
            key=lambda handler: handler["endpoint"],
        )

    def response(self) -> Response:
        return Response(
            generate_latest(self.registry), headers={"Content-Type": CONTENT_TYPE_LATEST}
        )
