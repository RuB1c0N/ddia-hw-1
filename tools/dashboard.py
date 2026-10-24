import json
import os
from pathlib import Path

import httpx

from tools.common import app_urls, checked, wait_ready

DATASOURCE = {"type": "prometheus", "uid": "ddia-vm"}
QUANTILES = (("p50", 0.50), ("p75", 0.75), ("p90", 0.90), ("p95", 0.95), ("p99", 0.99))


def discover(client: httpx.Client, urls: list[str]) -> list[dict[str, str]]:
    instances = []
    for url in urls:
        payload = checked(client.get(f"{url}/handlers"))
        handlers = payload["handlers"]
        if not handlers or len(handlers) > 100:
            raise ValueError("Discovery must return 1..100 business endpoints")
        normalized = {}
        for handler in handlers:
            method, path = handler["method"], handler["path"]
            if not isinstance(path, str) or not path.startswith("/") or not method.isalpha():
                raise ValueError("Invalid discovery response")
            endpoint = f"{method.upper()} {path}"
            normalized[endpoint] = {"endpoint": endpoint, "method": method.upper(), "path": path}
        instances.append(normalized)
    if any(set(instance) != set(instances[0]) for instance in instances[1:]):
        raise RuntimeError("Application instances expose different business endpoints")
    return [instances[0][key] for key in sorted(instances[0])]


def selectors(endpoint: str, metric_type: str, errors: bool = False) -> str:
    labels = [
        f"endpoint={json.dumps(endpoint)}",
        f'metric_type="{metric_type}"',
        'deployment=~"${deployment:regex}"',
        'datacenter=~"${datacenter:regex}"',
        'pod=~"${pod:regex}"',
    ]
    if errors:
        labels.append('status_code=~"4..|5.."')
    return ",".join(labels)


def queries(endpoint: str) -> dict:
    traffic = selectors(endpoint, "traffic")
    latency = selectors(endpoint, "latency")
    errors = selectors(endpoint, "traffic", errors=True)
    return {
        "latency": [
            (
                label,
                f"histogram_quantile({quantile}, sum by (le) (rate("
                f"http_request_duration_seconds_bucket{{{latency}}}[$__rate_interval])))",
            )
            for label, quantile in QUANTILES
        ],
        "rps": [("RPS", f"sum(rate(http_requests_total{{{traffic}}}[$__rate_interval]))")],
        "errors": [
            (
                "HTTP {{status_code}}",
                f"sum by (status_code) (rate(http_requests_total{{{errors}}}[$__rate_interval]))",
            )
        ],
    }


def variable(name: str, label: str, query: str) -> dict:
    return {
        "name": name,
        "label": label,
        "type": "query",
        "datasource": DATASOURCE,
        "definition": query,
        "query": {"query": query, "refId": f"var-{name}"},
        "multi": True,
        "includeAll": True,
        "allValue": ".*",
        "refresh": 1,
        "sort": 1,
        "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
        "options": [],
    }


def build_dashboard(handlers: list[dict[str, str]]) -> dict:
    panels = []
    for index, handler in enumerate(handlers):
        endpoint = handler["endpoint"]
        expressions = queries(endpoint)
        for column, (kind, label, unit) in enumerate(
            (
                ("latency", "Latency", "s"),
                ("rps", "Throughput", "reqps"),
                ("errors", "Errors by HTTP status", "reqps"),
            )
        ):
            panels.append(
                {
                    "id": index * 3 + column + 1,
                    "type": "timeseries",
                    "title": f"{endpoint} — {label}",
                    "datasource": DATASOURCE,
                    "gridPos": {"x": column * 8, "y": index * 8, "w": 8, "h": 8},
                    "targets": [
                        {
                            "refId": chr(65 + i),
                            "expr": expression,
                            "legendFormat": legend,
                            "datasource": DATASOURCE,
                            "range": True,
                        }
                        for i, (legend, expression) in enumerate(expressions[kind])
                    ],
                    "fieldConfig": {
                        "defaults": {
                            "unit": unit,
                            "min": 0,
                            "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 6},
                        },
                        "overrides": [],
                    },
                    "options": {
                        "legend": {"displayMode": "list", "placement": "bottom"},
                        "tooltip": {"mode": "multi"},
                    },
                }
            )
    return {
        "uid": "ddia-hw-1",
        "id": None,
        "title": "DDIA HW1 — REST endpoint monitoring",
        "description": "Runtime-discovered routes; merged histogram quantiles; errors per second.",
        "editable": False,
        "schemaVersion": 40,
        "version": 0,
        "tags": ["ddia", "mse-mhs"],
        "timezone": "browser",
        "refresh": "5s",
        "time": {"from": "now-5m", "to": "now"},
        "panels": panels,
        "templating": {
            "list": [
                variable(
                    "deployment", "Deployment", "label_values(http_requests_total, deployment)"
                ),
                variable(
                    "datacenter",
                    "Datacenter",
                    "label_values(http_requests_total{"
                    'deployment=~"${deployment:regex}"}, datacenter)',
                ),
                variable(
                    "pod",
                    "Pod",
                    "label_values(http_requests_total{"
                    'deployment=~"${deployment:regex}",datacenter=~"${datacenter:regex}"}, pod)',
                ),
            ]
        },
    }


def main() -> None:
    grafana = os.getenv("GRAFANA_URL", "http://grafana:3000").rstrip("/")
    urls = app_urls()
    token = (
        Path(os.getenv("GRAFANA_TOKEN_FILE", "/run/grafana-token/token"))
        .read_text(encoding="utf-8")
        .strip()
    )
    with httpx.Client(timeout=10) as discovery_client:
        wait_ready(discovery_client, [f"{url}/health" for url in urls] + [f"{grafana}/api/health"])
        dashboard = build_dashboard(discover(discovery_client, urls))
    with httpx.Client(timeout=10, headers={"Authorization": f"Bearer {token}"}) as client:
        result = checked(
            client.post(
                f"{grafana}/api/dashboards/db",
                json={
                    "dashboard": dashboard,
                    "overwrite": True,
                    "message": "Generated from live routes",
                },
            )
        )
        print(f"Dashboard ready: {result['url']} ({len(dashboard['panels'])} panels)")


if __name__ == "__main__":
    main()
