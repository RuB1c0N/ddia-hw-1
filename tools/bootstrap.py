import os
from pathlib import Path

import httpx

from tools.common import app_urls, checked, wait_ready


def ensure_token(admin: httpx.Client, grafana: str, token_file: Path) -> None:
    name = "ddia-dashboard-generator"
    accounts = checked(admin.get(f"{grafana}/api/serviceaccounts/search", params={"query": name}))
    matches = [account for account in accounts["serviceAccounts"] if account["name"] == name]
    if matches:
        account_id = matches[0]["id"]
        if matches[0]["role"] != "Editor":
            checked(
                admin.patch(f"{grafana}/api/serviceaccounts/{account_id}", json={"role": "Editor"})
            )
    else:
        account = checked(
            admin.post(f"{grafana}/api/serviceaccounts", json={"name": name, "role": "Editor"})
        )
        account_id = account["id"]

    if token_file.exists():
        token = token_file.read_text(encoding="utf-8").strip()
        with httpx.Client(timeout=10, headers={"Authorization": f"Bearer {token}"}) as service:
            response = service.get(f"{grafana}/api/user")
            if response.is_success and response.json().get("isGrafanaAdmin") is False:
                print("Existing service account token is valid")
                return

    tokens = checked(admin.get(f"{grafana}/api/serviceaccounts/{account_id}/tokens"))
    for token in tokens:
        if token["name"] == name:
            response = admin.delete(
                f"{grafana}/api/serviceaccounts/{account_id}/tokens/{token['id']}"
            )
            if not response.is_success:
                raise RuntimeError("Cannot rotate the generator token")
    result = checked(
        admin.post(f"{grafana}/api/serviceaccounts/{account_id}/tokens", json={"name": name})
    )
    token_file.parent.mkdir(parents=True, exist_ok=True)
    temporary = token_file.with_suffix(".tmp")
    temporary.write_text(result["key"], encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(token_file)
    print("Service account token is ready in the private volume")


def main() -> None:
    grafana = os.getenv("GRAFANA_URL", "http://grafana:3000").rstrip("/")
    victoria = os.getenv("VICTORIA_URL", "http://victoriametrics:8428").rstrip("/")
    password = (
        Path(os.getenv("GRAFANA_PASSWORD_FILE", "/run/credentials/grafana-password"))
        .read_text(encoding="utf-8")
        .strip()
    )
    with httpx.Client(timeout=5) as probe:
        wait_ready(
            probe,
            [
                f"{grafana}/api/health",
                f"{victoria}/health",
                *(f"{url}/health" for url in app_urls()),
            ],
        )
    with httpx.Client(timeout=10, auth=("admin", password)) as admin:
        datasource = checked(admin.get(f"{grafana}/api/datasources/uid/ddia-vm"))
        if datasource["type"] != "prometheus":
            raise RuntimeError("Provisioned datasource ddia-vm has an unexpected type")
        ensure_token(
            admin, grafana, Path(os.getenv("GRAFANA_TOKEN_FILE", "/run/grafana-token/token"))
        )


if __name__ == "__main__":
    main()
