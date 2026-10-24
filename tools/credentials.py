import os
import secrets
from pathlib import Path


def main() -> None:
    directory = Path(os.getenv("CREDENTIALS_DIR", "/run/credentials"))
    directory.mkdir(parents=True, exist_ok=True)
    password_file = directory / "grafana-password"
    if not password_file.exists():
        password = os.getenv("GRAFANA_ADMIN_PASSWORD") or secrets.token_urlsafe(24)
        if len(password) < 12:
            raise ValueError("GRAFANA_ADMIN_PASSWORD must contain at least 12 characters")
        descriptor = os.open(password_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(password)
    if hasattr(os, "chown"):
        os.chown(directory, 10001, 472)
        os.chmod(directory, 0o750)
        os.chown(password_file, 10001, 472)
        os.chmod(password_file, 0o640)
    print("Grafana credentials are ready in the private volume")


if __name__ == "__main__":
    main()
