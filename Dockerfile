FROM python:3.12.11-slim-bookworm AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /workspace
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && groupadd -g 10001 app && useradd -u 10001 -g app app \
    && mkdir -p /run/grafana-token && chown app:app /run/grafana-token
COPY app ./app
COPY endpoint_metrics ./endpoint_metrics
COPY tools ./tools
USER 10001:10001
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
