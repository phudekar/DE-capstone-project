FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install lakehouse dependency first (layer caching)
COPY services/lakehouse /app/services/lakehouse
RUN pip install --no-cache-dir /app/services/lakehouse

# Install dagster orchestrator
COPY services/dagster-orchestrator /app/services/dagster-orchestrator
RUN pip install --no-cache-dir /app/services/dagster-orchestrator

# Copy reference data for dimension seeding
COPY services/flink-processor/data/reference/symbols.json /app/data/reference/symbols.json

# Copy dagster config
COPY services/dagster-orchestrator/dagster.yaml /app/dagster.yaml
COPY services/dagster-orchestrator/workspace.yaml /app/workspace.yaml

ENV DAGSTER_HOME=/app

EXPOSE 3000
