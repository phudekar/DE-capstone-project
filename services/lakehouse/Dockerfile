FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY services/lakehouse /app/services/lakehouse
RUN pip install --no-cache-dir /app/services/lakehouse

# Copy reference data for dimension seeding
COPY services/flink-processor/data/reference/symbols.json /app/data/reference/symbols.json

CMD ["python", "-m", "lakehouse.writers.bronze_writer"]
