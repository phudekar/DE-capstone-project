FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install de-common first (changes less often)
COPY libs/common /app/libs/common
RUN pip install --no-cache-dir /app/libs/common

# Install kafka-bridge
COPY services/kafka-bridge /app/services/kafka-bridge
RUN pip install --no-cache-dir /app/services/kafka-bridge

ENV AVRO_SCHEMA_DIR=/app/libs/common/schemas/avro

CMD ["python", "-m", "kafka_bridge"]
