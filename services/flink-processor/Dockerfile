FROM flink:1.18-scala_2.12-java11

# Install Python 3.11 + pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv && \
    rm -rf /var/lib/apt/lists/*

# Download Kafka connector JAR for Flink SQL
RUN mkdir -p /opt/flink/lib && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar \
         -o /opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar

# Install PyFlink and project dependencies
COPY pyproject.toml /opt/flink-processor/
COPY src/ /opt/flink-processor/src/
COPY data/ /opt/flink-processor/data/

RUN pip3 install --no-cache-dir --break-system-packages \
    apache-flink==1.18.1

ENV PYTHONPATH=/opt/flink-processor/src
ENV REFERENCE_DATA_PATH=/opt/flink-processor/data/reference/symbols.json
