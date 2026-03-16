FROM apache/superset:3.1.3

USER root

RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install DuckDB SQLAlchemy driver and additional dependencies
RUN pip install --no-cache-dir \
    duckdb==1.1.3 \
    duckdb-engine==0.13.6 \
    pyiceberg[pyarrow,duckdb]==0.8.0 \
    slack_sdk==3.33.5 \
    jinja2==3.1.4

# Copy superset config
COPY superset_config.py /app/pythonpath/superset_config.py

# Copy DuckDB init SQL for view creation
COPY init_duckdb.sql /app/init_duckdb.sql

USER superset
