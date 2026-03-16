FROM python:3.11.12-slim
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir prometheus-client docker
COPY exporter.py /app/exporter.py
EXPOSE 9417
CMD ["python", "/app/exporter.py"]
