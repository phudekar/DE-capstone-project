FROM python:3.11.12-slim
RUN pip install --no-cache-dir prometheus-client docker
COPY exporter.py /app/exporter.py
EXPOSE 9417
CMD ["python", "/app/exporter.py"]
