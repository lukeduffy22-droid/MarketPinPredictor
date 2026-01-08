# Use an NVIDIA CUDA runtime image matching the CUDA version you need
FROM nvidia/cuda:11.8.0-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv git && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /app

# Make our helper scripts executable
RUN chmod +x /app/tools/collector_entrypoint.sh || true

EXPOSE 8000

# Multi-process entrypoint: runs both API server and live data collector
ENTRYPOINT ["/bin/bash", "-c"]
CMD ["exec bash /app/tools/collector_entrypoint.sh & PYTHONPATH=. uvicorn app.api.main:app --host 0.0.0.0 --port 8000"]
