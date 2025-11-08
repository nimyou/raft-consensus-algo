# Dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System deps for healthcheck (curl) and building wheels
RUN apt-get update && apt-get install -y --no-install-recommends curl build-essential \
    && rm -rf /var/lib/apt/lists/*

# If you already have a requirements.txt, keep this.
# Otherwise this minimal set works for your code.
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

# App code
COPY . /app

# data dir for JSON persistence & snapshots
RUN mkdir -p /app/data

# Expose nothing explicitly; we bind the PORT via env & compose
CMD ["python", "rpc_server.py"]
