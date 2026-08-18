# ─── RedNote Translate — Docker Image ─────────────────────────────────────────
# Multi-stage build: heavy deps installed once, final image kept lean.
#
# Target platforms (free always-on):
#   - Hugging Face Spaces (Docker SDK, 16GB RAM, 2 vCPU)  ← primary
#   - Koyeb free instance (512MB RAM — too small for Whisper, skip)
#   - Any Docker host
#
# Build:  docker build -t rednote-translate .
# Run:    docker run -p 7860:7860 -e PORT=7860 rednote-translate
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS base

# ─── System deps ────────────────────────────────────────────────────────────
# ffmpeg: audio extraction for Whisper transcription
# git:    XHS-Downloader is a git submodule
# curl:   fallback video downloader
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ffmpeg \
        git \
        curl \
        ca-certificates \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ─── Python dependencies ─────────────────────────────────────────────────────
# Install XHS-Downloader deps first (submodule), then project deps.
COPY vendor/XHS-Downloader/requirements.txt /app/vendor/XHS-Downloader/requirements.txt
RUN pip install --no-cache-dir -r /app/vendor/XHS-Downloader/requirements.txt

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# ─── Application code ────────────────────────────────────────────────────────
# Copy the full project (submodules already vendored in vendor/)
COPY . /app

# Paths are env-driven; default to /app/data for persistence inside the container
ENV REDNOTE_WORKSPACE=/app/data \
    XHS_DOWNLOADER_PATH=/app/vendor/XHS-Downloader \
    SECRETS_DIR=/app/secrets \
    PYTHONUNBUFFERED=1 \
    PORT=7860

# HF Spaces expects the app on port 7860. We default to that but allow override.
# The web_app.py main block reads $PORT and binds 0.0.0.0.
RUN mkdir -p /app/data /app/secrets

EXPOSE 7860

# Healthcheck — hit the config endpoint (lightweight, no Whisper load)
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -fs http://localhost:7860/api/config || exit 1

CMD ["python3", "web_app.py"]
