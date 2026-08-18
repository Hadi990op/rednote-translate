# ─── RedNote Translate — Docker Image ─────────────────────────────────────────
# Portable image that runs on:
#   - Hugging Face Spaces (Docker SDK, port 7860, user ID 1000)  ← primary free target
#   - Oracle Cloud Always Free (ARM Ampere A1, 24GB RAM)         ← best free always-on
#   - Any Docker host (fly.io, Render, local, etc.)
#
# Build:  docker build -t rednote-translate .
# Run:    docker run -p 7860:7860 -e PORT=7860 rednote-translate
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS base

# ─── System deps ────────────────────────────────────────────────────────────
# ffmpeg: audio extraction for Whisper transcription
# git:    XHS-Downloader is a git submodule
# curl:   fallback video downloader + healthcheck
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ffmpeg \
        git \
        curl \
        ca-certificates \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# ─── Create non-root user (Hugging Face Spaces requires UID 1000) ────────────
RUN useradd -m -u 1000 user

WORKDIR /home/user/app

# ─── Python dependencies ─────────────────────────────────────────────────────
# Install XHS-Downloader deps first (submodule), then project deps.
COPY --chown=user vendor/XHS-Downloader/requirements.txt /home/user/app/vendor/XHS-Downloader/requirements.txt
USER user
RUN pip install --no-cache-dir -r /home/user/app/vendor/XHS-Downloader/requirements.txt

COPY --chown=user requirements.txt /home/user/app/requirements.txt
RUN pip install --no-cache-dir -r /home/user/app/requirements.txt

# ─── Application code ────────────────────────────────────────────────────────
USER root
COPY --chown=user . /home/user/app
USER user

# Paths are env-driven; default to ~/data for persistence inside the container
ENV REDNOTE_WORKSPACE=/home/user/data \
    XHS_DOWNLOADER_PATH=/home/user/app/vendor/XHS-Downloader \
    SECRETS_DIR=/home/user/secrets \
    PYTHONUNBUFFERED=1 \
    PORT=7860

RUN mkdir -p /home/user/data /home/user/secrets

EXPOSE 7860

# Healthcheck — hit the config endpoint (lightweight, no Whisper load)
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -fs http://localhost:7860/api/config || exit 1

CMD ["python3", "web_app.py"]
