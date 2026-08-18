# Deployment Guide — RedNote Translate

This app is containerized (see `Dockerfile`) and runs on any Docker host.
It uses **FastAPI** + **faster-whisper** + **ffmpeg** + **Edge TTS** (no API keys needed for the default config).

Default port: **7860** (overridable via `PORT` env var).

---

## Option 1: Hugging Face Spaces (Free, 16GB RAM, 2 vCPU) — RECOMMENDED

> ⚠️ Docker Spaces require a **PRO account** ($9/month). If you only have a free account,
> use Option 2 (Oracle Cloud) instead — it's free forever with more RAM.

1. Sign up at [huggingface.co](https://huggingface.co) (or upgrade to PRO for Docker Spaces)
2. Create a new Space:
   - Owner: your account
   - Name: `rednote-translate`
   - SDK: **Docker**
   - Visibility: Public (or Private with PRO)
3. Clone the Space repo and copy the project files:
   ```bash
   git clone https://huggingface.co/spaces/YOUR_USERNAME/rednote-translate
   cp -r /path/to/rednote-translate/* rednote-translate/
   cd rednote-translate
   ```
4. Ensure `README.md` has this YAML frontmatter at the top:
   ```yaml
   ---
   title: RedNote Translate
   emoji: 🎬
   colorFrom: red
   colorTo: blue
   sdk: docker
   app_port: 7860
   pinned: false
   ---
   ```
5. Commit and push:
   ```bash
   git add -A && git commit -m "deploy" && git push
   ```
6. The Space builds automatically. URL: `https://YOUR_USERNAME-rednote-translate.hf.space`

### Add API keys (optional, for Fish Audio / Natural Drama AI)
- Go to Space **Settings** → **Secrets**
- Add `FISH_API_KEY` (for human-quality TTS + voice cloning)
- Add `LIBERTAI_API_KEY` (for LLM-powered drama translation)

---

## Option 2: Oracle Cloud Always Free (Free forever, 24GB RAM, 4 ARM vCPU) — BEST FREE

1. Sign up at [oracle.com/cloud/free](https://www.oracle.com/cloud/free/)
   - Requires credit card verification (no actual charge)
   - You get **$300 credit** (30 days) + **Always Free** resources forever
2. Create an **Ampere A1** compute instance:
   - Shape: `VM.Standard.A1.Flex`
   - OCPUs: 4, Memory: 24 GB (all free)
   - OS: Ubuntu 22.04 (Canonical Ubuntu)
   - Add SSH key
3. SSH into the instance and run:
   ```bash
   # Install Docker
   curl -fsSL https://get.docker.com | sh
   sudo usermod -aG docker $USER && newgrp docker

   # Clone and run
   git clone https://github.com/Hadi990op/rednote-translate.git
   cd rednote-translate
   git submodule update --init --recursive
   docker build -t rednote-translate .
   docker run -d --name rednote --restart=always -p 7860:7860 \
     -v rednote_data:/home/user/data \
     rednote-translate
   ```
4. Open port 7860 in the OCI security list (VCN → Security Lists → Ingress)
5. Optional: Add a free domain via Cloudflare + Caddy for HTTPS

### With API keys:
   ```bash
   docker run -d --name rednote --restart=always -p 7860:7860 \
     -v rednote_data:/home/user/data \
     -e FISH_API_KEY="your_key" \
     -e LIBERTAI_API_KEY="your_key" \
     rednote-translate
   ```

---

## Option 3: Any Docker host (fly.io, Render, VPS, etc.)

```bash
git clone https://github.com/Hadi990op/rednote-translate.git
cd rednote-translate
git submodule update --init --recursive
docker build -t rednote-translate .
docker run -d --name rednote --restart=always -p 7860:7860 \
  -v rednote_data:/home/user/data \
  rednote-translate
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `7860` | HTTP port to listen on |
| `REDNOTE_WORKSPACE` | `/home/user/data` | Where videos/output/jobs are stored |
| `XHS_DOWNLOADER_PATH` | `vendor/XHS-Downloader` | Path to XHS-Downloader module |
| `SECRETS_DIR` | `/home/user/secrets` | Where API key files live |
| `FISH_API_KEY` | *(none)* | Fish Audio API key (TTS + voice cloning) |
| `LIBERTAI_API_KEY` | *(none)* | LLM API key for Natural Drama translation |

### Without API keys
The app works fully without any API keys — it falls back to:
- **Google Translate** (free, no key) instead of Natural Drama AI
- **Edge Neural TTS** (free, no key) instead of Fish Audio

---

## Option 4: Bare metal / systemd (no Docker)

```bash
# Prerequisites
apt-get install -y python3-pip ffmpeg git
git clone https://github.com/Hadi990op/rednote-translate.git
cd rednote-translate
git submodule update --init --recursive

# Install deps
pip install -r requirements.txt
pip install -r vendor/XHS-Downloader/requirements.txt

# Run
python3 web_app.py
# Or with env vars:
REDNOTE_WORKSPACE=/var/rednote PORT=7860 python3 web_app.py
```
