# 🎬 RedNote Translate

**Download RedNote (XiaoHongShu) videos, transcribe Chinese audio, translate to multiple languages, and generate natural-sounding voice audio with voice cloning & emotion modes.**

A full-stack tool that takes a RedNote video URL and produces:
- 📝 Transcribed transcript (Chinese speech → text, with timestamps)
- 🌍 Translations in 25+ languages (Google Translate or Natural Drama AI)
- 🔊 Voice audio in the target language (Fish Audio S2.1 Pro or Edge Neural TTS)
- 🎙️ Voice cloning — translations sound like the original speaker
- 🎭 Emotion mode — expressive drama delivery (excited, sad, whispering, shouting)

Live demo: **https://peace-auto-mule-cry.2n6.me/rednote/**

---

## ✨ Features

### Video Processing
- **Download** RedNote/XiaoHongShu videos via XHS-Downloader
- **Transcribe** Chinese audio to text with timestamps using faster-whisper (CTranslate2)
- **5 Whisper models**: tiny → large-v3 (speed/accuracy tradeoff)

### Translation
- **⚡ Google Translate** — fast, free, no API key, accurate meaning
- **🎭 Natural Drama AI** — LLM-powered translation that understands context, emotion, relationships, and drama tone. Produces natural spoken dialogue, not machine-translated text. Uses any OpenAI-compatible API.

### Text-to-Speech
- **🐟 Fish Audio S2.1 Pro** — cloud API, 83 languages, human-quality voices, ~90ms latency
- **⚡ Edge Neural TTS** — local fallback, 300+ voices, fast, reliable, no API key

### Voice Cloning 🎙️
- Clones the speaker's voice from the original video audio
- Translations are spoken in the original person's voice across all languages
- Uses Fish Audio's voice cloning API (fast training mode)
- One cloned voice model serves all target languages

### Emotion Mode 🎭
- Detects emotions from translated text and adds expressive delivery
- Supports: `[excited]`, `[angry]`, `[sad]`, `[scared]`, `[whispering]`, `[shouting]`, `[laughing]`, `[curious]`
- Language-agnostic detection (matches English, Hindi, Urdu keywords + punctuation)

### Web UI
- Real-time progress via Server-Sent Events (SSE)
- Live preview of transcript, translations, and audio as they complete
- Download results as JSON, TXT, or SRT
- Per-language audio players and download buttons
- 25+ supported languages with visual language chips

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Browser (web/index.html)                      │
│  Language chips · Model selector · TTS engine · Voice clone      │
│  Live preview panel · Audio players · Download buttons           │
└──────────────────────────────┬────────────────────────────────────┘
                               │ HTTP + SSE
┌──────────────────────────────▼────────────────────────────────────┐
│                   FastAPI Backend (web_app.py)                     │
│                                                                   │
│  POST /api/process     → Start job (returns job_id)                │
│  GET  /api/status/:id  → SSE stream (progress + live results)      │
│  GET  /api/config      → Available languages, models, engines     │
│  GET  /api/jobs        → List completed jobs                       │
│  GET  /api/download/:id/:fmt → Download JSON/TXT/SRT/audio         │
│  GET  /                → Serve frontend HTML                      │
│                                                                   │
│  Pipeline:                                                        │
│  1. Download (XHS-Downloader)                                     │
│  2. Transcribe (faster-whisper)                                    │
│  3. [Optional] Clone voice (Fish Audio API)                      │
│  4. Translate (Google Translate or Natural Drama AI)              │
│  5. Generate TTS (Fish Audio or Edge Neural TTS)                  │
│  6. Save results (JSON, TXT, SRT, MP3)                            │
└───────────────────────────────────────────────────────────────────┘
```

### Files
| File | Description |
|------|-------------|
| `web_app.py` | FastAPI backend — the main web application |
| `rednote_translate.py` | CLI tool (standalone, no web server needed) |
| `web/index.html` | Frontend — single-page app with live preview |
| `drama_translation_prompt.txt` | System prompt for Natural Drama AI translation mode |
| `requirements.txt` | Python dependencies |

---

## 🚀 Setup

### Prerequisites
- Python 3.10+
- ffmpeg (for audio extraction)
- [XHS-Downloader](https://github.com/JoeanAmier/XHS-Downloader) (for RedNote video downloads)

### Install

```bash
# Clone the repo
git clone https://github.com/Hadi990op/rednote-translate.git
cd rednote-translate

# Install Python dependencies
pip install -r requirements.txt

# Install ffmpeg (Debian/Ubuntu)
apt-get install -y ffmpeg

# Install XHS-Downloader
git clone https://github.com/JoeanAmier/XHS-Downloader.git ../XHS-Downloader
pip install -r ../XHS-Downloader/requirements.txt
```

### API Keys (optional)

**Fish Audio** (for human-quality TTS + voice cloning):
1. Register at [fish.audio](https://fish.audio)
2. Get your API key from the dashboard
3. Save it:
```bash
echo "your_api_key_here" > secrets/fish_audio_api_key.txt
chmod 600 secrets/fish_audio_api_key.txt
```
Or set environment variable: `export FISH_API_KEY="your_key"`

**Natural Drama AI** (for LLM-powered translation):
- Uses any OpenAI-compatible API endpoint
- Set `LIBERTAI_API_KEY` in your environment or `.env` file
- Configure the endpoint and model in `web_app.py` (`translate_text_natural()`)

> Without API keys, the app falls back to Google Translate + Edge Neural TTS (both free, no keys needed).

### Run

```bash
# Start the web server (default port 9100)
python3 web_app.py

# Or with uvicorn directly
uvicorn web_app:app --host 0.0.0.0 --port 9100
```

Then open: **http://localhost:9100**

### CLI Usage (standalone, no web server)

```bash
# Basic usage
python3 rednote_translate.py "https://www.xiaohongshu.com/explore/XXXX?xsec_token=YYYY"

# Specify languages and model
python3 rednote_translate.py "https://xhslink.com/abc123" --languages en hi ur --model small

# Available options
python3 rednote_translate.py --help
```

---

## 🌐 Supported Languages

**25+ languages** for translation and TTS:

English, Hindi, Urdu, Spanish, French, German, Arabic, Japanese, Korean, Russian, Portuguese, Italian, Turkish, Indonesian, Vietnamese, Thai, Bengali, Punjabi, Tamil, Telugu, Marathi, Gujarati, Nepali, Persian, Chinese (Simplified & Traditional)

---

## 🎛️ Configuration

### Whisper Models
| Model | Speed | Accuracy | RAM |
|-------|-------|----------|-----|
| tiny | Fastest | Basic | ~1 GB |
| base | Fast | Basic | ~1 GB |
| small | Good | Good balance | ~2 GB |
| medium | Slow | High | ~5 GB |
| large-v3 | Slowest | Best | ~10 GB |

### TTS Engines
| Engine | Quality | Speed | Cost | Cloning | Emotions |
|--------|--------|-------|------|---------|----------|
| Fish Audio S2.1 Pro | Human-quality | ~90ms | Free tier | ✅ | ✅ |
| Edge Neural TTS | Good | Fast | Free | ❌ | ❌ |

### Translation Modes
| Mode | Quality | Speed | Cost |
|------|---------|-------|------|
| Google Translate | Basic meaning | Fast | Free |
| Natural Drama AI | Natural dialogue | Slow (~30s/lang) | API credits |

---

## 🔧 Deployment

### systemd service

```ini
[Unit]
Description=RedNote Translate Web App
After=network.target

[Service]
Type=simple
WorkingDirectory=/path/to/rednote_downloads
ExecStart=/usr/bin/python3 /path/to/rednote_downloads/web_app.py
Restart=always
RestartSec=3
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

### Caddy reverse proxy (optional, for HTTPS)

```caddy
handle_path /rednote/* {
    reverse_proxy localhost:9100
}
```

---

## 📝 Notes

- **RedNote URL tokens expire** — the `xsec_token` in the URL is time-limited. Use fresh URLs for reliable downloads.
- **Voice cloning** requires Fish Audio and works best with 10-30s of clean speech.
- **Emotion mode** only applies to Fish Audio TTS (Edge TTS doesn't support emotion tags).
- **Natural Drama AI** is significantly slower but produces dramatically better translations for drama/dialogue content.
- **Job state** persists to disk (`job_state/`) so jobs survive server restarts.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) file.

---

## 🙏 Acknowledgments

- [XHS-Downloader](https://github.com/JoeanAmier/XHS-Downloader) by JoeanAmier — RedNote video downloading
- [faster-whisper](https://github.com/SYSTRAN/faster-whisper) by SYSTRAN — fast Whisper inference
- [deep-translator](https://github.com/nidhaloff/deep-translator) — Google Translate wrapper
- [Fish Audio](https://fish.audio) — human-quality TTS & voice cloning
- [edge-tts](https://github.com/rany2/edge-tts) — Microsoft Edge Neural TTS
