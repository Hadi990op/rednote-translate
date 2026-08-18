#!/usr/bin/env python3
"""
RedNote Translate — Web API Backend

FastAPI server that downloads RedNote videos, transcribes audio, and translates
the transcript. Uses Server-Sent Events (SSE) for real-time progress updates.

Endpoints:
  GET  /            — Serves the frontend HTML
  POST /api/process — Starts processing, returns a job_id
  GET  /api/status/{job_id} — SSE stream of progress + results
  GET  /api/download/{job_id}/{fmt} — Download results (json/txt/srt)
  GET  /api/jobs    — List all completed jobs
"""

import asyncio
import json
import os
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from pydantic import BaseModel

# ─── Paths ───────────────────────────────────────────────────────────────────
WORKSPACE = Path("/opt/baal-agent/workspace/rednote_downloads")
XHS_DOWNLOADER_PATH = Path("/opt/baal-agent/workspace/XHS-Downloader")
VIDEOS_DIR = WORKSPACE / "videos"
OUTPUT_DIR = WORKSPACE / "output"
WEB_DIR = WORKSPACE / "web"

sys.path.insert(0, str(XHS_DOWNLOADER_PATH))

# ─── Constants ───────────────────────────────────────────────────────────────
SUPPORTED_LANGS = {
    "en": "English", "hi": "Hindi", "ur": "Urdu", "es": "Spanish",
    "fr": "French", "de": "German", "ar": "Arabic", "ja": "Japanese",
    "ko": "Korean", "ru": "Russian", "pt": "Portuguese", "it": "Italian",
    "tr": "Turkish", "id": "Indonesian", "vi": "Vietnamese", "th": "Thai",
    "bn": "Bengali", "pa": "Punjabi", "ta": "Tamil", "te": "Telugu",
    "mr": "Marathi", "gu": "Gujarati", "ne": "Nepali", "fa": "Persian",
    "zh-CN": "Chinese (Simplified)", "zh-TW": "Chinese (Traditional)",
}

WHISPER_MODELS = {
    "tiny": "Fastest, least accurate",
    "base": "Fast, basic accuracy",
    "small": "Good balance (recommended)",
    "medium": "High accuracy",
    "large-v3": "Best accuracy, slowest",
}

# ─── TTS Configuration ─────────────────────────────────────────────────────
# Two TTS engines:
#   1. Fish Audio S2.1 Pro  — cloud API, 83 languages, human-quality voices.
#   2. Edge Neural TTS      — local fallback, fast, reliable.

# Fish Audio voice IDs (curated from voice library for each language/gender)
FISH_VOICES = {
    "hi":    {"female": "4d7609058bd34213b1378b29efbde1f1", "male": "ea7cdc74aeae4b608be27fdc37fdcb05"},
    "ur":    {"female": "e30937c47f8649baa74cc0ddb7b260a8", "male": "c05fda21c3614d5d82b6430f106f9357"},
    # For languages without a specific voice, Fish Audio uses its default voice
    # which handles 83 languages natively — no reference_id needed.
}

# Edge Neural TTS voices (fallback)
TTS_VOICES = {
    "en":    {"female": "en-US-AriaNeural",       "male": "en-US-AndrewNeural"},
    "hi":    {"female": "hi-IN-SwaraNeural",      "male": "hi-IN-MadhurNeural"},
    "ur":    {"female": "ur-PK-UzmaNeural",       "male": "ur-PK-AsadNeural"},
    "es":    {"female": "es-ES-ElviraNeural",     "male": "es-ES-AlvaroNeural"},
    "fr":    {"female": "fr-FR-DeniseNeural",     "male": "fr-FR-HenriNeural"},
    "de":    {"female": "de-DE-KatjaNeural",      "male": "de-DE-ConradNeural"},
    "ar":    {"female": "ar-SA-ZariyahNeural",    "male": "ar-SA-HamedNeural"},
    "ja":    {"female": "ja-JP-NanamiNeural",     "male": "ja-JP-KeitaNeural"},
    "ko":    {"female": "ko-KR-SunHiNeural",      "male": "ko-KR-InJoonNeural"},
    "ru":    {"female": "ru-RU-SvetlanaNeural",   "male": "ru-RU-DmitryNeural"},
    "pt":    {"female": "pt-BR-FranciscaNeural",  "male": "pt-BR-AntonioNeural"},
    "it":    {"female": "it-IT-ElsaNeural",       "male": "it-IT-DiegoNeural"},
    "tr":    {"female": "tr-TR-EmelNeural",       "male": "tr-TR-AhmetNeural"},
    "id":    {"female": "id-ID-GadisNeural",      "male": "id-ID-ArdiNeural"},
    "vi":    {"female": "vi-VN-HoaiMyNeural",     "male": "vi-VN-NamMinhNeural"},
    "th":    {"female": "th-TH-PremwadeeNeural",  "male": "th-TH-NiwatNeural"},
    "bn":    {"female": "bn-IN-TanishaaNeural",   "male": "bn-IN-BashkarNeural"},
    "pa":    {"female": "pa-IN-OjasviNeural",     "male": "pa-IN-VaibhavNeural"},
    "ta":    {"female": "ta-IN-PallaviNeural",    "male": "ta-IN-ValluvarNeural"},
    "te":    {"female": "te-IN-ShrutiNeural",     "male": "te-IN-MohanNeural"},
    "mr":    {"female": "mr-IN-AarohiNeural",     "male": "mr-IN-ManoharNeural"},
    "gu":    {"female": "gu-IN-DhwaniNeural",     "male": "gu-IN-NiranjanNeural"},
    "ne":    {"female": "ne-NP-HemkalaNeural",    "male": "ne-NP-SagarNeural"},
    "fa":    {"female": "fa-IR-DilaraNeural",     "male": "fa-IR-FaridNeural"},
    "zh-CN": {"female": "zh-CN-XiaoxiaoNeural",  "male": "zh-CN-YunyangNeural"},
    "zh-TW": {"female": "zh-TW-HsiaoChenNeural", "male": "zh-TW-YunJheNeural"},
}

# Fish Audio API key
FISH_API_KEY_FILE = Path("/opt/baal-agent/workspace/secrets/fish_audio_api_key.txt")


def _load_fish_api_key() -> str:
    """Load Fish Audio API key from secrets file or env."""
    if FISH_API_KEY_FILE.exists():
        return FISH_API_KEY_FILE.read_text().strip()
    return os.environ.get("FISH_API_KEY", "")

# ─── Job storage (with disk persistence) ─────────────────────────────────────
JOBS_DIR = WORKSPACE / "job_state"
JOBS_DIR.mkdir(parents=True, exist_ok=True)
jobs: dict = {}  # job_id -> { status, progress, logs, result, error, ... }


def _job_file(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.json"


def save_job(job_id: str):
    """Persist job state to disk so it survives server restarts."""
    try:
        with open(_job_file(job_id), "w", encoding="utf-8") as f:
            json.dump(jobs[job_id], f, ensure_ascii=False, default=str)
    except Exception:
        pass  # persistence is best-effort, don't crash the job


def load_jobs_from_disk():
    """Load all persisted jobs on startup."""
    for f in JOBS_DIR.glob("*.json"):
        try:
            with open(f, encoding="utf-8") as fh:
                job = json.load(fh)
                jid = job.get("id", f.stem)
                # If job was processing when server died, mark as failed
                if job.get("status") == "processing":
                    job["status"] = "failed"
                    job["error"] = "Server restarted during processing (likely OOM). Try again with a smaller model."
                    job["failed_at"] = time.time()
                jobs[jid] = job
        except Exception:
            pass


# Load persisted jobs on import
load_jobs_from_disk()


# ─── Pydantic models ─────────────────────────────────────────────────────────
class ProcessRequest(BaseModel):
    url: str
    languages: list[str] = ["en", "hi"]
    model: str = "base"
    voice_gender: str = "female"  # "female" or "male" for TTS
    generate_audio: bool = True    # whether to generate TTS audio
    translation_mode: str = "google"  # "google" (fast) or "natural" (AI drama-style)
    tts_engine: str = "fish"  # "fish" (Fish Audio S2.1 Pro) or "edge" (Edge Neural TTS)
    clone_voice: bool = False  # clone voice from original video audio
    emotion_mode: bool = False  # add emotion tags for drama delivery


# ─── Core processing functions (adapted from CLI script) ──────────────────────
async def download_rednote_video(url: str, log_fn):
    """Download video from RedNote using XHS-Downloader."""
    from source import XHS

    log_fn("download", "Connecting to RedNote...", 10)
    VIDEOS_DIR.mkdir(parents=True, exist_ok=True)

    async with XHS(
        work_path=str(WORKSPACE),
        folder_name="videos",
        download_record=False,
        timeout=30,
        max_retry=3,
        video_download=True,
        image_download=False,
        folder_mode=True,
    ) as xhs:
        result = await xhs.extract(url, download=True)

    if not result or not result[0]:
        raise RuntimeError(
            "Failed to extract video data. The URL may be invalid, expired, "
            "or the post may not be a video post."
        )

    info = result[0]
    if info.get("作品类型") != "视频":
        raise RuntimeError(
            f"This post is type '{info.get('作品类型', 'unknown')}', not a video. "
            "Only video posts can be transcribed."
        )

    log_fn("download", f"Found: {info.get('作品标题', 'N/A')}", 25)

    # Find the downloaded video file
    # XHS-Downloader saves to a folder named "发布时间_作者_标题/发布时间_作者_标题.mp4"
    # But when the file already exists it says "跳过下载" and doesn't return a path.
    # Strategy: try the expected folder, then fall back to most recently modified MP4.
    note_id = info.get("作品ID", "")
    author = info.get("作者昵称", "")
    title = info.get("作品标题", "")

    # Try matching by author+title in folder name (most reliable)
    all_video_files = sorted(
        VIDEOS_DIR.rglob("*.mp4"),
        key=lambda f: f.stat().st_mtime,
        reverse=True
    )
    video_path = None

    # First preference: file whose parent dir contains the author name
    for f in all_video_files:
        if author and author in str(f.parent):
            video_path = f
            break

    # Second preference: most recently modified file (just downloaded or skipped)
    if not video_path and all_video_files:
        video_path = all_video_files[0]

    if not video_path:
        # Fallback: download directly from the URL
        download_url = info.get("下载地址", [])
        if download_url and download_url[0]:
            video_path = VIDEOS_DIR / f"{note_id or 'video'}.mp4"
            subprocess.run(["curl", "-sL", "-o", str(video_path), download_url[0]], check=True)

    if not video_path or not video_path.exists():
        raise RuntimeError("Video file was not found after download.")
    file_size = video_path.stat().st_size / (1024 * 1024)
    log_fn("download", f"Downloaded {file_size:.1f} MB", 35)

    return {
        "info": info,
        "video_path": str(video_path),
        "title": info.get("作品标题", ""),
        "description": info.get("作品描述", ""),
        "author": info.get("作者昵称", ""),
        "note_id": info.get("作品ID", ""),
        "tags": info.get("作品标签", ""),
        "file_size_mb": round(file_size, 1),
    }


def transcribe_video(video_path: str, model_name: str, log_fn, keep_audio: bool = False):
    """Extract audio and transcribe using faster-whisper.

    If keep_audio=True, the extracted audio file is kept (for voice cloning).
    Returns dict with 'segments', 'full_text', 'language', 'duration', and
    optionally 'audio_path'.
    """
    from faster_whisper import WhisperModel

    log_fn("transcribe", "Extracting audio with ffmpeg...", 40)
    audio_path = video_path.rsplit(".", 1)[0] + "_audio.wav"

    result = subprocess.run(
        ["ffmpeg", "-y", "-i", video_path,
         "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
         audio_path],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed:\n{result.stderr}")

    log_fn("transcribe", f"Loading Whisper model '{model_name}'...", 50)
    model = WhisperModel(model_name, device="cpu", compute_type="int8")

    log_fn("transcribe", "Transcribing audio...", 60)
    segments_iter, info = model.transcribe(
        audio_path, language="zh", beam_size=5, vad_filter=True
    )

    segments = []
    full_text_parts = []
    for seg in segments_iter:
        text = seg.text.strip()
        if text:
            segments.append({
                "start": round(seg.start, 2),
                "end": round(seg.end, 2),
                "text": text,
            })
            full_text_parts.append(text)

    full_text = " ".join(full_text_parts)

    result_data = {
        "segments": segments,
        "full_text": full_text,
        "language": info.language,
        "duration": info.duration,
    }

    if keep_audio:
        result_data["audio_path"] = audio_path
        log_fn("transcribe", f"Transcribed {len(segments)} segments ({info.duration:.1f}s), audio kept for cloning", 70)
    else:
        os.remove(audio_path)
        log_fn("transcribe", f"Transcribed {len(segments)} segments ({info.duration:.1f}s)", 70)

    result_data = {
        "segments": segments,
        "full_text": full_text,
        "language": info.language,
        "language_probability": round(info.language_probability, 3),
        "duration": round(info.duration, 1),
    }

    if keep_audio:
        result_data["audio_path"] = audio_path

    return result_data


def translate_text(text: str, target_lang: str) -> str:
    if not text.strip():
        return ""
    from deep_translator import GoogleTranslator
    max_chars = 4500
    if len(text) <= max_chars:
        return GoogleTranslator(source="zh-CN", target=target_lang).translate(text)
    sentences = re.split(r'([。！？\.\!\?])', text)
    chunks, current = [], ""
    for i in range(0, len(sentences) - 1, 2):
        sentence = sentences[i] + (sentences[i + 1] if i + 1 < len(sentences) else "")
        if len(current) + len(sentence) > max_chars:
            if current:
                chunks.append(current)
            current = sentence
        else:
            current += sentence
    if current:
        chunks.append(current)
    translator = GoogleTranslator(source="zh-CN", target=target_lang)
    return " ".join(translator.translate(c) for c in chunks)


# ─── Natural Drama Translation (LLM-powered) ────────────────────────────────
DRAMA_PROMPT_PATH = Path(__file__).parent / "drama_translation_prompt.txt"

# Language code → natural language name for LLM prompt
LANG_DISPLAY = {
    "en": "English", "hi": "Hindi (Devanagari script)", "ur": "Urdu (Nastaliq script)",
    "es": "Spanish", "fr": "French", "de": "German", "ar": "Arabic",
    "ja": "Japanese", "ko": "Korean", "ru": "Russian", "pt": "Portuguese",
    "it": "Italian", "tr": "Turkish", "id": "Indonesian", "vi": "Vietnamese",
    "th": "Thai", "bn": "Bengali", "pa": "Punjabi", "ta": "Tamil",
    "te": "Telugu", "mr": "Marathi", "gu": "Gujarati", "ne": "Nepali",
    "fa": "Persian", "zh-CN": "Chinese (Simplified)", "zh-TW": "Chinese (Traditional)",
}

# Language code → short instruction for romanized output
ROMAN_LANGS = {"roman-ur": "Roman Urdu (Urdu written in Latin letters)",
               "roman-hi": "Roman Hindi (Hindi written in Latin letters)"}


def _get_llm_client():
    """Get an OpenAI-compatible client pointed at the LibertAI API."""
    from openai import AsyncOpenAI
    from dotenv import load_dotenv

    load_dotenv("/opt/baal-agent/app/.env")
    api_key = os.environ.get("LIBERTAI_API_KEY", "")
    if not api_key:
        raise RuntimeError("LIBERTAI_API_KEY not found in .env")
    return AsyncOpenAI(
        base_url="https://api.libertai.io/v1",
        api_key=api_key,
        timeout=120.0,
    )


def _load_drama_prompt() -> str:
    """Load the drama translation system prompt from file."""
    if DRAMA_PROMPT_PATH.exists():
        return DRAMA_PROMPT_PATH.read_text(encoding="utf-8").strip()
    # Fallback inline prompt
    return (
        "You are an expert Chinese-to-Urdu/Hindi translator for drama dialogue. "
        "Translate the meaning and emotion naturally, not word-for-word. "
        "Output ONLY the translated text, no explanations."
    )


async def translate_text_natural(text: str, target_lang: str, log_fn=None) -> str:
    """Translate Chinese text using LLM with natural drama-style translation.

    Falls back to Google Translate if LLM fails.
    """
    if not text.strip():
        return ""

    # Determine target language name
    lang_name = LANG_DISPLAY.get(target_lang) or ROMAN_LANGS.get(target_lang) or target_lang

    # Load the system prompt
    system_prompt = _load_drama_prompt()

    # Build user message
    user_msg = (
        f"Translate the following Chinese text into {lang_name}. "
        f"This is dialogue from a Chinese drama/web series. "
        f"Make it sound like natural spoken dialogue that a real person would say. "
        f"Output ONLY the translation, nothing else.\n\n"
        f"--- CHINESE TEXT ---\n{text}\n--- END ---"
    )

    try:
        client = _get_llm_client()
        response = await client.chat.completions.create(
            model="claw-large",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.3,  # low temp for consistency, slight creativity
            max_tokens=4096,
        )
        result = response.choices[0].message.content.strip()

        # Strip any markdown code fences if present
        if result.startswith("```"):
            lines = result.split("\n")
            result = "\n".join(lines[1:-1] if lines[-1].startswith("```") else lines[1:])

        if log_fn:
            log_fn("translate", f"LLM translation done ({len(result)} chars)", -1)

        return result

    except Exception as e:
        if log_fn:
            log_fn("translate", f"LLM failed ({e}), falling back to Google Translate...", -1)
        # Fallback to Google Translate
        return translate_text(text, target_lang)


async def translate_text_natural_chunked(text: str, target_lang: str, log_fn=None) -> str:
    """Translate long text in chunks using LLM, preserving context between chunks."""
    max_chars = 3000  # smaller chunks for LLM to maintain quality

    if len(text) <= max_chars:
        return await translate_text_natural(text, target_lang, log_fn)

    # Split on sentence boundaries
    sentences = re.split(r'([。！？\.\!\?\n])', text)
    chunks, current = [], ""
    for i in range(0, len(sentences) - 1, 2):
        sentence = sentences[i] + (sentences[i + 1] if i + 1 < len(sentences) else "")
        if len(current) + len(sentence) > max_chars:
            if current:
                chunks.append(current)
            current = sentence
        else:
            current += sentence
    if current:
        chunks.append(current)

    if log_fn:
        log_fn("translate", f"Split into {len(chunks)} chunks for LLM translation", -1)

    # Translate each chunk
    results = []
    for i, chunk in enumerate(chunks):
        if log_fn:
            log_fn("translate", f"Translating chunk {i+1}/{len(chunks)}...", -1)
        translated = await translate_text_natural(chunk, target_lang, log_fn)
        results.append(translated)

    return " ".join(results)


async def translate_transcript(transcript: dict, target_languages: list, log_fn,
                                translation_mode: str = "google",
                                job=None, save_job_fn=None):
    """Translate transcript to target languages.

    Args:
        translation_mode: "google" for fast Google Translate, "natural" for
                         LLM-powered natural drama-style translation.
        job: optional job dict to store intermediate translations for live preview.
        save_job_fn: optional save function to persist intermediate state.
    """
    full_text = transcript["full_text"]
    if not full_text.strip():
        log_fn("translate", "No speech detected, skipping translation", 100)
        return {}

    mode_label = "Natural Drama AI" if translation_mode == "natural" else "Google Translate"
    log_fn("translate", f"Translation mode: {mode_label}", 70)

    translations = {}
    if job is not None:
        job["preview_translations"] = translations
    total = len(target_languages)
    for i, lang_code in enumerate(target_languages):
        lang_name = SUPPORTED_LANGS.get(lang_code, lang_code)
        progress = 70 + int((i / total) * 25)
        log_fn("translate", f"Translating to {lang_name} ({mode_label})...", progress)
        try:
            if translation_mode == "natural":
                translated = await translate_text_natural_chunked(
                    full_text, lang_code,
                    log_fn=lambda s, m, p: None  # suppress chunk-level logs from clobbering progress
                )
            else:
                translated = translate_text(full_text, lang_code)

            translations[lang_code] = {
                "language": lang_name,
                "code": lang_code,
                "text": translated,
                "mode": translation_mode,
            }
            if save_job_fn:
                save_job_fn()
            log_fn("translate", f"✓ {lang_name} done", progress + max(1, int(25 / total)))
        except Exception as e:
            translations[lang_code] = {
                "language": lang_name,
                "code": lang_code,
                "text": "",
                "error": str(e),
            }
            log_fn("translate", f"✗ {lang_name} failed: {e}", progress)
    log_fn("translate", "All translations complete", 100)
    return translations


# ─── Voice Cloning ───────────────────────────────────────────────────────────

async def clone_voice_from_audio(audio_path: str, title: str, log_fn=None) -> str:
    """Clone a voice using Fish Audio API from a reference audio sample.

    Creates a persistent voice model and returns its ID.
    Returns None on failure.
    """
    import httpx

    api_key = _load_fish_api_key()
    if not api_key:
        if log_fn:
            log_fn("tts", "No Fish Audio API key, cannot clone voice", -1)
        return None

    if not os.path.exists(audio_path):
        if log_fn:
            log_fn("tts", f"Audio file not found: {audio_path}", -1)
        return None

    # Fish Audio recommends 10-30s of clean speech for best cloning
    # For short clips, we send the full audio (Fish Audio handles trimming)
    file_size = os.path.getsize(audio_path) / (1024 * 1024)
    if log_fn:
        log_fn("tts", f"🎙️ Cloning voice from {file_size:.1f} MB audio sample...", 0)

    try:
        with open(audio_path, "rb") as f:
            audio_data = f.read()

        # Use multipart/form-data for file upload
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(
                "https://api.fish.audio/model",
                headers={"Authorization": f"Bearer {api_key}"},
                data={
                    "type": "tts",
                    "title": title[:100],
                    "train_mode": "fast",
                    "visibility": "private",
                    "enhance_audio_quality": "true",
                },
                files={
                    "voices": ("reference.wav", audio_data, "audio/wav"),
                },
            )
            resp.raise_for_status()
            result = resp.json()

            voice_id = result.get("_id", "")
            state = result.get("state", "unknown")

            if log_fn:
                log_fn("tts", f"✓ Voice cloned! ID: {voice_id[:12]}... (state: {state})", 100)

            return voice_id if voice_id else None

    except Exception as e:
        if log_fn:
            log_fn("tts", f"Voice cloning failed: {e}", -1)
        return None


# ─── Text-to-Speech ──────────────────────────────────────────────────────────

async def generate_tts_fish(text: str, lang_code: str, voice_gender: str = "female",
                             output_path: str = None, log_fn=None,
                             cloned_voice_id: str = None,
                             emotion_tags: str = None) -> str:
    """Generate speech using Fish Audio S2.1 Pro (human-quality, cloud API).

    Returns path to the generated MP3 file, or None on failure.
    """
    import httpx

    api_key = _load_fish_api_key()
    if not api_key:
        if log_fn:
            log_fn("tts", "No Fish Audio API key, skipping", 100)
        return None

    if not text.strip():
        return None

    if output_path is None:
        output_path = f"/tmp/tts_fish_{uuid.uuid4().hex[:8]}.mp3"

    # Determine which voice to use:
    # 1. Cloned voice (highest priority — from original video speaker)
    # 2. Curated Fish Audio voice for this language/gender
    # 3. Fish Audio default voice (no reference_id)
    if cloned_voice_id:
        reference_id = cloned_voice_id
    else:
        voice_ref = FISH_VOICES.get(lang_code, {})
        reference_id = voice_ref.get(voice_gender, voice_ref.get("female", ""))

    # Build the text — prepend emotion tags if provided (drama mode)
    tts_text = text
    if emotion_tags:
        # Fish Audio S2 uses [bracket] syntax for emotions
        # e.g. "[excited] This is amazing!"
        tts_text = f"{emotion_tags} {text}"

    payload = {
        "text": tts_text,
        "format": "mp3",
        "latency": "normal",  # more stable for long text
    }
    if reference_id:
        payload["reference_id"] = reference_id

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "model": "s2.1-pro-free",
    }

    if log_fn:
        lang_name = SUPPORTED_LANGS.get(lang_code, lang_code)
        voice_desc = f"voice={reference_id[:8]}..." if reference_id else "default voice"
        log_fn("tts", f"🐟 Fish Audio: {lang_name} ({voice_gender}, {voice_desc})...", 0)

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(
                "https://api.fish.audio/v1/tts",
                json=payload,
                headers=headers,
            )
            resp.raise_for_status()

            with open(output_path, "wb") as f:
                f.write(resp.content)

            file_size = len(resp.content) / 1024
            if log_fn:
                log_fn("tts", f"✓ Fish Audio: {lang_name} done ({file_size:.0f} KB)", 100)

            return output_path

    except Exception as e:
        if log_fn:
            log_fn("tts", f"Fish Audio failed ({e}), will try Edge TTS fallback...", -1)
        return None


async def generate_tts_edge(text: str, lang_code: str, voice_gender: str = "female",
                              output_path: str = None, log_fn=None) -> str:
    """Generate speech using Microsoft Edge Neural TTS (local fallback).

    Returns path to the generated MP3 file, or None on failure.
    """
    import edge_tts

    if not text.strip():
        return None

    # Pick the best voice for this language
    voices = TTS_VOICES.get(lang_code)
    if not voices:
        for k, v in TTS_VOICES.items():
            if k.startswith(lang_code) or lang_code.startswith(k):
                voices = v
                break
    if not voices:
        if log_fn:
            log_fn("tts", f"No voice available for '{lang_code}', skipping", 100)
        return None

    voice = voices.get(voice_gender, voices.get("female", voices.get("male")))
    if not voice:
        return None

    if output_path is None:
        output_path = f"/tmp/tts_edge_{uuid.uuid4().hex[:8]}.mp3"

    if log_fn:
        lang_name = SUPPORTED_LANGS.get(lang_code, lang_code)
        log_fn("tts", f"Edge TTS: {lang_name} ({voice_gender})...", 0)

    try:
        communicate = edge_tts.Communicate(text, voice)
        await communicate.save(output_path)

        if log_fn:
            log_fn("tts", f"✓ Edge TTS: {lang_name} done", 100)

        return output_path
    except Exception as e:
        if log_fn:
            log_fn("tts", f"Edge TTS failed: {e}", 100)
        return None


async def generate_tts(text: str, lang_code: str, voice_gender: str = "female",
                       output_path: str = None, log_fn=None,
                       engine: str = "fish",
                       cloned_voice_id: str = None,
                       emotion_tags: str = None) -> str:
    """Generate natural-sounding speech from text.

    Args:
        engine: "fish" for Fish Audio S2.1 Pro (default), "edge" for Edge Neural TTS.
        cloned_voice_id: if set, use this cloned voice ID instead of curated voices.
        emotion_tags: if set, prepend emotion tags for drama delivery (Fish Audio only).
    """
    if not text.strip():
        if log_fn:
            log_fn("tts", "No text to synthesize, skipping", 100)
        return None

    if engine == "fish":
        # Try Fish Audio first, fall back to Edge TTS on failure
        result = await generate_tts_fish(
            text, lang_code, voice_gender, output_path, log_fn,
            cloned_voice_id=cloned_voice_id,
            emotion_tags=emotion_tags,
        )
        if result:
            return result
        if log_fn:
            log_fn("tts", f"Falling back to Edge TTS for {lang_code}...", -1)
        return await generate_tts_edge(text, lang_code, voice_gender, output_path, log_fn)
    else:
        return await generate_tts_edge(text, lang_code, voice_gender, output_path, log_fn)


def _on_audio_ready(job_id: str, job: dict, lang_code: str, audio_path: str):
    """Callback when a TTS audio file is ready — stores it for live preview."""
    job["audio_files"][lang_code] = audio_path
    save_job(job_id)


# ─── Emotion Detection for Drama Mode ─────────────────────────────────────────

# Keyword patterns for detecting emotions in translated text.
# Fish Audio S2 emotion tags: https://docs.fish.audio/developer-guide/core-features/emotions
EMOTION_PATTERNS = [
    # [excited] — exclamations, celebration, surprise joy
    (r'[!！]|wow|amazing|incredible|awesome|太棒了|太好了|wah|哇|amazing|بہت|زبردست|शानदार|कमाल', '[excited]'),
    # [angry] — frustration, anger
    (r'stupid|idiot|hate|damn|shut up|fool|بےوقوف|نامرد|मूर्ख|चुप|हट', '[angry]'),
    # [sad] — sadness, loss, sorrow
    (r'sad|sorry|miss you|lost|gone|cry|tears|افسوس|غم|दुख|उदास|खो|रोन', '[sad]'),
    # [scared] — fear, horror
    (r'scared|afraid|fear|terrified|horror|run|danger|ڈر|خوف|खौफ|डर|भाग|खतरा', '[scared]'),
    # [whispering] — secrets, quiet, intimate
    (r'whisper|secret|quiet|shh|listen carefully|آہستہ|خفیہ|फुसफुस|राज|धीरे', '[whispering]'),
    # [shouting] — loud, urgent, calling
    (r'(?:^|(?<=[.!?]))\s*(?:hey|stop|wait|look out|watch out|آہستہ|روکو|دیکھو|अरे|रुको|देखो)', '[shouting]'),
    # [laughing] — laughter cues
    (r'haha|hahaha|lol|hehe|ہہہہ|ہاہاہا|हाहा|हाहाहा|ہاہا', '[laughing]'),
    # [curious] — questions
    (r'\?|؟|what|why|how|when|where|who|کیا|کون|کہاں|क्या|कौन|कहां|क्यों|कैसे', '[curious]'),
]


def detect_emotion_tags(text: str) -> str:
    """Detect appropriate Fish Audio emotion tag from text content.

    Returns a bracket tag like '[excited]' or None if no strong emotion detected.
    Uses the first matching pattern — priority order matters.
    """
    import re

    if not text or len(text.strip()) < 3:
        return None

    text_lower = text.lower().strip()

    # Check patterns in priority order
    for pattern, tag in EMOTION_PATTERNS:
        if re.search(pattern, text_lower, re.IGNORECASE):
            return tag

    # Default: no emotion tag for neutral statements
    return None


async def generate_tts_for_translations(translations: dict, output_dir: Path,
                                         voice_gender: str = "female", log_fn=None,
                                         engine: str = "fish",
                                         on_audio_ready=None,
                                         cloned_voice_id: str = None,
                                         emotion_mode: bool = False):
    """Generate TTS audio files for all translations.

    Args:
        on_audio_ready: optional callback(lang_code, path) called when each audio is ready.
        cloned_voice_id: if set, use this cloned voice for all TTS.
        emotion_mode: if True, detect emotion from translation text and add Fish Audio tags.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    audio_files = {}

    total = len(translations)
    for i, (lang_code, trans_data) in enumerate(translations.items()):
        text = trans_data.get("text", "")
        if not text.strip():
            continue

        progress = int((i / max(total, 1)) * 100)

        # Detect emotion tags for drama mode
        emotion_tags = None
        if emotion_mode and engine == "fish":
            emotion_tags = detect_emotion_tags(text)
            if log_fn and emotion_tags:
                log_fn("tts", f"Emotion: {emotion_tags} for {trans_data['language']}", progress)

        if log_fn:
            voice_label = "cloned voice" if cloned_voice_id else voice_gender
            emo_label = f" + {emotion_tags}" if emotion_tags else ""
            log_fn("tts", f"Generating {trans_data['language']} audio ({voice_label}{emo_label})...", progress)

        audio_path = str(output_dir / f"tts_{lang_code}.mp3")
        result = await generate_tts(
            text, lang_code, voice_gender, audio_path,
            log_fn=lambda s, m, p: log_fn(s, m, int(progress + (p / max(total, 1)))) if log_fn else None,
            engine=engine,
            cloned_voice_id=cloned_voice_id,
            emotion_tags=emotion_tags,
        )

        if result:
            audio_files[lang_code] = result
            if on_audio_ready:
                on_audio_ready(lang_code, result)
            if log_fn:
                log_fn("tts", f"✓ {trans_data['language']} audio done", int(progress + (100 / max(total, 1))))

    if log_fn:
        log_fn("tts", "All audio generated", 100)

    return audio_files


def save_results(video_data, transcript, translations, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    note_id = video_data.get("note_id", "video")
    base_name = f"rednote_{note_id}"
    saved = {}

    # JSON
    json_path = output_dir / f"{base_name}.json"
    full_result = {
        "video_info": {
            "title": video_data.get("title"), "description": video_data.get("description"),
            "author": video_data.get("author"), "note_id": note_id,
            "tags": video_data.get("tags"), "video_file": video_data.get("video_path"),
        },
        "transcript": transcript, "translations": translations,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(full_result, f, ensure_ascii=False, indent=2)
    saved["json"] = str(json_path)

    # TXT
    txt_path = output_dir / f"{base_name}.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("=" * 65 + "\nRedNote Video Transcript & Translation\n" + "=" * 65 + "\n\n")
        f.write(f"Title:       {video_data.get('title', 'N/A')}\n")
        f.write(f"Author:      {video_data.get('author', 'N/A')}\n")
        f.write(f"Note ID:     {note_id}\n")
        f.write(f"Tags:        {video_data.get('tags', 'N/A')}\n")
        f.write(f"Description: {video_data.get('description', 'N/A')}\n\n")
        f.write("─" * 65 + "\nORIGINAL TRANSCRIPT (Chinese)\n" + "─" * 65 + "\n")
        for seg in transcript["segments"]:
            f.write(f"[{seg['start']:.1f}s - {seg['end']:.1f}s] {seg['text']}\n")
        f.write(f"\nFull text: {transcript['full_text']}\n\n")
        for lang_code, data in translations.items():
            if data.get("text"):
                f.write("─" * 65 + f"\nTRANSLATION ({data['language']} - {lang_code})\n" + "─" * 65 + f"\n{data['text']}\n\n")
    saved["txt"] = str(txt_path)

    # SRT
    srt_path = output_dir / f"{base_name}.srt"
    with open(srt_path, "w", encoding="utf-8") as f:
        for i, seg in enumerate(transcript["segments"], 1):
            f.write(f"{i}\n{_srt_time(seg['start'])} --> {_srt_time(seg['end'])}\n{seg['text']}\n\n")
    saved["srt"] = str(srt_path)

    return saved


def _srt_time(seconds):
    h, m = int(seconds // 3600), int((seconds % 3600) // 60)
    s, ms = int(seconds % 60), int((seconds % 1) * 1000)
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


# ─── Background processing ───────────────────────────────────────────────────
async def process_job(job_id: str, request: ProcessRequest):
    job = jobs[job_id]
    job["status"] = "processing"
    job["started_at"] = time.time()
    save_job(job_id)

    def log_fn(step, message, progress):
        job["step"] = step
        job["message"] = message
        job["progress"] = progress
        job["logs"].append({
            "time": time.time(),
            "step": step,
            "message": message,
            "progress": progress,
        })

    try:
        # Step 1: Download
        video_data = await download_rednote_video(request.url, log_fn)
        job["video_path"] = video_data["video_path"]
        job["video_info"] = {
            "title": video_data["title"],
            "author": video_data["author"],
            "note_id": video_data["note_id"],
            "tags": video_data["tags"],
            "description": video_data["description"],
            "file_size_mb": video_data["file_size_mb"],
        }

        # Step 2: Transcribe (run in thread to not block event loop)
        # If voice cloning is enabled, keep the extracted audio for cloning
        keep_audio = request.clone_voice and request.generate_audio
        transcript = await asyncio.to_thread(
            transcribe_video, video_data["video_path"], request.model, log_fn,
            keep_audio=keep_audio,
        )
        job["transcript"] = transcript
        job["preview_transcript"] = transcript  # available for live preview
        save_job(job_id)

        # Step 2b: Clone voice from original audio (if enabled)
        cloned_voice_id = None
        if request.clone_voice and request.generate_audio and transcript.get("audio_path"):
            audio_path = transcript["audio_path"]
            voice_title = f"RedNote: {video_data.get('title', 'Unknown')[:50]}"
            cloned_voice_id = await clone_voice_from_audio(audio_path, voice_title, log_fn)
            job["cloned_voice_id"] = cloned_voice_id
            # Clean up the audio file after cloning
            if os.path.exists(audio_path):
                os.remove(audio_path)
            if not cloned_voice_id and log_fn:
                log_fn("tts", "Voice cloning failed — will use preset voices", -1)
            save_job(job_id)

        # Step 3: Translate
        # Natural mode uses async LLM calls; Google mode is sync but wrapped for uniformity
        translations = await translate_transcript(
            transcript, request.languages, log_fn, request.translation_mode,
            job=job, save_job_fn=lambda: save_job(job_id),
        )
        job["translations"] = translations
        save_job(job_id)

        # Save results
        saved = save_results(video_data, transcript, translations, OUTPUT_DIR)

        # Step 4: Generate TTS audio (if enabled)
        if request.generate_audio:
            # Update progress range: translation was 70-100, now shift to make room
            tts_dir = OUTPUT_DIR / f"tts_{job_id}"
            # Store audio_files in job for live preview as they complete
            job["audio_files"] = {}
            job["preview_audio"] = {}
            save_job(job_id)
            audio_files = await generate_tts_for_translations(
                translations, tts_dir, request.voice_gender, log_fn,
                engine=request.tts_engine,
                on_audio_ready=lambda lang_code, path: _on_audio_ready(job_id, job, lang_code, path),
                cloned_voice_id=cloned_voice_id,
                emotion_mode=request.emotion_mode,
            )
            job["audio_files"] = audio_files
            job["tts_engine"] = request.tts_engine
            # Add audio files to downloadable files
            for lang_code, audio_path in audio_files.items():
                saved[f"audio_{lang_code}"] = audio_path

        job["files"] = saved
        job["status"] = "completed"
        job["progress"] = 100
        job["completed_at"] = time.time()
        save_job(job_id)

    except Exception as e:
        import traceback
        job["status"] = "failed"
        job["error"] = str(e)
        job["traceback"] = traceback.format_exc()
        job["failed_at"] = time.time()
        save_job(job_id)


# ─── FastAPI app ──────────────────────────────────────────────────────────────
app = FastAPI(title="RedNote Translate")


@app.get("/api/config")
async def get_config():
    """Return available languages and models for the frontend."""
    return {
        "languages": SUPPORTED_LANGS,
        "models": WHISPER_MODELS,
        "tts_voices": list(TTS_VOICES.keys()),
        "translation_modes": {
            "google": "Google Translate (fast, basic)",
            "natural": "Natural Drama AI (slow, natural dialogue)",
        },
        "tts_engines": {
            "fish": "Fish Audio S2.1 Pro (human-quality, cloud)",
            "edge": "Edge Neural TTS (fast, local fallback)",
        },
    }


@app.post("/api/process")
async def start_processing(request: ProcessRequest):
    """Start a new processing job. Returns job_id."""
    # Validate languages
    invalid = [l for l in request.languages if l not in SUPPORTED_LANGS]
    if invalid:
        raise HTTPException(400, f"Unsupported languages: {invalid}")

    if request.model not in WHISPER_MODELS:
        raise HTTPException(400, f"Unsupported model: {request.model}")

    if not request.url.strip():
        raise HTTPException(400, "URL is required")

    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {
        "id": job_id,
        "status": "queued",
        "progress": 0,
        "step": "",
        "message": "",
        "logs": [],
        "url": request.url,
        "languages": request.languages,
        "model": request.model,
        "voice_gender": request.voice_gender,
        "generate_audio": request.generate_audio,
        "translation_mode": request.translation_mode,
        "tts_engine": request.tts_engine,
        "audio_files": {},
        "created_at": time.time(),
    }

    asyncio.create_task(process_job(job_id, request))
    save_job(job_id)
    return {"job_id": job_id}


@app.get("/api/status/{job_id}")
async def job_status_sse(job_id: str):
    """SSE stream of job progress updates."""
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")

    async def event_stream():
        last_progress = -1
        last_log_count = 0
        job = jobs[job_id]

        while True:
            job = jobs[job_id]

            # Send progress update
            data = {
                "id": job["id"],
                "status": job["status"],
                "progress": job["progress"],
                "step": job["step"],
                "message": job["message"],
            }

            # Include new logs
            new_logs = job["logs"][last_log_count:]
            if new_logs:
                data["new_logs"] = new_logs
                last_log_count = len(job["logs"])

            # Live preview: send intermediate results while processing
            if job["status"] == "processing":
                # Video info (available after download step)
                if job.get("video_info"):
                    data["video_info"] = job["video_info"]
                # Transcript (available after transcription step)
                if job.get("preview_transcript"):
                    data["preview_transcript"] = job["preview_transcript"]
                # Translations (available incrementally during translation step)
                if job.get("preview_translations"):
                    data["preview_translations"] = job["preview_translations"]
                # Audio files ready so far (available during TTS step)
                audio_so_far = job.get("audio_files", {})
                if audio_so_far:
                    data["preview_audio"] = {
                        lang: f"api/download/{job_id}/audio_{lang}"
                        for lang in audio_so_far.keys()
                    }

            # Include results when complete
            if job["status"] == "completed":
                data["video_info"] = job.get("video_info", {})
                data["transcript"] = job.get("transcript", {})
                data["translations"] = job.get("translations", {})
                data["has_video"] = bool(job.get("video_path") and Path(job["video_path"]).exists())
                # Audio files info: map lang_code -> download endpoint
                audio_files = job.get("audio_files", {})
                data["audio_files"] = {
                    lang: f"api/download/{job_id}/audio_{lang}"
                    for lang in audio_files.keys()
                }
                data["audio_available"] = len(audio_files) > 0
                data["elapsed"] = round(job.get("completed_at", 0) - job.get("started_at", 0), 1)
                yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
                break

            if job["status"] == "failed":
                data["error"] = job.get("error", "Unknown error")
                yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
                break

            yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/jobs")
async def list_jobs():
    """List all jobs."""
    return [
        {
            "id": j["id"],
            "status": j["status"],
            "url": j["url"],
            "progress": j["progress"],
            "title": j.get("video_info", {}).get("title", ""),
            "created_at": j["created_at"],
        }
        for j in sorted(jobs.values(), key=lambda x: x["created_at"], reverse=True)
    ]


@app.get("/api/download/{job_id}/{fmt}")
async def download_result(job_id: str, fmt: str):
    """Download result file (json, txt, srt, or video)."""
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    job = jobs[job_id]
    if job["status"] != "completed":
        raise HTTPException(400, "Job not completed")

    # Video download
    if fmt == "video":
        video_path = job.get("video_path")
        if not video_path or not Path(video_path).exists():
            raise HTTPException(404, "Video file not found")
        return FileResponse(
            video_path,
            filename=Path(video_path).name,
            media_type="video/mp4",
        )

    if fmt not in job.get("files", {}):
        raise HTTPException(404, f"File format '{fmt}' not available")

    file_path = Path(job["files"][fmt])
    if not file_path.exists():
        raise HTTPException(404, "File not found")

    # Set proper content type for audio files
    if fmt.startswith("audio_"):
        media_type = "audio/mpeg"
    else:
        media_type = "application/octet-stream"

    return FileResponse(
        file_path,
        filename=file_path.name,
        media_type=media_type,
    )


@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the frontend."""
    index_path = WEB_DIR / "index.html"
    if index_path.exists():
        return HTMLResponse(index_path.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>Frontend not found</h1>", status_code=404)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=9100)
