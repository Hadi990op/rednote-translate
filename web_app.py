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
    dub_video: bool = True  # compose final dubbed video (segment-synced)


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
DRAMA_PROMPT_PATH = Path(__file__).parent / "tts_friendly_prompt.txt"

# Language code → natural language name for LLM prompt
LANG_DISPLAY = {
    "en": "English",
    "hi": "Hindi (Devanagari script)",
    "ur": "Urdu (Nastaliq script)",
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


# ─── LLM Providers ──────────────────────────────────────────────────────────
# Primary: Agnes AI (agnes-2.0-flash) — requires API key
# Fallback: LLM7.io (deepseek-v3) — free, no API key
# Final fallback: Google Translate

AGNES_BASE_URL = "https://apihub.agnes-ai.com/v1"
AGNES_MODEL = "agnes-2.0-flash"
AGNES_API_KEY_PATH = Path("/opt/baal-agent/workspace/secrets/agnes_api_key.txt")

LLM7_BASE_URL = "https://api.llm7.io/v1"
LLM7_MODEL = "deepseek-v3"


def _get_agnes_api_key() -> str | None:
    """Load Agnes AI API key from secrets file."""
    try:
        if AGNES_API_KEY_PATH.exists():
            key = AGNES_API_KEY_PATH.read_text(encoding="utf-8").strip()
            if key and key.startswith("sk-"):
                return key
    except Exception:
        pass
    return None


def _get_llm_client():
    """Get an OpenAI-compatible client for natural drama translation.

    Tries Agnes AI first (if API key available), falls back to LLM7.io (free).

    Returns: (client, model_name)
    """
    from openai import AsyncOpenAI

    # Primary: Agnes AI
    agnes_key = _get_agnes_api_key()
    if agnes_key:
        return (
            AsyncOpenAI(
                base_url=AGNES_BASE_URL,
                api_key=agnes_key,
                timeout=45.0,
            ),
            AGNES_MODEL,
        )

    # Fallback: LLM7.io (free, no key)
    return (
        AsyncOpenAI(
            base_url=LLM7_BASE_URL,
            api_key="unused",  # LLM7.io doesn't require a key, but SDK needs one
            timeout=30.0,  # short timeout — if LLM7.io is slow/rate-limited, fall back fast
        ),
        LLM7_MODEL,
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


async def _llm_chat_with_retry(client, model_name, messages, log_fn=None,
                                max_retries=2, base_delay=10, **kwargs):
    """Call LLM with retry on rate-limit (429) or timeout.

    LLM7.io has a concurrent request limit. On 429 or timeout, we wait
    (with backoff) and retry. Falls through to caller's
    exception handler if all retries fail, which then uses Google Translate.
    """
    import asyncio
    last_err = None
    for attempt in range(max_retries):
        try:
            response = await client.chat.completions.create(
                model=model_name, messages=messages, **kwargs,
            )
            return response
        except Exception as e:
            last_err = e
            err_str = str(e).lower()
            # Retry on rate-limit, timeout, or connection errors
            should_retry = (
                "429" in err_str or
                "rate" in err_str or
                "timeout" in err_str or
                "timed out" in err_str or
                "connection" in err_str or
                "concurrent" in err_str
            )
            if not should_retry or attempt == max_retries - 1:
                raise
            delay = base_delay * (attempt + 1)  # 10, 20 seconds
            if log_fn:
                log_fn("translate",
                       f"LLM rate-limit/timeout (attempt {attempt+1}/{max_retries}), "
                       f"retrying in {delay}s...", -1)
            await asyncio.sleep(delay)
    raise last_err


def _is_mostly_chinese(text: str) -> bool:
    """Check if text is mostly Chinese characters (echo-back bug detection).

    Returns True if >30% of the non-whitespace characters are CJK ideographs,
    which means the LLM echoed back Chinese instead of translating.
    """
    if not text.strip():
        return False
    cjk_count = sum(1 for c in text if '\u4e00' <= c <= '\u9fff')
    total = len(text.strip())
    return total > 0 and (cjk_count / total) > 0.3


# Global flag: once LLM7.io fails, skip LLM for rest of the job and use Google
_LLM_DISABLED = False


async def translate_text_natural(text: str, target_lang: str, log_fn=None) -> str:
    """Translate Chinese text using LLM with natural drama-style translation.

    Falls back to Google Translate if LLM fails.

    """
    global _LLM_DISABLED
    if not text.strip():
        return ""

    # If LLM was disabled due to repeated failures, use Google directly
    if _LLM_DISABLED:
        return translate_text(text, target_lang)

    # Determine target language name
    lang_name = LANG_DISPLAY.get(target_lang) or ROMAN_LANGS.get(target_lang) or target_lang

    # Load the system prompt
    system_prompt = _load_drama_prompt()

    # Build user message — emphasize output language to prevent Chinese echo-back
    user_msg = (
        f"Translate the following Chinese text into {lang_name}. "
        f"This is dialogue from a Chinese drama/web series. "
        f"Make it sound like natural spoken dialogue that a real person would say. "
        f"CRITICAL: Your output MUST be written in {lang_name}. "
        f"Do NOT output Chinese. Do NOT include any Chinese characters in your response. "
        f"Output ONLY the {lang_name} translation, nothing else — no explanations, no notes.\n\n"
        f"--- CHINESE TEXT TO TRANSLATE ---\n{text}\n--- END OF CHINESE TEXT ---"
    )

    try:
        client, model_name = _get_llm_client()
        if log_fn:
            log_fn("translate", f"Using AI model: {model_name}", -1)
        response = await _llm_chat_with_retry(
            client, model_name,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_msg},
            ],
            log_fn=log_fn,
            temperature=0.3,  # low temp for consistency, slight creativity
            max_tokens=8192,  # DeepSeek-V3 uses reasoning tokens; 4096 is too small
        )
        result = response.choices[0].message.content.strip()

        # Strip any markdown code fences if present
        if result.startswith("```"):
            lines = result.split("\n")
            result = "\n".join(lines[1:-1] if lines[-1].startswith("```") else lines[1:])

        # Guard: if LLM returned mostly Chinese (echo-back bug), fall back to Google
        if result and _is_mostly_chinese(result):
            if log_fn:
                log_fn("translate", "AI returned Chinese (echo-back), using Google Translate fallback...", -1)
            return translate_text(text, target_lang)

        if log_fn:
            log_fn("translate", f"AI translation done ({len(result)} chars)", -1)

        return result

    except Exception as e:
        _LLM_DISABLED = True  # disable LLM for rest of this job
        if log_fn:
            log_fn("translate", f"AI failed ({e}), falling back to Google Translate (LLM disabled for this job)...", -1)
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


async def translate_segments(segments: list, target_lang: str, log_fn=None) -> list:
    """Translate each Whisper segment individually so the translated text aligns
    1:1 with the original timing. Returns a list of dicts:
        [{"start":..,"end":..,"text": original,"translated": translated}, ...]

    Batches short consecutive segments together for the LLM call to keep context,
    then splits the result back to match the number of source segments.
    """
    global _LLM_DISABLED
    if not segments:
        return []

    lang_name = LANG_DISPLAY.get(target_lang) or ROMAN_LANGS.get(target_lang) or target_lang

    # Batch consecutive segments up to ~500 chars of Chinese text
    batches = []
    cur_batch, cur_len = [], 0
    for seg in segments:
        seg_len = len(seg.get("text", ""))
        if cur_batch and cur_len + seg_len > 500:
            batches.append(cur_batch)
            cur_batch, cur_len = [], 0
        cur_batch.append(seg)
        cur_len += seg_len
    if cur_batch:
        batches.append(cur_batch)

    if log_fn:
        log_fn("translate", f"Translating {len(segments)} segments in {len(batches)} batches...", -1)

    out = []
    for bi, batch in enumerate(batches):
        # If LLM is disabled (previous failures), use Google Translate directly
        if _LLM_DISABLED:
            for si, seg in enumerate(batch):
                out.append({
                    "start": seg["start"],
                    "end": seg["end"],
                    "text": seg["text"],
                    "translated": translate_text(seg["text"], target_lang),
                })
            if log_fn and bi == 0:
                log_fn("translate", f"LLM disabled — using Google Translate for all segments", -1)
            continue

        # Number each segment in the batch so we can split the LLM response
        # Include duration so LLM can calibrate translation length
        lines = []
        for si, seg in enumerate(batch):
            seg_dur = seg["end"] - seg["start"]
            text = seg['text'].strip()
            lines.append(f"[{si+1}] ({seg_dur:.1f}s) {text}")
        numbered_input = "\n".join(lines)

        prompt_msg = (
            f"Translate each numbered Chinese line below into {lang_name}. "
            f"Keep the SAME numbering format [1] [2] etc. — one translated line per source line, "
            f"in the SAME ORDER. Do NOT merge or skip lines. "
            f"Output ONLY the numbered translations, nothing else.\n\n"
            f"IMPORTANT: The number in parentheses (e.g. '(2.3s)') shows the SPEAKING TIME of that line. "
            f"Use it to calibrate your translation length — keep it SHORT ENOUGH to be spoken in that time. "
            f"Chinese is very compact, Urdu/Hindi is longer. You MUST compress the meaning. "
            f"Use the SHORTEST natural phrasing. Drop filler words. Never exceed the time budget.\n"
            f"CRITICAL: Do NOT include the time number in your output. "
            f"Output format must be: [1] translated text only. NOT [1] (2.3s) translated text.\n\n"
            f"{numbered_input}"
        )

        try:
            client, model_name = _get_llm_client()
            system_prompt = _load_drama_prompt()
            response = await _llm_chat_with_retry(
                client, model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt_msg},
                ],
                log_fn=log_fn,
                temperature=0.3,
                max_tokens=8192,
            )
            raw = response.choices[0].message.content.strip()
        except Exception as e:
            _LLM_DISABLED = True  # disable LLM for rest of job
            if log_fn:
                log_fn("translate", f"Batch {bi+1} LLM failed ({e}), using Google Translate...", -1)
            raw = None

        # Parse numbered lines from response → dict {1: text, 2: text, ...}
        translated_map = {}
        if raw:
            for m in re.finditer(r'\[(\d+)\]\s*(.+)', raw):
                text = m.group(2).strip()
                # Strip any leading (Xs) duration prefix that LLM might include
                text = re.sub(r'^\([\d.]+s\)\s*', '', text)
                translated_map[int(m.group(1))] = text

        # Fallback: if parsing failed or count mismatch, use Google Translate per segment
        if len(translated_map) != len(batch):
            if log_fn:
                log_fn("translate", f"Batch {bi+1}: parse mismatch ({len(translated_map)}/{len(batch)}), using Google fallback", -1)
            for si, seg in enumerate(batch):
                translated_map[si + 1] = translate_text(seg["text"], target_lang)

        # Emit aligned output
        for si, seg in enumerate(batch):
            out.append({
                "start": seg["start"],
                "end": seg["end"],
                "text": seg["text"],
                "translated": translated_map.get(si + 1, ""),
            })

        if log_fn:
            log_fn("translate", f"Batch {bi+1}/{len(batches)} done", -1)

    return out


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
            # Also produce segment-aligned translations for dubbing
            # (works in both natural and google mode — LLM with Google fallback)
            if transcript.get("segments"):
                try:
                    seg_translations = await translate_segments(
                        transcript["segments"], lang_code,
                        log_fn=lambda s, m, p: None,
                    )
                    translations[lang_code]["segments"] = seg_translations
                except Exception as seg_err:
                    if log_fn:
                        log_fn("translate", f"Segment alignment failed for {lang_name}: {seg_err}", -1)
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
                             emotion_tags: str = None,
                             speed: float = 1.0) -> str:
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

    # Clamp speed to Fish Audio's accepted range (0.5 - 2.0)
    speed = max(0.5, min(2.0, speed))

    payload = {
        "text": tts_text,
        "format": "mp3",
        "latency": "normal",  # more stable for long text
    }
    if reference_id:
        payload["reference_id"] = reference_id

    # Add prosody speed control if not default
    if abs(speed - 1.0) > 0.05:
        payload["prosody"] = {"speed": speed}

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
                              output_path: str = None, log_fn=None,
                              speed: float = 1.0) -> str:
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
        # Edge TTS rate: -100% to +100%, 0 = normal
        rate_percent = int((speed - 1.0) * 100)
        rate_str = f"{rate_percent:+d}%"
        communicate = edge_tts.Communicate(text, voice, rate=rate_str)
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
                       emotion_tags: str = None,
                       speed: float = 1.0) -> str:
    """Generate natural-sounding speech from text.

    Args:
        engine: "fish" for Fish Audio S2.1 Pro (default), "edge" for Edge Neural TTS.
        cloned_voice_id: if set, use this cloned voice ID instead of curated voices.
        emotion_tags: if set, prepend emotion tags for drama delivery (Fish Audio only).
        speed: TTS speech rate (0.5-2.0, 1.0=normal). Fish Audio uses prosody.speed.
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
            speed=speed,
        )
        if result:
            return result
        if log_fn:
            log_fn("tts", f"Falling back to Edge TTS for {lang_code}...", -1)
        return await generate_tts_edge(text, lang_code, voice_gender, output_path, log_fn, speed=speed)
    else:
        return await generate_tts_edge(text, lang_code, voice_gender, output_path, log_fn, speed=speed)


def _on_audio_ready(job_id: str, job: dict, lang_code: str, audio_path: str):
    """Callback when a TTS audio file is ready — stores it for live preview."""
    job["audio_files"][lang_code] = audio_path
    save_job(job_id)


# ─── Dubbing: per-segment TTS + time-stretch + video compose ──────────────────

def _probe_audio_duration(path: str) -> float:
    """Return audio duration in seconds using ffprobe."""
    try:
        out = subprocess.check_output(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
             "-of", "default=nw=1:nk=1", path],
            stderr=subprocess.DEVNULL,
        )
        return float(out.decode().strip())
    except Exception:
        return 0.0


def _stretch_audio(input_path: str, target_duration: float, output_path: str,
                    max_ratio: float = 1.25, min_ratio: float = 0.8) -> bool:
    """Time-stretch (or shrink) audio to target_duration, but ONLY with gentle ratios.

    Uses ffmpeg's atempo filter. Only stretches within [min_ratio, max_ratio] to
    preserve voice quality. Outside that range, returns False (caller should handle
    via TTS speed or overflow). Preserves pitch. Returns True on success.
    """
    src_dur = _probe_audio_duration(input_path)
    if src_dur <= 0 or target_duration <= 0:
        return False

    ratio = target_duration / src_dur
    # If already close enough, just copy
    if 0.97 <= ratio <= 1.03:
        import shutil
        shutil.copy(input_path, output_path)
        return True

    # Only apply gentle atempo — extreme ratios sound robotic
    if ratio > max_ratio or ratio < min_ratio:
        return False

    atempo = f"atempo={ratio:.4f}"
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-i", input_path, "-filter:a", atempo,
             "-vn", output_path],
            check=True, capture_output=True, timeout=60,
        )
        return Path(output_path).exists()
    except Exception:
        return False


def _estimate_tts_duration(text: str, lang_code: str) -> float:
    """Rough estimate of how long TTS will take for given text.
    Based on empirical testing with Fish Audio:
    - Urdu/Hindi Devanagari: ~13 chars/sec (Fish Audio speaks fast)
    - Latin scripts: ~15 chars/sec
    """
    if not text:
        return 0.0
    # Strip emotion tags
    import re
    clean = re.sub(r'\[.*?\]', '', text).strip()
    # Urdu/Hindi/Arabic scripts — measured ~13 chars/sec with Fish Audio
    # Latin scripts — ~15 chars/sec
    rate = 13.0 if any(ord(c) > 0x0900 for c in clean) else 15.0
    return len(clean) / rate


def _compute_tts_speed(target_duration: float, text: str, lang_code: str) -> float:
    """Compute optimal TTS speed so the generated audio matches target_duration.

    Strategy:
    - Estimate how long TTS will take at normal speed (1.0)
    - If estimated > target, speed up TTS (up to 1.8x)
    - If estimated < target, slow down TTS (down to 0.7x)
    - Leave a small margin for atempo fine-tuning
    """
    est_dur = _estimate_tts_duration(text, lang_code)
    if est_dur <= 0 or target_duration <= 0:
        return 1.0

    # We want: est_dur / speed ≈ target_duration (with 5% margin)
    # So speed = est_dur / target_duration
    speed = est_dur / target_duration

    # Clamp to Fish Audio's useful range
    # 0.7 = noticeable slow, 1.8 = fast but not chipmunk
    speed = max(0.7, min(1.8, speed))
    return round(speed, 2)


async def generate_dubbed_audio(segments: list, lang_code: str, voice_gender: str,
                                  output_dir: Path, log_fn=None,
                                  engine: str = "fish", cloned_voice_id: str = None,
                                  emotion_mode: bool = False,
                                  on_progress=None) -> str:
    """Generate a single synced dub audio track from segment-aligned translations.

    Smart approach for natural-sounding dub:
      1. Estimate TTS duration, set prosody.speed so raw TTS ≈ original segment duration
      2. Apply gentle atempo fine-tuning (0.8x–1.25x) only if needed
      3. Allow slight overflow into inter-segment gaps (no hard cuts)
      4. For segments with large gaps, let TTS play naturally (no extreme stretching)

    Returns path to the final mixed audio file, or None on failure.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Compute available gap after each segment (for overflow tolerance)
    seg_count = len(segments)

    # First pass: generate TTS for each segment with adaptive speed
    seg_clips = []  # list of (start, clip_path, duration)
    total = len(segments)
    for i, seg in enumerate(segments):
        text = seg.get("translated", "").strip()
        if not text:
            continue
        seg_dur = seg["end"] - seg["start"]
        if seg_dur <= 0:
            continue

        emotion_tags = None
        if emotion_mode and engine == "fish":
            emotion_tags = detect_emotion_tags(text)

        # Compute TTS speed to match original segment duration
        tts_speed = _compute_tts_speed(seg_dur, text, lang_code)

        raw_path = str(output_dir / f"seg_{i:04d}_raw.mp3")
        stretched_path = str(output_dir / f"seg_{i:04d}.wav")

        result = await generate_tts(
            text, lang_code, voice_gender, raw_path,
            log_fn=None, engine=engine,
            cloned_voice_id=cloned_voice_id,
            emotion_tags=emotion_tags,
            speed=tts_speed,
        )
        if not result:
            if log_fn:
                log_fn("dub", f"Segment {i+1}/{total} TTS failed, skipping", -1)
            continue

        # Check raw TTS duration
        raw_dur = _probe_audio_duration(result)

        # Compute available gap before next segment (overflow tolerance)
        next_start = segments[i + 1]["start"] if i + 1 < seg_count else seg["end"] + 1.0
        available_gap = next_start - seg["start"]  # total available time

        # Target duration: original seg_dur, but allow overflow into gap
        # Use up to 90% of available gap to avoid overlapping next segment
        effective_target = max(seg_dur, min(available_gap * 0.9, seg_dur * 1.5))

        # Try gentle atempo fine-tuning
        if _stretch_audio(result, effective_target, stretched_path):
            final_clip = stretched_path
        else:
            # atempo couldn't do it gently — use raw clip as-is
            # (TTS speed already got it close)
            final_clip = result

        final_dur = _probe_audio_duration(final_clip) if final_clip == stretched_path else raw_dur
        seg_clips.append((seg["start"], final_clip, final_dur))

        if log_fn and (i % 5 == 0 or i == total - 1):
            pct = int((i + 1) / total * 100)
            speed_str = f"speed={tts_speed:.1f}x" if abs(tts_speed - 1.0) > 0.05 else "normal"
            log_fn("dub", f" Dubbed segment {i+1}/{total} ({speed_str})", pct)
        if on_progress:
            on_progress(int((i + 1) / total * 100))

    if not seg_clips:
        if log_fn:
            log_fn("dub", "No segments dubbed", -1)
        return None

    # Second pass: build a single audio track using ffmpeg concat with delays
    # Use a silent base track of full video duration, then overlay each clip at its timestamp
    # Determine total duration from last segment end
    last_seg = segments[-1]
    total_duration = last_seg["end"] + 0.5

    # Build ffmpeg complex filter: silent base + overlay each clip at its start time
    # Use adelay filter for each clip, then amix all together
    inputs = []
    filter_parts = []
    # Silent base track
    base_path = str(output_dir / "_silence_base.wav")
    subprocess.run(
        ["ffmpeg", "-y", "-f", "lavfi", "-i", f"anullsrc=channel_layout=stereo:sample_rate=44100",
         "-t", f"{total_duration}", "-c:a", "pcm_s16le", base_path],
        check=True, capture_output=True, timeout=30,
    )
    inputs.append(f"-i \"{base_path}\"")

    # Add each segment clip as an input with adelay
    for idx, (start, clip, _dur) in enumerate(seg_clips):
        delay_ms = int(start * 1000)
        inputs.append(f"-i \"{clip}\"")
        # adelay applies to the clip; index+1 because base is input 0
        filter_parts.append(f"[{idx+1}:a]adelay={delay_ms}|{delay_ms}[d{idx}]")

    # Mix all delayed clips + base
    # normalize=0 prevents amix from dividing volume by number of inputs
    # (segments are non-overlapping, so no clipping risk)
    mix_inputs = "[0:a]" + "".join(f"[d{idx}]" for idx in range(len(seg_clips)))
    mix_inputs += f"amix=inputs={len(seg_clips)+1}:duration=longest:dropout_transition=0:normalize=0"
    filter_complex = ";".join(filter_parts) + ";" + mix_inputs

    final_path = str(output_dir / f"dub_{lang_code}.wav")
    cmd = f'ffmpeg -y {" ".join(inputs)} -filter_complex "{filter_complex}" -c:a pcm_s16le "{final_path}"'

    if log_fn:
        log_fn("dub", f" Mixing {len(seg_clips)} segments into final track...", 90)
    try:
        subprocess.run(["bash", "-c", cmd], check=True, capture_output=True, timeout=300)
    except Exception as e:
        if log_fn:
            log_fn("dub", f"Mix failed: {e}", -1)
        return None

    if log_fn:
        log_fn("dub", f"✓ Dub audio ready ({len(seg_clips)} segments)", 100)
    return final_path


def compose_dubbed_video(video_path: str, dub_audio_path: str, output_path: str,
                          log_fn=None) -> bool:
    """Replace the original video's audio with the dubbed audio track.

    Uses ffmpeg to mux: original video (no audio) + dubbed audio track.
    Returns True on success.
    """
    try:
        if log_fn:
            log_fn("compose", "🎬 Composing final dubbed video...", 0)
        subprocess.run(
            ["ffmpeg", "-y",
             "-i", video_path,
             "-i", dub_audio_path,
             "-c:v", "copy",
             "-c:a", "aac", "-b:a", "192k",
             "-map", "0:v:0", "-map", "1:a:0",
             "-shortest",
             output_path],
            check=True, capture_output=True, timeout=600,
        )
        ok = Path(output_path).exists()
        if log_fn:
            size_mb = Path(output_path).stat().st_size / (1024 * 1024) if ok else 0
            log_fn("compose", f"✓ Dubbed video ready ({size_mb:.1f} MB)" if ok else "✗ Video compose failed", 100 if ok else -1)
        return ok
    except Exception as e:
        if log_fn:
            log_fn("compose", f"Video compose failed: {e}", -1)
        return False


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
    global _LLM_DISABLED
    _LLM_DISABLED = False  # reset LLM availability for each new job
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

        # Step 4b: Compose dubbed videos (segment-synced, if enabled)
        if request.dub_video and request.generate_audio:
            job["dubbed_videos"] = {}
            save_job(job_id)
            for lang_code, trans_data in translations.items():
                seg_translations = trans_data.get("segments")
                if not seg_translations:
                    if log_fn:
                        log_fn("dub", f"Skipping {lang_code} dub — no segment translations", -1)
                    continue
                lang_name = SUPPORTED_LANGS.get(lang_code, lang_code)
                log_fn("dub", f"🎬 Dubbing {lang_name} ({len(seg_translations)} segments)...", -1)
                dub_dir = OUTPUT_DIR / f"dub_{job_id}"
                dub_audio = await generate_dubbed_audio(
                    seg_translations, lang_code, request.voice_gender,
                    dub_dir, log_fn,
                    engine=request.tts_engine,
                    cloned_voice_id=cloned_voice_id,
                    emotion_mode=request.emotion_mode,
                )
                if not dub_audio:
                    log_fn("dub", f"✗ {lang_name} dub audio failed", -1)
                    continue
                # Compose final video
                dubbed_video_path = str(dub_dir / f"dubbed_{lang_code}.mp4")
                if compose_dubbed_video(video_data["video_path"], dub_audio, dubbed_video_path, log_fn):
                    job["dubbed_videos"][lang_code] = dubbed_video_path
                    saved[f"dubbed_video_{lang_code}"] = dubbed_video_path
                    save_job(job_id)
                else:
                    log_fn("dub", f"✗ {lang_name} video compose failed", -1)

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
                data["tts_engine"] = job.get("tts_engine", "edge")
                # Audio files info: map lang_code -> download endpoint
                audio_files = job.get("audio_files", {})
                data["audio_files"] = {
                    lang: f"api/download/{job_id}/audio_{lang}"
                    for lang in audio_files.keys()
                }
                data["audio_available"] = len(audio_files) > 0
                # Dubbed videos (segment-synced)
                dubbed = job.get("dubbed_videos", {})
                data["dubbed_videos"] = {
                    lang: f"api/download/{job_id}/dubbed_video_{lang}"
                    for lang in dubbed.keys()
                }
                data["dub_available"] = len(dubbed) > 0
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
            headers={"Content-Disposition": f'inline; filename="{Path(video_path).name}"'},
        )

    if fmt not in job.get("files", {}):
        raise HTTPException(404, f"File format '{fmt}' not available")

    file_path = Path(job["files"][fmt])
    if not file_path.exists():
        raise HTTPException(404, "File not found")

    # Set proper content type
    if fmt.startswith("audio_"):
        media_type = "audio/mpeg"
    elif fmt.startswith("dubbed_video_"):
        media_type = "video/mp4"
    else:
        media_type = "application/octet-stream"

    # Use inline so browsers play audio/video in-browser instead of forcing download
    headers = {}
    if media_type.startswith(("audio/", "video/")):
        headers["Content-Disposition"] = f'inline; filename="{file_path.name}"'

    return FileResponse(
        file_path,
        filename=file_path.name,
        media_type=media_type,
        headers=headers,
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
