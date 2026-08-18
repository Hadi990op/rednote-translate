#!/usr/bin/env python3
"""
RedNote (XiaoHongShu) Video Downloader + Transcription + Translation Tool

Usage:
    python3 rednote_translate.py <rednote_url> [--languages en hi] [--model small]
    python3 rednote_translate.py --help

Examples:
    python3 rednote_translate.py "https://www.xiaohongshu.com/explore/6a599abd0000000011010624?xsec_token=ABS-ODPMMvwXROass31ZVHGAxBXWbob0Bj3rvyxMwGBRY="
    python3 rednote_translate.py "https://www.xiaohongshu.com/explore/XXXX?xsec_token=YYYY" --languages en hi ur
    python3 rednote_translate.py "https://xhslink.com/abc123" --languages en --model medium
"""

import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

# ─── Paths ───────────────────────────────────────────────────────────────────
WORKSPACE = Path("/opt/baal-agent/workspace/rednote_downloads")
XHS_DOWNLOADER_PATH = Path("/opt/baal-agent/workspace/XHS-Downloader")
VIDEOS_DIR = WORKSPACE / "videos"
OUTPUT_DIR = WORKSPACE / "output"

# Add XHS-Downloader to path for imports
sys.path.insert(0, str(XHS_DOWNLOADER_PATH))

# ─── Supported translation languages ─────────────────────────────────────────
# Full list: https://cloud.google.com/translate/docs/languages
SUPPORTED_LANGS = {
    "en": "English",
    "hi": "Hindi",
    "ur": "Urdu",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "ar": "Arabic",
    "ja": "Japanese",
    "ko": "Korean",
    "ru": "Russian",
    "pt": "Portuguese",
    "it": "Italian",
    "tr": "Turkish",
    "id": "Indonesian",
    "vi": "Vietnamese",
    "th": "Thai",
    "bn": "Bengali",
    "pa": "Punjabi",
    "ta": "Tamil",
    "te": "Telugu",
    "mr": "Marathi",
    "gu": "Gujarati",
    "ne": "Nepali",
    "fa": "Persian",
    "zh-CN": "Chinese (Simplified)",
    "zh-TW": "Chinese (Traditional)",
}

WHISPER_MODELS = {
    "tiny": "Fastest, least accurate (~1GB RAM)",
    "base": "Fast, basic accuracy (~1GB RAM)",
    "small": "Good balance of speed/accuracy (~2GB RAM)",
    "medium": "High accuracy (~5GB RAM)",
    "large-v3": "Best accuracy (~10GB RAM)",
}


def print_banner():
    print("=" * 65)
    print("  RedNote Video → Transcription → Translation Pipeline")
    print("=" * 65)


# ─── Step 1: Download video from RedNote ──────────────────────────────────────
async def download_rednote_video(url: str, model_name: str = "small") -> dict:
    """Download a video from RedNote/XiaoHongShu using XHS-Downloader.

    Returns dict with video metadata and file path.
    """
    from source import XHS

    print(f"\n📥 [1/3] Downloading video from RedNote...")
    print(f"   URL: {url[:80]}{'...' if len(url) > 80 else ''}")

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
            "or the post may not be a video post. Make sure you use a fresh URL "
            "with a valid xsec_token."
        )

    info = result[0]
    if info.get("作品类型") != "视频":
        raise RuntimeError(
            f"This post is type '{info.get('作品类型', 'unknown')}', not a video. "
            "Only video posts can be transcribed."
        )

    # Find the downloaded video file
    note_id = info.get("作品ID", "")
    author = info.get("作者昵称", "")
    title = info.get("作品标题", "")

    all_video_files = sorted(
        VIDEOS_DIR.rglob("*.mp4"),
        key=lambda f: f.stat().st_mtime,
        reverse=True
    )
    video_path = None

    for f in all_video_files:
        if author and author in str(f.parent):
            video_path = f
            break

    if not video_path and all_video_files:
        video_path = all_video_files[0]

    if not video_path:
        download_url = info.get("下载地址", [])
        if download_url and download_url[0]:
            print(f"   ⚠️  XHS-Downloader didn't save the file, downloading directly...")
            video_path = VIDEOS_DIR / f"{note_id or 'video'}.mp4"
            subprocess.run(
                ["curl", "-sL", "-o", str(video_path), download_url[0]],
                check=True
            )

    if not video_path or not video_path.exists():
        raise RuntimeError("Video file was not found after download.")
    file_size = video_path.stat().st_size / (1024 * 1024)

    print(f"   ✅ Downloaded: {video_path.name}")
    print(f"   📊 Size: {file_size:.1f} MB")
    print(f"   📝 Title: {info.get('作品标题', 'N/A')}")
    print(f"   👤 Author: {info.get('作者昵称', 'N/A')}")

    return {
        "info": info,
        "video_path": str(video_path),
        "title": info.get("作品标题", ""),
        "description": info.get("作品描述", ""),
        "author": info.get("作者昵称", ""),
        "note_id": info.get("作品ID", ""),
        "tags": info.get("作品标签", ""),
    }


# ─── Step 2: Transcribe audio using Whisper ───────────────────────────────────
def transcribe_video(video_path: str, model_name: str = "small") -> dict:
    """Extract audio from video and transcribe using faster-whisper.

    Returns dict with segments and full text.
    """
    from faster_whisper import WhisperModel

    print(f"\n🎤 [2/3] Transcribing audio (model: {model_name})...")

    # Extract audio using ffmpeg
    audio_path = video_path.rsplit(".", 1)[0] + "_audio.wav"
    print(f"   Extracting audio...")

    result = subprocess.run(
        [
            "ffmpeg", "-y", "-i", video_path,
            "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
            audio_path
        ],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed to extract audio:\n{result.stderr}")

    # Transcribe
    print(f"   Loading Whisper model '{model_name}' (first run downloads it)...")
    model = WhisperModel(model_name, device="cpu", compute_type="int8")

    print(f"   Transcribing...")
    segments_iter, info = model.transcribe(
        audio_path,
        language="zh",
        beam_size=5,
        vad_filter=True,  # Skip silence
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

    # Clean up audio file
    os.remove(audio_path)

    print(f"   ✅ Transcribed {len(segments)} segments")
    print(f"   🌐 Detected language: {info.language} ({info.language_probability:.0%})")
    print(f"   ⏱️  Duration: {info.duration:.1f}s")

    if not full_text:
        print(f"   ⚠️  No speech detected in this video (may be music/text-only)")

    return {
        "segments": segments,
        "full_text": full_text,
        "language": info.language,
        "language_probability": info.language_probability,
        "duration": info.duration,
    }


# ─── Step 3: Translate transcript ────────────────────────────────────────────
def translate_text(text: str, target_lang: str) -> str:
    """Translate text using Google Translate (free, no API key needed)."""
    if not text.strip():
        return ""

    from deep_translator import GoogleTranslator

    # Google Translate has a ~5000 char limit per request; chunk if needed
    max_chars = 4500
    if len(text) <= max_chars:
        return GoogleTranslator(source="zh-CN", target=target_lang).translate(text)

    # Split into sentences and batch
    sentences = re.split(r'([。！？\.\!\?])', text)
    chunks = []
    current_chunk = ""

    for i in range(0, len(sentences) - 1, 2):
        sentence = sentences[i] + (sentences[i + 1] if i + 1 < len(sentences) else "")
        if len(current_chunk) + len(sentence) > max_chars:
            if current_chunk:
                chunks.append(current_chunk)
            current_chunk = sentence
        else:
            current_chunk += sentence

    if current_chunk:
        chunks.append(current_chunk)

    translator = GoogleTranslator(source="zh-CN", target=target_lang)
    translated_chunks = []
    for chunk in chunks:
        translated_chunks.append(translator.translate(chunk))

    return " ".join(translated_chunks)


def translate_transcript(transcript: dict, target_languages: list) -> dict:
    """Translate the full transcript text into target languages."""
    from deep_translator import GoogleTranslator

    full_text = transcript["full_text"]

    if not full_text.strip():
        print(f"\n🌍 [3/3] Skipping translation (no speech detected)")
        return {}

    print(f"\n🌍 [3/3] Translating to: {', '.join(target_languages)}")

    translations = {}

    for lang_code in target_languages:
        lang_name = SUPPORTED_LANGS.get(lang_code, lang_code)
        print(f"   Translating to {lang_name} ({lang_code})...")
        try:
            translated = translate_text(full_text, lang_code)
            translations[lang_code] = {
                "language": lang_name,
                "code": lang_code,
                "text": translated,
            }
            # Show preview
            preview = translated[:100] + "..." if len(translated) > 100 else translated
            print(f"   ✅ {lang_name}: {preview}")
        except Exception as e:
            print(f"   ❌ {lang_name}: Translation failed - {e}")
            translations[lang_code] = {
                "language": lang_name,
                "code": lang_code,
                "text": "",
                "error": str(e),
            }

    return translations


# ─── Output formatting ───────────────────────────────────────────────────────
def save_results(video_data: dict, transcript: dict, translations: dict,
                 output_dir: Path) -> list:
    """Save results in multiple formats: JSON, TXT, and SRT."""
    output_dir.mkdir(parents=True, exist_ok=True)
    note_id = video_data.get("note_id", "video")
    base_name = f"rednote_{note_id}"
    saved_files = []

    # ── JSON (complete data) ──
    json_path = output_dir / f"{base_name}.json"
    full_result = {
        "video_info": {
            "title": video_data.get("title"),
            "description": video_data.get("description"),
            "author": video_data.get("author"),
            "note_id": note_id,
            "tags": video_data.get("tags"),
            "video_file": video_data.get("video_path"),
        },
        "transcript": transcript,
        "translations": translations,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(full_result, f, ensure_ascii=False, indent=2)
    saved_files.append(json_path)

    # ── TXT (human-readable) ──
    txt_path = output_dir / f"{base_name}.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("=" * 65 + "\n")
        f.write("RedNote Video Transcript & Translation\n")
        f.write("=" * 65 + "\n\n")

        f.write(f"Title:       {video_data.get('title', 'N/A')}\n")
        f.write(f"Author:      {video_data.get('author', 'N/A')}\n")
        f.write(f"Note ID:     {note_id}\n")
        f.write(f"Tags:        {video_data.get('tags', 'N/A')}\n")
        f.write(f"Description: {video_data.get('description', 'N/A')}\n\n")

        f.write("─" * 65 + "\n")
        f.write("ORIGINAL TRANSCRIPT (Chinese)\n")
        f.write("─" * 65 + "\n")
        for seg in transcript["segments"]:
            f.write(f"[{seg['start']:.1f}s - {seg['end']:.1f}s] {seg['text']}\n")
        f.write(f"\nFull text: {transcript['full_text']}\n\n")

        for lang_code, data in translations.items():
            if data.get("text"):
                f.write("─" * 65 + "\n")
                f.write(f"TRANSLATION ({data['language']} - {lang_code})\n")
                f.write("─" * 65 + "\n")
                f.write(f"{data['text']}\n\n")

    saved_files.append(txt_path)

    # ── SRT subtitle file ──
    srt_path = output_dir / f"{base_name}.srt"
    with open(srt_path, "w", encoding="utf-8") as f:
        for i, seg in enumerate(transcript["segments"], 1):
            start_s = seg["start"]
            end_s = seg["end"]
            f.write(f"{i}\n")
            f.write(f"{_srt_time(start_s)} --> {_srt_time(end_s)}\n")
            f.write(f"{seg['text']}\n\n")
    saved_files.append(srt_path)

    return saved_files


def _srt_time(seconds: float) -> str:
    """Convert seconds to SRT time format: HH:MM:SS,mmm"""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    ms = int((seconds % 1) * 1000)
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Download RedNote videos, transcribe audio, and translate.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s "https://www.xiaohongshu.com/explore/XXXX?xsec_token=YYYY"
  %(prog)s "https://www.xiaohongshu.com/explore/XXXX?xsec_token=YYYY" --languages en hi ur
  %(prog)s "https://xhslink.com/abc123" --languages en --model medium
        """,
    )
    parser.add_argument("url", help="RedNote/XiaoHongShu video URL (with xsec_token)")
    parser.add_argument(
        "--languages", "-l", nargs="+", default=["en", "hi"],
        help=f"Target languages for translation (default: en hi). "
             f"Supported: {', '.join(sorted(SUPPORTED_LANGS.keys()))}",
    )
    parser.add_argument(
        "--model", "-m", default="small",
        choices=WHISPER_MODELS.keys(),
        help=f"Whisper model size (default: small). Options: "
             f"{', '.join(f'{k} ({v})' for k, v in WHISPER_MODELS.items())}",
    )
    parser.add_argument(
        "--output-dir", "-o", default=str(OUTPUT_DIR),
        help=f"Output directory (default: {OUTPUT_DIR})",
    )

    args = parser.parse_args()

    print_banner()

    # Validate languages
    invalid = [l for l in args.languages if l not in SUPPORTED_LANGS]
    if invalid:
        print(f"❌ Unsupported language(s): {', '.join(invalid)}")
        print(f"   Supported: {', '.join(sorted(SUPPORTED_LANGS.keys()))}")
        sys.exit(1)

    start_time = time.time()

    try:
        # Step 1: Download
        video_data = asyncio.run(download_rednote_video(args.url, args.model))

        # Step 2: Transcribe
        transcript = transcribe_video(video_data["video_path"], args.model)

        # Step 3: Translate
        translations = translate_transcript(transcript, args.languages)

        # Save results
        output_dir = Path(args.output_dir)
        saved_files = save_results(video_data, transcript, translations, output_dir)

        elapsed = time.time() - start_time
        print(f"\n{'=' * 65}")
        print(f"✅ Complete! ({elapsed:.1f}s)")
        print(f"{'=' * 65}")
        print(f"\n📁 Output files:")
        for f in saved_files:
            print(f"   • {f}")

        print(f"\n📄 Transcript preview:")
        print(f"   {transcript['full_text'][:200]}")
        if translations:
            for lang_code, data in translations.items():
                if data.get("text"):
                    preview = data["text"][:200]
                    print(f"\n   [{data['language']}] {preview}")

    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
