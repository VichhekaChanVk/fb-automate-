"""
Bulk video downloader + editor.

Two modes:
  1) download : paste a profile/channel/playlist link, download every video,
                then speed up + flip each one.
  2) edit     : speed up + flip videos already on your PC (files or folders).

Edited files are saved as "<name>.edited.mp4" and the original is deleted
after a successful edit (use --keep-original to keep it).

Downloads work on any site yt-dlp supports (YouTube, TikTok, Instagram,
X/Twitter, Facebook, Vimeo, Twitch, Reddit, Dailymotion, ... 1800+ sites).
Editing runs on an NVIDIA GPU (NVENC) when available, else CPU.

Setup:
    pip install -U yt-dlp          (only needed for download mode)
    Install ffmpeg and put it on PATH.

Usage:
    python bulk_download.py                                   # menu: pick mode 1 or 2
    python bulk_download.py download URL [URL ...]            # download + 1.5x + flip
    python bulk_download.py download URL --no-edit            # plain download
    python bulk_download.py download URL -o D:\\Videos --cookies-from-browser chrome
    python bulk_download.py edit D:\\Videos                   # edit every video in folder
    python bulk_download.py edit a.mp4 b.mkv --speed 1.25 --no-flip

Only download content you have the right to download. Respect each
platform's terms of service and creators' copyright.
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

VIDEO_EXTS = {".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v", ".flv", ".ts", ".wmv"}


# ---------------------------------------------------------------- editing

def has_ffmpeg():
    return shutil.which("ffmpeg") is not None


def has_nvenc():
    try:
        out = subprocess.run(["ffmpeg", "-hide_banner", "-encoders"],
                             capture_output=True, text=True).stdout
    except OSError:
        return False
    return "h264_nvenc" in out


def atempo_chain(speed):
    # atempo accepts 0.5-2.0 per filter on older ffmpeg; chain for other values.
    parts = []
    while speed > 2.0:
        parts.append("atempo=2.0")
        speed /= 2.0
    while speed < 0.5:
        parts.append("atempo=0.5")
        speed /= 0.5
    parts.append(f"atempo={speed:.6g}")
    return ",".join(parts)


def edit_video(path, speed, flip, use_gpu, keep_original, audio_only=False):
    """Speed up and/or mirror one file. Decode + encode on NVIDIA GPU when available."""
    src = Path(path)
    if not src.exists():
        return
    ext = src.suffix if audio_only else ".mp4"
    dst = src.with_name(src.stem + ".edited" + ext)

    vf = []
    if flip and not audio_only:
        vf.append("hflip")
    if speed != 1.0 and not audio_only:
        vf.append(f"setpts=PTS/{speed}")
    af = atempo_chain(speed) if speed != 1.0 else None

    def command(gpu):
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-stats", "-y"]
        if gpu:
            cmd += ["-hwaccel", "cuda"]
        cmd += ["-i", str(src), "-map", "0:v?", "-map", "0:a?"]
        if vf:
            cmd += ["-filter:v", ",".join(vf)]
        if af:
            cmd += ["-filter:a", af]
        if audio_only:
            cmd += ["-vn"]
        elif gpu:
            cmd += ["-c:v", "h264_nvenc", "-preset", "p5", "-rc", "vbr", "-cq", "23", "-b:v", "0"]
        else:
            cmd += ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23"]
        if not audio_only:
            cmd += ["-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart"]
        return cmd + [str(dst)]

    print(f"[edit] {src.name}  speed={speed}x flip={flip} encoder={'NVENC' if use_gpu else 'CPU'}")
    result = subprocess.run(command(use_gpu))
    if result.returncode != 0 and use_gpu:
        print("[edit] GPU encode failed, retrying on CPU...")
        result = subprocess.run(command(False))
    if result.returncode != 0 or not dst.exists() or dst.stat().st_size == 0:
        print(f"[edit] failed, original kept: {src.name}")
        dst.unlink(missing_ok=True)
        return

    # Output keeps the ".edited" tag so a re-run never speeds it up twice.
    if keep_original:
        print(f"[edit] saved {dst.name}")
    else:
        src.unlink()
        print(f"[edit] saved {dst.name}, deleted original")


def make_editor(args, audio_only=False):
    """Return a function(path) that edits one file, or None if no edit requested."""
    if args.speed == 1.0 and not args.flip:
        return None
    if not has_ffmpeg():
        sys.exit("Editing needs ffmpeg on PATH.")
    use_gpu = not args.cpu and has_nvenc()
    if not args.cpu and not use_gpu:
        print("[warn] NVENC not available in ffmpeg: editing on CPU.")
    return lambda path: edit_video(path, args.speed, args.flip, use_gpu,
                                   args.keep_original, audio_only)


def find_videos(inputs):
    files = []
    for item in inputs:
        p = Path(item.strip().strip('"').strip("'")).expanduser()
        if p.is_dir():
            files += sorted(f for f in p.rglob("*") if f.suffix.lower() in VIDEO_EXTS)
        elif p.is_file():
            files.append(p)
        else:
            print(f"[skip] not found: {p}")
    # Never re-edit our own output, and skip videos already edited on a previous run.
    files = [f for f in files if ".edited" not in f.stem]
    done = [f for f in files if f.with_name(f.stem + ".edited.mp4").exists()]
    if done:
        print(f"[skip] {len(done)} already edited")
    return [f for f in files if f not in done]


def run_edit(args):
    files = find_videos(args.paths)
    if not files:
        sys.exit("No video files found.")
    editor = make_editor(args)
    if editor is None:
        sys.exit("Nothing to do: speed is 1.0 and flip is off.")
    print(f"{len(files)} video(s) to edit.")
    for i, f in enumerate(files, 1):
        print(f"\n--- {i}/{len(files)} ---")
        editor(f)
    print("\nDone.")


# ---------------------------------------------------------------- download

def build_options(args, yt_dlp):
    out_dir = Path(args.output).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    ffmpeg = has_ffmpeg()
    if not ffmpeg:
        print("[warn] ffmpeg not found: falling back to single-file formats (may be lower quality).")

    if args.audio_only:
        fmt = "bestaudio/best"
    elif ffmpeg:
        fmt = "bestvideo*+bestaudio/best"
    else:
        fmt = "best"

    opts = {
        "format": fmt,
        # One folder per uploader/channel, file named by date + title + id.
        "outtmpl": str(out_dir / "%(extractor_key)s" / "%(uploader,channel,uploader_id|unknown)s"
                       / "%(upload_date|nodate)s - %(title).150B [%(id)s].%(ext)s"),
        # Remembers finished videos, so re-running only fetches new uploads.
        "download_archive": str(out_dir / "downloaded.txt"),
        "ignoreerrors": True,          # skip private/removed videos, keep going
        "retries": 10,
        "fragment_retries": 10,
        "concurrent_fragment_downloads": 4,
        "windowsfilenames": True,
        "writethumbnail": args.thumbnails,
        "writeinfojson": args.info_json,
        "noplaylist": False,
        "postprocessors": [],
    }

    if ffmpeg and not args.audio_only:
        opts["merge_output_format"] = "mp4"
    if args.audio_only and ffmpeg:
        opts["postprocessors"].append(
            {"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}
        )
    if args.limit:
        opts["playlistend"] = args.limit
    if args.cookies:
        opts["cookiefile"] = args.cookies
    if args.cookies_from_browser:
        opts["cookiesfrombrowser"] = (args.cookies_from_browser,)
    if args.sleep:
        opts["sleep_interval"] = args.sleep
        opts["max_sleep_interval"] = args.sleep * 2

    editor = make_editor(args, audio_only=args.audio_only)
    if editor:
        # post_hooks run once per video with the final file path, after merge/convert.
        opts["post_hooks"] = [editor]

    return opts


def normalize(url):
    url = url.strip().strip('"').strip("'")
    if url and not url.startswith(("http://", "https://")):
        url = "https://" + url
    # YouTube profile link "youtube.com/@name" only lists the home tab; point at all videos.
    if "youtube.com/@" in url and url.rstrip("/").count("/") == 3:
        url = url.rstrip("/") + "/videos"
    return url


def run_download(args):
    try:
        import yt_dlp
    except ImportError:
        sys.exit("yt-dlp not installed. Run: pip install -U yt-dlp")

    urls = [normalize(u) for u in args.urls if u.strip()]
    if not urls:
        sys.exit("No links given.")

    with yt_dlp.YoutubeDL(build_options(args, yt_dlp)) as ydl:
        for url in urls:
            print(f"\n=== {url} ===")
            try:
                ydl.download([url])
            except yt_dlp.utils.DownloadError as e:
                print(f"[error] {url}: {e}")
    print(f"\nDone. Files in: {Path(args.output).resolve()}")


# ---------------------------------------------------------------- CLI

def add_edit_flags(p):
    p.add_argument("--speed", type=float, default=1.5, help="Playback speed (default 1.5)")
    p.add_argument("--no-flip", dest="flip", action="store_false", help="Do not mirror video")
    p.add_argument("--cpu", action="store_true", help="Edit on CPU instead of NVIDIA GPU")
    p.add_argument("--keep-original", action="store_true",
                   help="Keep original after edit (default: delete it to save space)")
    p.set_defaults(flip=True)


def build_parser():
    p = argparse.ArgumentParser(description="Bulk-download and/or speed up + flip videos.")
    sub = p.add_subparsers(dest="mode")

    d = sub.add_parser("download", help="Download from links, then speed up + flip")
    d.add_argument("urls", nargs="+", help="Profile, channel or playlist URLs")
    d.add_argument("-o", "--output", default="downloads", help="Output folder (default: ./downloads)")
    d.add_argument("--limit", type=int, default=0, help="Max videos per link (0 = all)")
    d.add_argument("--audio-only", action="store_true", help="Download audio only (mp3)")
    d.add_argument("--cookies", help="Path to cookies.txt (for private/login-only content)")
    d.add_argument("--cookies-from-browser", help="Read cookies from browser: chrome, firefox, edge, brave...")
    d.add_argument("--sleep", type=int, default=0, help="Seconds to wait between videos (avoid rate limits)")
    d.add_argument("--thumbnails", action="store_true", help="Also save thumbnails")
    d.add_argument("--info-json", action="store_true", help="Also save metadata .info.json")
    d.add_argument("--no-edit", action="store_true", help="Download only, no speed/flip")
    add_edit_flags(d)

    e = sub.add_parser("edit", help="Speed up + flip videos already on disk")
    e.add_argument("paths", nargs="+", help="Video files and/or folders (searched recursively)")
    add_edit_flags(e)
    return p


def ask_lines(prompt):
    print(prompt)
    lines = []
    while True:
        line = input("> ").strip()
        if not line:
            return lines
        lines.append(line)


def menu(parser):
    print("1) Download from link + speed up + flip")
    print("2) Speed up + flip videos on my PC")
    choice = input("Choose 1 or 2: ").strip()
    if choice == "1":
        mode, items = "download", ask_lines("Paste profile links (one per line). Empty line to start:")
    elif choice == "2":
        mode, items = "edit", ask_lines("Paste video files or folders (one per line). Empty line to start:")
    else:
        sys.exit("Invalid choice.")
    if not items:
        sys.exit("Nothing given.")
    return parser.parse_args([mode, *items])


def main():
    parser = build_parser()
    try:
        args = parser.parse_args() if len(sys.argv) > 1 else menu(parser)
    except (EOFError, KeyboardInterrupt):
        sys.exit("\nCancelled.")
    if args.mode is None:
        parser.print_help()
        return
    if args.speed <= 0:
        parser.error("--speed must be > 0")
    if getattr(args, "no_edit", False):
        args.speed, args.flip = 1.0, False

    if args.mode == "download":
        run_download(args)
    else:
        run_edit(args)


if __name__ == "__main__":
    main()
