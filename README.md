# fb-automate

[![CI](https://github.com/VichhekaChanVk/fb-automate-/actions/workflows/ci.yml/badge.svg)](https://github.com/VichhekaChanVk/fb-automate-/actions/workflows/ci.yml)

Local dashboard that queues and auto-publishes posts to the Facebook Pages you manage, through Meta's official Graph API.

## Features

- Import every Page you admin from one User Access Token
- Bulk-schedule a folder of videos/photos with a fixed gap between posts
- Captions from a default text, a CSV (`filename,caption`), or a `.txt` file next to each video
- Single posts: video, photo, text, or link
- Calendar dashboard: week view with video thumbnails, color per Page, stat cards (today's progress, next post, queued, published, followers, engagement), and a detail panel to preview, edit, post now, retry, or delete
- Queue tools: re-space the whole queue to a new start time and gap, or delete all queued posts
- Live queue list (updates every 3 s) with retry and delete
- Duplicate protection: the same video (matched by file content, not name) is never posted twice to the same Page
- Survives restarts: the queue is saved in SQLite, and missed posts resume from now with the same gaps

## Setup

```bash
pip install -r fb_scheduler/requirements.txt
python fb_scheduler/app.py
```

Open http://127.0.0.1:5000. Set `PORT` to use another port.

### Getting a token

1. On [developers.facebook.com](https://developers.facebook.com), create an app and add the **Manage everything on your Page** use case.
2. In that use case, add `pages_manage_posts`, `pages_read_engagement`, and `pages_show_list` (plus `business_management` if your Pages are in a Business portfolio).
3. In **Graph API Explorer**, select your app and **User Token**, add those permissions, click **Generate Access Token**, and tick your Pages.
4. Extend the token in the **Access Token Debugger** ("Extend Access Token"), then paste it into **Import my Pages**. Page tokens made from an extended user token do not expire.

## Video downloader + editor (`bulk_download.py`)

Command-line tool with two modes:

- `download`: download every video from a profile/channel/playlist link (any site [yt-dlp](https://github.com/yt-dlp/yt-dlp) supports), then optionally speed up and mirror each one.
- `edit`: speed up and/or mirror videos already on disk (files or folders, searched recursively).

Editing uses ffmpeg with NVIDIA NVENC when available and falls back to CPU. Edited files are saved as `<name>.edited.mp4`, and the original is deleted after a successful edit unless you pass `--keep-original`.

```bash
pip install -U yt-dlp      # needs ffmpeg on PATH
python bulk_download.py                                  # menu
python bulk_download.py download URL --speed 1.5         # download + edit
python bulk_download.py download URL --no-edit           # plain download
python bulk_download.py edit D:\Videos --no-flip         # edit local files
```

Run `python bulk_download.py download -h` or `edit -h` for every option.

## Development

```bash
pip install -r requirements-dev.txt
ruff check .
pytest
```

- **CI** (`.github/workflows/ci.yml`): every push and pull request runs lint and tests on Ubuntu and Windows (Python 3.11 and 3.12), plus a gitleaks secret scan that fails if a token is committed.
- **Release** (`.github/workflows/release.yml`): push a tag like `v1.0.0` to re-run the checks, build `fb-automate-v1.0.0.zip`, and publish it as a GitHub Release.

```bash
git tag v1.0.0
git push origin v1.0.0
```

## Notes

- The app must stay running (PC on) for scheduled posts to go out.
- Tokens are stored in `fb_scheduler/posts.db`. It is git-ignored: keep it private.
- Only post content you own or have rights to. Facebook removes and demonetizes reuploaded content.
