"""
Facebook Page auto-poster: local dashboard that queues and publishes posts to
the Facebook Pages you manage, through Meta's official Graph API.

Setup:
    pip install flask requests
    python app.py
    open http://127.0.0.1:5000

Getting tokens (one time):
    1. developers.facebook.com -> My Apps -> Create App (type: Business).
    2. Tools -> Graph API Explorer -> pick your app -> "Get User Access Token"
       with permissions: pages_show_list, pages_read_engagement,
       pages_manage_posts.
    3. Paste that user token into "Import my Pages". Every Page you admin is
       added with its own Page token.
    For long-lived tokens (about 60 days, Page tokens then never expire),
    exchange the user token first in the Access Token Debugger -> "Extend".

The PC must stay on with this app running for scheduled posts to go out.
Tokens are stored in posts.db next to this file: keep it private.
"""

import csv
import hashlib
import os
import shutil
import sqlite3
import subprocess
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from flask import (Flask, abort, flash, jsonify, redirect, render_template_string, request,
                   send_file, url_for)

GRAPH_VERSION = "v23.0"
GRAPH = f"https://graph.facebook.com/{GRAPH_VERSION}"
GRAPH_VIDEO = f"https://graph-video.facebook.com/{GRAPH_VERSION}"
DB_PATH = Path(__file__).with_name("posts.db")
VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".avi", ".mkv", ".webm"}
PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
WORKER_TICK = 15  # seconds between queue checks

app = Flask(__name__)
app.secret_key = "local-only"


# ---------------------------------------------------------------- database

def db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS pages (
                id TEXT PRIMARY KEY, name TEXT, token TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                page_id TEXT NOT NULL,
                kind TEXT NOT NULL,            -- video | photo | text
                path TEXT, caption TEXT, link TEXT,
                run_at REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',  -- queued|posting|done|failed
                result TEXT,
                posted_at REAL);
        """)
        # A post left "posting" means the app stopped mid-upload. It may or may
        # not be live on Facebook, so flag it instead of silently re-posting.
        c.execute("UPDATE posts SET status='failed', result='Interrupted: check the Page, then Retry if missing' "
                  "WHERE status='posting'")
        # App was off past some post times: push each Page's queue forward so it resumes
        # from now with the same gaps, instead of firing all missed posts at once.
        now = time.time()
        for r in c.execute("SELECT page_id, MIN(run_at) AS first FROM posts WHERE status='queued' "
                           "GROUP BY page_id HAVING first < ?", (now - 60,)).fetchall():
            c.execute("UPDATE posts SET run_at = run_at + ? WHERE page_id=? AND status='queued'",
                      (now - r["first"], r["page_id"]))
            print(f"[startup] Page {r['page_id']}: missed posts moved forward {(now - r['first']) / 3600:.1f} h")
        # Older databases: add the fingerprint column and fill it for files still on disk.
        cols = {r["name"] for r in c.execute("PRAGMA table_info(posts)")}
        if "file_hash" not in cols:
            c.execute("ALTER TABLE posts ADD COLUMN file_hash TEXT")
        for row in c.execute("SELECT id, path FROM posts WHERE path IS NOT NULL AND file_hash IS NULL").fetchall():
            if Path(row["path"]).is_file():
                c.execute("UPDATE posts SET file_hash=? WHERE id=?", (fingerprint(row["path"]), row["id"]))


# ---------------------------------------------------------------- duplicates

def fingerprint(path, chunk=4 * 1024 * 1024):
    """Content id of a file: size + SHA-256 of its first and last 4 MB.
    Same video renamed or copied elsewhere gives the same id; fast on big files."""
    p = Path(path)
    size = p.stat().st_size
    h = hashlib.sha256(str(size).encode())
    with open(p, "rb") as f:
        h.update(f.read(chunk))
        if size > chunk:
            f.seek(max(chunk, size - chunk))
            h.update(f.read(chunk))
    return h.hexdigest()


def used_fingerprints(c, page_id):
    """Fingerprints already posted, posting, or waiting in the queue for this Page."""
    rows = c.execute("SELECT file_hash FROM posts WHERE page_id=? AND file_hash IS NOT NULL "
                     "AND status IN ('queued','posting','done')", (page_id,))
    return {r["file_hash"] for r in rows}


# ---------------------------------------------------------------- Graph API

def graph_error(resp):
    try:
        err = resp.json().get("error", {})
        return f"{err.get('message', resp.text)} (code {err.get('code')})"
    except ValueError:
        return f"HTTP {resp.status_code}: {resp.text[:300]}"


NEEDED_PERMISSIONS = ["pages_show_list", "pages_read_engagement", "pages_manage_posts"]


def graph_get_all(path, params, token):
    url = f"{GRAPH}/{path}"
    params = {**params, "limit": 100, "access_token": token}
    items = []
    while url:
        r = requests.get(url, params=params, timeout=30)
        if not r.ok:
            raise RuntimeError(graph_error(r))
        data = r.json()
        items += data.get("data", [])
        url, params = data.get("paging", {}).get("next"), None
    return items


def import_pages(user_token):
    fields = {"fields": "id,name,access_token"}
    pages = {p["id"]: p for p in graph_get_all("me/accounts", fields, user_token)}
    # Pages owned by a Business portfolio may only show up here (needs business_management).
    try:
        for biz in graph_get_all("me/businesses", {"fields": "id"}, user_token):
            for edge in ("owned_pages", "client_pages"):
                for p in graph_get_all(f"{biz['id']}/{edge}", fields, user_token):
                    if p.get("access_token"):
                        pages.setdefault(p["id"], p)
    except RuntimeError:
        pass  # no business_management permission or no business: fine
    return list(pages.values())


def explain_no_pages(user_token):
    """Why a valid token sees zero Pages, in plain words."""
    try:
        perms = graph_get_all("me/permissions", {}, user_token)
    except RuntimeError as e:
        return f"Token check failed: {e}"
    granted = {p["permission"] for p in perms if p.get("status") == "granted"}
    missing = [p for p in NEEDED_PERMISSIONS if p not in granted]
    if missing:
        return ("Token is missing permissions: " + ", ".join(missing)
                + ". In Graph API Explorer add them, click Generate Access Token, and tick your Pages.")
    return ("Token works but sees no Pages. Either you did not tick your Pages in the Facebook login "
            "pop-up (generate the token again and select them), or you post from a profile in "
            "Professional mode, which is not a Page. The API cannot post to profiles. "
            "If your Pages are in a Business portfolio, also add the business_management permission.")


def publish(post, token):
    """Publish one post. Returns the Facebook id, raises RuntimeError on failure."""
    kind, caption = post["kind"], post["caption"] or ""
    page = post["page_id"]
    if kind == "video":
        with open(post["path"], "rb") as f:
            r = requests.post(f"{GRAPH_VIDEO}/{page}/videos",
                              data={"description": caption, "access_token": token},
                              files={"source": f}, timeout=3600)
    elif kind == "photo":
        with open(post["path"], "rb") as f:
            r = requests.post(f"{GRAPH}/{page}/photos",
                              data={"caption": caption, "access_token": token},
                              files={"source": f}, timeout=600)
    else:
        data = {"message": caption, "access_token": token}
        if post["link"]:
            data["link"] = post["link"]
        r = requests.post(f"{GRAPH}/{page}/feed", data=data, timeout=60)
    if not r.ok:
        raise RuntimeError(graph_error(r))
    body = r.json()
    return body.get("post_id") or body.get("id") or str(body)


# ---------------------------------------------------------------- worker

def worker():
    while True:
        try:
            with db() as c:
                row = c.execute(
                    "SELECT p.*, pg.token FROM posts p JOIN pages pg ON pg.id = p.page_id "
                    "WHERE p.status='queued' AND p.run_at <= ? ORDER BY p.run_at LIMIT 1",
                    (time.time(),)).fetchone()
                if row:
                    # Last check before upload: same file already live on this Page?
                    dup = row["file_hash"] and c.execute(
                        "SELECT id FROM posts WHERE page_id=? AND file_hash=? AND status IN ('done','posting') "
                        "AND id != ? LIMIT 1", (row["page_id"], row["file_hash"], row["id"])).fetchone()
                    if dup:
                        c.execute("UPDATE posts SET status='skipped', result=?, posted_at=? WHERE id=?",
                                  (f"Same video already posted (#{dup['id']})", time.time(), row["id"]))
                        continue
                    c.execute("UPDATE posts SET status='posting' WHERE id=?", (row["id"],))
            if not row:
                time.sleep(WORKER_TICK)
                continue
            try:
                fb_id = publish(row, row["token"])
                status, result = "done", fb_id
            except Exception as e:  # network, file, or API error
                status, result = "failed", str(e)
            with db() as c:
                c.execute("UPDATE posts SET status=?, result=?, posted_at=? WHERE id=?",
                          (status, result, time.time(), row["id"]))
            print(f"[worker] post {row['id']} -> {status}: {result}")
        except Exception as e:
            print(f"[worker] error: {e}")
            time.sleep(WORKER_TICK)


# ---------------------------------------------------------------- helpers

def kind_for(path):
    ext = Path(path).suffix.lower()
    if ext in VIDEO_EXTS:
        return "video"
    if ext in PHOTO_EXTS:
        return "photo"
    return None


def load_captions(csv_path):
    """CSV with columns: filename,caption"""
    caps = {}
    if csv_path:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            for row in csv.reader(f):
                if len(row) >= 2 and row[0].strip().lower() != "filename":
                    caps[row[0].strip()] = row[1]
    return caps


def caption_for(file, caps, default):
    sidecar = file.with_suffix(".txt")
    if sidecar.exists():
        return sidecar.read_text(encoding="utf-8").strip()
    return caps.get(file.name, caps.get(file.stem, default))


def parse_local(dt_str):
    return datetime.fromisoformat(dt_str).timestamp() if dt_str else time.time()


@app.template_filter("ts")
def fmt_ts(ts):
    """12-hour time, e.g. 'Oct 18, 2026 3:52 PM'."""
    if not ts:
        return ""
    d = datetime.fromtimestamp(ts)
    hour = d.hour % 12 or 12
    return f"{d:%b} {d.day}, {d.year} {hour}:{d:%M} {d:%p}"


# ---------------------------------------------------------------- routes

def render_queue(status):
    with db() as c:
        q = ("SELECT p.*, pg.name AS page_name FROM posts p LEFT JOIN pages pg ON pg.id=p.page_id "
             + ("WHERE p.status=? " if status else "") + "ORDER BY p.run_at DESC LIMIT 300")
        posts = c.execute(q, (status,) if status else ()).fetchall()
        counts = dict(c.execute("SELECT status, COUNT(*) FROM posts GROUP BY status").fetchall())
    return render_template_string(QUEUE_TEMPLATE, posts=posts, counts=counts, status=status)


@app.get("/")
def index():
    status = request.args.get("status", "")
    with db() as c:
        pages = c.execute("SELECT id, name FROM pages ORDER BY name").fetchall()
    now = datetime.now().strftime("%Y-%m-%dT%H:%M")
    return render_template_string(TEMPLATE, pages=pages, now=now,
                                  queue_html=render_queue(status))


@app.get("/queue")
def queue_fragment():
    """Queue table only; the dashboard polls this to update without reloading."""
    return render_queue(request.args.get("status", ""))


@app.post("/pages/import")
def pages_import():
    token = request.form["user_token"].strip()
    try:
        found = import_pages(token)
    except Exception as e:
        flash(f"Import failed: {e}", "err")
        return redirect(url_for("index"))
    if not found:
        flash(explain_no_pages(token), "err")
        return redirect(url_for("index"))
    with db() as c:
        for p in found:
            c.execute("INSERT OR REPLACE INTO pages(id, name, token) VALUES (?,?,?)",
                      (p["id"], p["name"], p["access_token"]))
    flash(f"Imported {len(found)} Page(s).", "ok")
    return redirect(url_for("index"))


@app.post("/pages/add")
def pages_add():
    f = request.form
    with db() as c:
        c.execute("INSERT OR REPLACE INTO pages(id, name, token) VALUES (?,?,?)",
                  (f["page_id"].strip(), f["name"].strip() or f["page_id"].strip(), f["token"].strip()))
    flash("Page saved.", "ok")
    return redirect(url_for("index"))


@app.post("/pages/<page_id>/delete")
def pages_delete(page_id):
    with db() as c:
        c.execute("DELETE FROM posts WHERE page_id=? AND status='queued'", (page_id,))
        c.execute("DELETE FROM pages WHERE id=?", (page_id,))
    flash("Page removed (its queued posts too).", "ok")
    return redirect(url_for("index"))


@app.post("/bulk")
def bulk():
    f = request.form
    page_ids = f.getlist("page_ids")
    folder = Path(f["folder"].strip().strip('"'))
    if not page_ids:
        flash("Pick at least one Page.", "err")
        return redirect(url_for("index"))
    if not folder.is_dir():
        flash(f"Folder not found: {folder}", "err")
        return redirect(url_for("index"))
    files = sorted(p for p in folder.iterdir() if p.is_file() and kind_for(p))
    if not files:
        flash("No videos or photos in that folder.", "err")
        return redirect(url_for("index"))
    try:
        caps = load_captions(f.get("captions_csv", "").strip().strip('"'))
    except OSError as e:
        flash(f"Captions CSV error: {e}", "err")
        return redirect(url_for("index"))

    start = parse_local(f.get("start"))
    gap = max(1, int(f.get("interval") or 60)) * 60
    default = f.get("caption", "")

    fps = {file: fingerprint(file) for file in files}
    rows, skipped = [], []
    with db() as c:
        used = {pid: used_fingerprints(c, pid) for pid in page_ids}
        slot = {pid: 0 for pid in page_ids}  # next free time slot per Page, so skips leave no gaps

        def add(pid, file):
            used[pid].add(fps[file])
            rows.append((pid, kind_for(file), str(file), caption_for(file, caps, default),
                         start + slot[pid] * gap, fps[file]))
            slot[pid] += 1

        if f.get("mode") == "all":
            # Every Page gets every file it has not had yet, each Page on its own timeline.
            for pid in page_ids:
                for file in files:
                    if fps[file] in used[pid]:
                        skipped.append(file.name)
                    else:
                        add(pid, file)
        else:
            # Split round-robin; a video already on any picked Page is skipped entirely.
            i = 0
            for file in files:
                if any(fps[file] in used[pid] for pid in page_ids):
                    skipped.append(file.name)
                    continue
                add(page_ids[i % len(page_ids)], file)
                i += 1

        c.executemany(
            "INSERT INTO posts(page_id, kind, path, caption, run_at, file_hash) VALUES (?,?,?,?,?,?)", rows)
    msg = f"Queued {len(rows)} post(s) from {len(files)} file(s)."
    if skipped:
        msg += f" Skipped {len(skipped)} already uploaded or queued: " + ", ".join(skipped[:10])
        msg += " ..." if len(skipped) > 10 else ""
    flash(msg, "ok")
    return redirect(url_for("index"))


@app.post("/post")
def single_post():
    f = request.form
    path = f.get("path", "").strip().strip('"')
    kind = kind_for(path) if path else "text"
    if path and (not kind or not Path(path).is_file()):
        flash(f"File not found or unsupported: {path}", "err")
        return redirect(url_for("index"))
    page_ids = f.getlist("page_ids")
    if not page_ids:
        flash("Pick at least one Page.", "err")
        return redirect(url_for("index"))
    link = f.get("link", "").strip()
    if link and not link.startswith(("http://", "https://")):
        flash(f"Link must be a web address starting with http:// or https:// (got: {link[:40]}). "
              "Put text in Caption / message.", "err")
        return redirect(url_for("index"))
    if not path and not f.get("caption", "").strip() and not link:
        flash("Text post needs a caption or a link.", "err")
        return redirect(url_for("index"))
    run_at = parse_local(f.get("run_at"))
    fp = fingerprint(path) if path else None
    with db() as c:
        dup_pages = [pid for pid in page_ids if fp and fp in used_fingerprints(c, pid)]
        targets = [pid for pid in page_ids if pid not in dup_pages]
        c.executemany(
            "INSERT INTO posts(page_id, kind, path, caption, link, run_at, file_hash) VALUES (?,?,?,?,?,?,?)",
            [(pid, kind, path or None, f.get("caption", ""), f.get("link", "").strip() or None, run_at, fp)
             for pid in targets])
    msg = f"Queued {len(targets)} post(s)."
    if dup_pages:
        msg += f" Skipped {len(dup_pages)} Page(s): this file is already uploaded or queued there."
    flash(msg, "ok" if targets else "err")
    return redirect(url_for("index"))


@app.post("/posts/<int:post_id>/retry")
def retry(post_id):
    # Only failed posts can be retried: done ones stay done so a video never goes up twice.
    with db() as c:
        c.execute("UPDATE posts SET status='queued', result=NULL, run_at=? WHERE id=? AND status='failed'",
                  (time.time(), post_id))
    return redirect(request.referrer or url_for("index"))


@app.post("/posts/<int:post_id>/delete")
def delete_post(post_id):
    with db() as c:
        c.execute("DELETE FROM posts WHERE id=? AND status!='posting'", (post_id,))
    return redirect(request.referrer or url_for("index"))


@app.post("/posts/clear")
def clear():
    which = request.form.get("which", "done")
    with db() as c:
        c.execute("DELETE FROM posts WHERE status=?", (which,))
    flash(f"Cleared {which} posts.", "ok")
    return redirect(url_for("index"))


# ---------------------------------------------------------------- dashboard API

THUMB_DIR = Path(__file__).with_name("thumbs")
PAGE_COLORS = ["#4f7cff", "#ff7a45", "#22b07d", "#b45cff", "#e0a800", "#ff5c8a", "#16a2b8"]
INSIGHTS_TTL = 600  # seconds between Graph API insight refreshes
_insights_cache = {"at": 0.0, "data": None}
_insights_lock = threading.Lock()


def post_dict(row):
    d = dict(row)
    d.pop("token", None)
    d.pop("file_hash", None)
    d["when"] = fmt_ts(d["run_at"])
    d["posted"] = fmt_ts(d.get("posted_at"))
    d["file"] = Path(d["path"]).name if d.get("path") else ""
    d["fb_url"] = f"https://www.facebook.com/{d['result']}" if d["status"] == "done" and d.get("result") else ""
    return d


def get_post(c, post_id):
    return c.execute("SELECT p.*, pg.name AS page_name FROM posts p LEFT JOIN pages pg ON pg.id=p.page_id "
                     "WHERE p.id=?", (post_id,)).fetchone()


@app.get("/api/state")
def api_state():
    """Everything the calendar view needs for one date range, in one call."""
    start = float(request.args.get("start") or time.time())
    days = max(1, min(int(request.args.get("days") or 7), 42))
    end = start + days * 86400
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    day0, day1 = today.timestamp(), (today + timedelta(days=1)).timestamp()
    with db() as c:
        pages = [dict(r) for r in c.execute("SELECT id, name FROM pages ORDER BY name")]
        posts = [post_dict(r) for r in c.execute(
            "SELECT p.*, pg.name AS page_name FROM posts p LEFT JOIN pages pg ON pg.id=p.page_id "
            "WHERE p.run_at >= ? AND p.run_at < ? ORDER BY p.run_at, p.id", (start, end))]
        counts = dict(c.execute("SELECT status, COUNT(*) FROM posts GROUP BY status").fetchall())
        nxt = c.execute("SELECT p.*, pg.name AS page_name FROM posts p LEFT JOIN pages pg ON pg.id=p.page_id "
                        "WHERE p.status='queued' ORDER BY p.run_at LIMIT 1").fetchone()
        posted_today = c.execute("SELECT COUNT(*) FROM posts WHERE status='done' AND posted_at >= ?",
                                 (day0,)).fetchone()[0]
        left_today = c.execute("SELECT COUNT(*) FROM posts WHERE status IN ('queued','posting') AND run_at < ?",
                               (day1,)).fetchone()[0]
        per_page = dict(c.execute("SELECT page_id, COUNT(*) FROM posts WHERE status='queued' GROUP BY page_id"))
        last = c.execute("SELECT MAX(run_at) FROM posts WHERE status='queued'").fetchone()[0]
    for i, p in enumerate(pages):
        p["color"] = PAGE_COLORS[i % len(PAGE_COLORS)]
        p["queued"] = per_page.get(p["id"], 0)
    return jsonify(now=time.time(), pages=pages, posts=posts, counts=counts,
                   posted_today=posted_today, left_today=left_today,
                   next=post_dict(nxt) if nxt else None, queue_ends=fmt_ts(last))


@app.get("/api/posts/<int:post_id>")
def api_post(post_id):
    with db() as c:
        row = get_post(c, post_id)
    if not row:
        abort(404)
    return jsonify(post_dict(row))


@app.post("/api/posts/<int:post_id>")
def api_post_edit(post_id):
    """Edit caption and/or time of a post that has not gone out yet."""
    body = request.get_json(silent=True) or {}
    with db() as c:
        row = get_post(c, post_id)
        if not row:
            abort(404)
        if row["status"] not in ("queued", "failed"):
            return jsonify(error="Only queued or failed posts can be edited."), 400
        caption = body.get("caption", row["caption"])
        run_at = parse_local(body["run_at"]) if body.get("run_at") else row["run_at"]
        c.execute("UPDATE posts SET caption=?, run_at=? WHERE id=?", (caption, run_at, post_id))
        return jsonify(post_dict(get_post(c, post_id)))


@app.post("/api/posts/<int:post_id>/now")
def api_post_now(post_id):
    """Move a queued (or failed) post to the front so the worker sends it within one tick."""
    with db() as c:
        n = c.execute("UPDATE posts SET status='queued', result=NULL, run_at=? "
                      "WHERE id=? AND status IN ('queued','failed')", (time.time(), post_id)).rowcount
    return jsonify(ok=bool(n)), (200 if n else 400)


@app.post("/api/posts/<int:post_id>/delete")
def api_post_delete(post_id):
    with db() as c:
        n = c.execute("DELETE FROM posts WHERE id=? AND status!='posting'", (post_id,)).rowcount
    return jsonify(ok=bool(n))


@app.post("/api/queue/reschedule")
def api_reschedule():
    """Re-space every queued post: start time + fixed gap, per Page, keeping the current order."""
    body = request.get_json(silent=True) or {}
    try:
        start = parse_local(body.get("start"))
        gap = max(1, int(body.get("gap") or 240)) * 60
    except (TypeError, ValueError):
        return jsonify(error="Bad start time or gap."), 400
    page_id = body.get("page_id") or None
    with db() as c:
        rows = c.execute("SELECT id, page_id FROM posts WHERE status='queued' "
                         + ("AND page_id=? " if page_id else "") + "ORDER BY run_at, id",
                         (page_id,) if page_id else ()).fetchall()
        slot = {}
        for r in rows:
            n = slot.get(r["page_id"], 0)
            c.execute("UPDATE posts SET run_at=? WHERE id=?", (start + n * gap, r["id"]))
            slot[r["page_id"]] = n + 1
    return jsonify(ok=True, moved=len(rows))


@app.post("/api/queue/clear")
def api_queue_clear():
    """Delete every queued post (optionally for one Page). Done posts are kept."""
    page_id = (request.get_json(silent=True) or {}).get("page_id")
    with db() as c:
        n = c.execute("DELETE FROM posts WHERE status='queued'" + (" AND page_id=?" if page_id else ""),
                      (page_id,) if page_id else ()).rowcount
    return jsonify(ok=True, deleted=n)


def make_thumb(src, dst):
    """First frame (video) or a resized copy (photo) as a small JPEG, via ffmpeg."""
    if not shutil.which("ffmpeg"):
        return False
    dst.parent.mkdir(exist_ok=True)
    tmp = dst.with_suffix(".tmp.jpg")
    seek = ["-ss", "1"] if kind_for(src) == "video" else []
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", *seek, "-i", str(src),
           "-frames:v", "1", "-vf", "scale=360:-2", str(tmp)]
    try:
        ok = subprocess.run(cmd, timeout=30).returncode == 0 and tmp.exists()
        if not ok and seek:  # clip shorter than 1 s: take the very first frame
            ok = subprocess.run([x for x in cmd if x not in seek], timeout=30).returncode == 0 and tmp.exists()
    except (OSError, subprocess.TimeoutExpired):
        ok = False
    if ok:
        tmp.replace(dst)
    else:
        tmp.unlink(missing_ok=True)
    return ok


@app.get("/thumb/<int:post_id>")
def thumb(post_id):
    with db() as c:
        row = c.execute("SELECT path, file_hash FROM posts WHERE id=?", (post_id,)).fetchone()
    if not row or not row["path"] or not Path(row["path"]).is_file():
        abort(404)
    dst = THUMB_DIR / f"{row['file_hash'] or post_id}.jpg"
    if not dst.exists() and not make_thumb(row["path"], dst):
        abort(404)
    resp = send_file(dst, mimetype="image/jpeg")
    resp.headers["Cache-Control"] = "max-age=86400"
    return resp


@app.get("/media/<int:post_id>")
def media(post_id):
    """The original file, so the post drawer can preview the video."""
    with db() as c:
        row = c.execute("SELECT path FROM posts WHERE id=?", (post_id,)).fetchone()
    if not row or not row["path"] or not Path(row["path"]).is_file():
        abort(404)
    return send_file(row["path"], conditional=True)


def graph_get(path, params, token):
    r = requests.get(f"{GRAPH}/{path}", params={**params, "access_token": token}, timeout=20)
    if not r.ok:
        raise RuntimeError(graph_error(r))
    return r.json()


def fetch_insights(limit=20):
    """Followers per Page plus reactions/comments on the most recent posts."""
    with db() as c:
        pages = c.execute("SELECT id, name, token FROM pages").fetchall()
        recent = c.execute("SELECT p.id, p.kind, p.result, pg.token FROM posts p JOIN pages pg ON pg.id=p.page_id "
                           "WHERE p.status='done' AND p.result IS NOT NULL ORDER BY p.posted_at DESC LIMIT ?",
                           (limit,)).fetchall()
    out = {"pages": [], "posts": {}, "followers": 0, "reactions": 0, "comments": 0, "errors": []}
    for pg in pages:
        try:
            d = graph_get(pg["id"], {"fields": "followers_count,fan_count"}, pg["token"])
            n = d.get("followers_count") or d.get("fan_count") or 0
            out["pages"].append({"id": pg["id"], "name": pg["name"], "followers": n})
            out["followers"] += n
        except Exception as e:
            out["errors"].append(f"{pg['name']}: {e}")
    for p in recent:
        fields = ("reactions.summary(total_count).limit(0),comments.summary(total_count).limit(0)")
        try:
            d = graph_get(p["result"], {"fields": fields}, p["token"])
            reactions = d.get("reactions", {}).get("summary", {}).get("total_count", 0)
            comments = d.get("comments", {}).get("summary", {}).get("total_count", 0)
            out["posts"][p["id"]] = {"reactions": reactions, "comments": comments}
            out["reactions"] += reactions
            out["comments"] += comments
        except Exception as e:
            out["errors"].append(f"post #{p['id']}: {e}")
    out["fetched"] = fmt_ts(time.time())
    return out


@app.get("/api/insights")
def api_insights():
    with _insights_lock:
        fresh = time.time() - _insights_cache["at"] < INSIGHTS_TTL
        if not fresh or request.args.get("refresh"):
            _insights_cache["data"] = fetch_insights()
            _insights_cache["at"] = time.time()
        return jsonify(_insights_cache["data"])


TEMPLATE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Page Auto-Poster</title>
<style>
:root{--bg:#f4f5f8;--panel:#fff;--panel2:#f8f9fb;--text:#1b1d22;--muted:#6b7080;--line:#e3e5ea;
 --accent:#3d6bff;--accent-soft:#e8eeff;--ok:#1a8f55;--ok-soft:#e3f6ec;--err:#d33a3a;--err-soft:#fdeaea;
 --warn:#b86e00;--warn-soft:#fff3dd;--shadow:0 1px 2px rgba(16,24,40,.06),0 4px 14px rgba(16,24,40,.06)}
@media (prefers-color-scheme:dark){:root{--bg:#121316;--panel:#1c1d22;--panel2:#23252b;--text:#e8e9ed;
 --muted:#9aa0ad;--line:#2f3138;--accent:#6b8cff;--accent-soft:#252c45;--ok:#4cc98a;--ok-soft:#1d3329;
 --err:#ff7070;--err-soft:#3a2224;--warn:#ffb547;--warn-soft:#3a2f1c;--shadow:0 1px 2px rgba(0,0,0,.3)}}
*{box-sizing:border-box}html,body{margin:0;height:100%}
body{background:var(--bg);color:var(--text);font:14px/1.45 system-ui,"Segoe UI",sans-serif}
button,input,select,textarea{font:inherit;color:inherit}
a{color:var(--accent)}
.app{display:grid;grid-template-columns:76px 1fr;min-height:100vh}
/* sidebar */
.side{background:var(--panel);border-right:1px solid var(--line);display:flex;flex-direction:column;
 align-items:center;padding:14px 0;gap:6px;position:sticky;top:0;height:100vh}
.logo{width:40px;height:40px;border-radius:12px;background:var(--accent);display:grid;place-items:center;margin-bottom:14px}
.nav{width:60px;border:0;background:none;border-radius:12px;padding:8px 0;cursor:pointer;color:var(--muted);
 display:flex;flex-direction:column;align-items:center;gap:3px;font-size:11px}
.nav svg{width:22px;height:22px}.nav:hover{background:var(--panel2)}
.nav.on{background:var(--accent-soft);color:var(--accent);font-weight:600}
/* main */
main{padding:20px 24px 40px;min-width:0}
.top{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:18px}
.top h1{font-size:20px;margin:0 8px 0 0}
.btn{border:1px solid var(--line);background:var(--panel);border-radius:10px;padding:7px 14px;cursor:pointer}
.btn:hover{border-color:var(--muted)}.btn.primary{background:var(--accent);border-color:var(--accent);color:#fff}
.btn.danger{color:var(--err)}.btn.round{width:34px;height:34px;padding:0;border-radius:50%}
.range{font-weight:600;font-size:15px;min-width:170px}
.spacer{flex:1}
.toast{padding:10px 14px;border-radius:10px;margin-bottom:14px;background:var(--ok-soft);color:var(--ok)}
.toast.err{background:var(--err-soft);color:var(--err)}
/* stats */
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:14px;margin-bottom:18px}
.stat{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:14px 16px;box-shadow:var(--shadow);
 display:flex;gap:12px;align-items:center;min-height:92px}
.stat .k{color:var(--muted);font-size:12px}.stat .v{font-size:26px;font-weight:700;line-height:1.1}
.stat .s{color:var(--muted);font-size:12px}.stat img{width:54px;height:54px;border-radius:10px;object-fit:cover}
.ring{width:64px;height:64px;flex:none}
/* calendar */
.cal{background:var(--panel);border:1px solid var(--line);border-radius:14px;box-shadow:var(--shadow);overflow-x:auto}
.week{display:grid;grid-template-columns:repeat(7,minmax(150px,1fr));min-width:1050px}
.day{border-right:1px solid var(--line);min-height:420px}.day:last-child{border-right:0}
.dh{position:sticky;top:0;text-align:center;padding:10px 4px 8px;border-bottom:1px solid var(--line);background:var(--panel)}
.dh .dn{color:var(--muted);font-size:12px}.dh .dd{font-size:20px;font-weight:600;width:36px;height:36px;line-height:36px;
 margin:2px auto 0;border-radius:50%}.day.today .dd{background:var(--text);color:var(--panel)}
.dh .cnt{font-size:11px;color:var(--muted)}
.cards{padding:8px;display:flex;flex-direction:column;gap:8px;max-height:640px;overflow-y:auto}
.card{flex:none;width:100%;border:1px solid var(--line);border-radius:10px;background:var(--panel2);padding:0;cursor:pointer;
 text-align:left;overflow:hidden;border-left:4px solid var(--pc,#999)}
.card:hover{border-color:var(--muted)}.card.sel{outline:2px solid var(--accent)}
.card .th{height:86px;background:var(--line) center/cover no-repeat;position:relative}
.card .th .badge{position:absolute;top:6px;right:6px}
.card .ph{height:44px;display:grid;place-items:center;color:var(--muted);font-weight:700;background:var(--line)}
.card .meta{padding:6px 8px 8px}.card .row{display:flex;justify-content:space-between;gap:6px;font-size:12px}
.card .pg{font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.card .cap{color:var(--muted);font-size:12px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;margin-top:2px}
.empty{color:var(--muted);font-size:12px;text-align:center;padding:16px 4px}
.badge{font-size:11px;padding:1px 7px;border-radius:99px;background:var(--panel);border:1px solid var(--line)}
.b-done{background:var(--ok-soft);color:var(--ok);border-color:transparent}
.b-failed{background:var(--err-soft);color:var(--err);border-color:transparent}
.b-posting{background:var(--warn-soft);color:var(--warn);border-color:transparent}
.b-skipped{color:var(--muted)}
.legend{display:flex;gap:14px;flex-wrap:wrap;margin:12px 2px 0;color:var(--muted);font-size:12px}
.legend i{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:5px;vertical-align:-1px}
/* drawer */
.drawer{position:fixed;top:0;right:0;width:400px;max-width:100vw;height:100vh;background:var(--panel);border-left:1px solid var(--line);
 box-shadow:-8px 0 30px rgba(0,0,0,.12);transform:translateX(105%);transition:transform .2s;z-index:20;display:flex;flex-direction:column}
.drawer.open{transform:none}
.dw-h{display:flex;align-items:center;justify-content:space-between;padding:14px 16px;border-bottom:1px solid var(--line)}
.dw-b{padding:16px;overflow-y:auto;flex:1}
.dw-b video,.dw-b img.prev{width:100%;max-height:420px;border-radius:12px;background:#000;display:block;margin-bottom:12px}
.dw-b .who{display:flex;align-items:center;gap:8px;margin-bottom:10px}
.dot{width:12px;height:12px;border-radius:50%;flex:none}
dl{display:grid;grid-template-columns:110px 1fr;gap:6px 10px;margin:12px 0}dt{color:var(--muted)}dd{margin:0;word-break:break-word}
.dw-f{padding:12px 16px;border-top:1px solid var(--line);display:flex;gap:8px;flex-wrap:wrap}
/* forms */
.grid{display:grid;gap:16px;grid-template-columns:repeat(auto-fit,minmax(320px,1fr))}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:16px;box-shadow:var(--shadow)}
.panel h2{font-size:15px;margin:0 0 10px}
label{display:block;font-size:12px;color:var(--muted);margin:10px 0 4px}
input,textarea,select{width:100%;padding:8px 10px;border:1px solid var(--line);border-radius:8px;background:var(--panel2)}
textarea{min-height:70px;resize:vertical}.row2{display:flex;gap:10px}.row2>*{flex:1}
.chk{display:flex;gap:8px;align-items:center;color:var(--text);font-size:14px;margin:4px 0}.chk input{width:auto}
.pagelist .p{display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid var(--line)}
.pagelist .p:last-child{border:0}.pagelist small{color:var(--muted)}
.ghost{border:0;background:none;color:var(--accent);cursor:pointer;padding:2px 6px}
/* list view (server fragment) */
table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:7px;border-bottom:1px solid var(--line);vertical-align:top}
th{font-size:12px;color:var(--muted)}td.res{max-width:300px;word-break:break-word;color:var(--muted)}
.s-done{color:var(--ok)}.s-failed{color:var(--err)}.s-posting{color:var(--warn)}.s-skipped{color:var(--muted)}
.tabs a{margin-right:12px}.tabs a.on{font-weight:600;color:var(--text);text-decoration:none}
.wrap{overflow-x:auto}form.inline{display:inline}small{color:var(--muted)}
.live{color:var(--ok);font-weight:400}.live::before{content:"\25CF ";animation:blink 2s infinite}
@keyframes blink{50%{opacity:.3}}
.view{display:none}.view.on{display:block}
@media (max-width:760px){
 .app{grid-template-columns:1fr}
 .side{position:fixed;bottom:0;top:auto;height:auto;width:100%;flex-direction:row;justify-content:space-around;
  border-right:0;border-top:1px solid var(--line);padding:4px 0;z-index:10}
 .logo{display:none}main{padding:16px 16px 90px}.drawer{width:100vw}}
</style></head>
<body>
<div class="app">
 <nav class="side">
  <div class="logo"><svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="#fff" stroke-width="2">
   <rect x="3" y="5" width="18" height="16" rx="3"/><path d="M3 10h18M8 3v4M16 3v4M9 15l2 2 4-4"/></svg></div>
  <button class="nav" data-view="calendar"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
   <rect x="3" y="5" width="18" height="16" rx="3"/><path d="M3 10h18M8 3v4M16 3v4"/></svg>Calendar</button>
  <button class="nav" data-view="list"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
   <path d="M8 6h13M8 12h13M8 18h13M3.5 6h.01M3.5 12h.01M3.5 18h.01"/></svg>Queue</button>
  <button class="nav" data-view="create"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
   <circle cx="12" cy="12" r="9"/><path d="M12 8v8M8 12h8"/></svg>Create</button>
  <button class="nav" data-view="pages"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
   <path d="M5 21V4h11l-1.5 4L16 12H5"/></svg>Pages</button>
 </nav>

 <main>
  {% for cat, msg in get_flashed_messages(with_categories=true) %}
   <div class="toast {{ 'err' if cat=='err' else '' }}">{{ msg }}</div>{% endfor %}

  <!-- ===== Calendar ===== -->
  <section class="view" id="v-calendar">
   <div class="top">
    <h1>Calendar</h1>
    <button class="btn round" id="prev" aria-label="Previous week">&#8249;</button>
    <button class="btn" id="today">Today</button>
    <button class="btn round" id="next" aria-label="Next week">&#8250;</button>
    <span class="range" id="range"></span>
    <span class="spacer"></span>
    <button class="btn primary" data-go="create">+ New posts</button>
   </div>
   <div class="stats" id="stats"></div>
   <div class="cal"><div class="week" id="week"></div></div>
   <div class="legend" id="legend"></div>
  </section>

  <!-- ===== Queue list ===== -->
  <section class="view" id="v-list">
   <div class="top"><h1>Queue</h1></div>
   <div class="panel" id="queue">{{ queue_html|safe }}</div>
  </section>

  <!-- ===== Create ===== -->
  <section class="view" id="v-create">
   <div class="top"><h1>Create</h1></div>
   <div class="grid">
    <div class="panel">
     <h2>Bulk schedule a folder</h2>
     <form method="post" action="{{ url_for('bulk') }}">
      <label>Folder with videos/photos</label><input name="folder" required placeholder="D:\Videos\ready">
      <label>Pages</label>
      {% for p in pages %}<label class="chk"><input type="checkbox" name="page_ids" value="{{ p.id }}">{{ p.name }}</label>{% endfor %}
      <label>Distribution</label>
      <select name="mode"><option value="split">Split files across Pages (each file posted once)</option>
       <option value="all">Every Page gets every file</option></select>
      <div class="row2"><div><label>Start</label><input type="datetime-local" name="start" value="{{ now }}"></div>
       <div><label>Minutes between posts (per Page)</label><input type="number" name="interval" value="240" min="1"></div></div>
      <label>Default caption</label><textarea name="caption"></textarea>
      <label>Captions CSV (optional: filename,caption)</label><input name="captions_csv" placeholder="D:\Videos\captions.csv">
      <small>A .txt file with the same name as a video overrides both.</small><br>
      <button class="btn primary" style="margin-top:12px">Queue folder</button>
     </form>
    </div>
    <div class="panel">
     <h2>Single post</h2>
     <form method="post" action="{{ url_for('single_post') }}">
      <label>Pages</label>
      {% for p in pages %}<label class="chk"><input type="checkbox" name="page_ids" value="{{ p.id }}">{{ p.name }}</label>{% endfor %}
      <label>Video/photo file (empty = text post)</label><input name="path" placeholder="D:\Videos\clip.mp4">
      <label>Caption / message</label><textarea name="caption"></textarea>
      <label>Link (text posts only)</label><input name="link" placeholder="https://...">
      <label>When</label><input type="datetime-local" name="run_at" value="{{ now }}">
      <button class="btn primary" style="margin-top:12px">Queue</button>
     </form>
    </div>
    <div class="panel">
     <h2>Queue tools</h2>
     <label>Page</label>
     <select id="t-page"><option value="">All Pages</option>
      {% for p in pages %}<option value="{{ p.id }}">{{ p.name }}</option>{% endfor %}</select>
     <div class="row2"><div><label>New start</label><input type="datetime-local" id="t-start" value="{{ now }}"></div>
      <div><label>Minutes between posts</label><input type="number" id="t-gap" value="240" min="1"></div></div>
     <button class="btn primary" style="margin-top:12px" id="t-resched">Re-space queue</button>
     <p><small>Keeps the order, moves every queued post onto the new start time and gap.</small></p>
     <button class="btn danger" id="t-clear">Delete all queued posts</button>
     <p><small>Done posts and history are kept, so the duplicate check still works.</small></p>
    </div>
   </div>
  </section>

  <!-- ===== Pages ===== -->
  <section class="view" id="v-pages">
   <div class="top"><h1>Pages</h1></div>
   <div class="grid">
    <div class="panel">
     <h2>Connected Pages</h2>
     <div class="pagelist" id="pagelist">
      {% for p in pages %}
      <div class="p"><span class="dot" data-page="{{ p.id }}"></span><span>{{ p.name }} <small>{{ p.id }}</small></span>
       <span class="spacer"></span><small data-followers="{{ p.id }}"></small>
       <form class="inline" method="post" action="{{ url_for('pages_delete', page_id=p.id) }}"
        onsubmit="return confirm('Remove {{ p.name }}? Its queued posts are deleted too.')"><button class="ghost">remove</button></form></div>
      {% else %}<small>No Pages yet. Import them with a token.</small>{% endfor %}
     </div>
    </div>
    <div class="panel">
     <h2>Import or refresh tokens</h2>
     <form method="post" action="{{ url_for('pages_import') }}">
      <label>User Access Token (extended)</label><input name="user_token" required placeholder="EAAB...">
      <button class="btn primary" style="margin-top:12px">Import</button>
     </form>
     <details style="margin-top:12px"><summary><small>Add one Page manually</small></summary>
      <form method="post" action="{{ url_for('pages_add') }}">
       <div class="row2"><div><label>Page ID</label><input name="page_id" required></div><div><label>Name</label><input name="name"></div></div>
       <label>Page Access Token</label><input name="token" required>
       <button class="btn" style="margin-top:10px">Save</button>
      </form></details>
    </div>
   </div>
  </section>
 </main>
</div>

<aside class="drawer" id="drawer" aria-hidden="true">
 <div class="dw-h"><strong id="dw-title">Post</strong><button class="btn round" id="dw-close" aria-label="Close">&times;</button></div>
 <div class="dw-b" id="dw-body"></div>
 <div class="dw-f" id="dw-foot"></div>
</aside>

<script>
(function () {
  const $ = s => document.querySelector(s);
  const DAY = 86400000;
  function h(tag, attrs, ...kids) {
    const e = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (k === "class") e.className = v;
      else if (k === "style") e.style.cssText = v;
      else if (k.startsWith("on")) e.addEventListener(k.slice(2), v);
      else if (v !== null && v !== undefined && v !== false) e.setAttribute(k, v === true ? "" : v);
    }
    for (const kid of kids.flat()) if (kid !== null && kid !== undefined && kid !== false)
      e.append(kid.nodeType ? kid : document.createTextNode(String(kid)));
    return e;
  }
  const time12 = t => new Date(t * 1000).toLocaleTimeString("en-US", {hour: "numeric", minute: "2-digit"});
  const full12 = t => new Date(t * 1000).toLocaleString("en-US", {month: "short", day: "numeric", year: "numeric", hour: "numeric", minute: "2-digit"});
  const sod = d => { const x = new Date(d); x.setHours(0, 0, 0, 0); return x; };
  const localInput = t => { const d = new Date(t * 1000); d.setMinutes(d.getMinutes() - d.getTimezoneOffset()); return d.toISOString().slice(0, 16); };
  function until(t) {
    const m = Math.round((t * 1000 - Date.now()) / 60000);
    if (m <= 0) return "any moment";
    if (m < 60) return `in ${m} min`;
    const hrs = Math.floor(m / 60), mm = m % 60;
    return hrs < 24 ? `in ${hrs} h ${mm} min` : `in ${Math.round(hrs / 24)} days`;
  }
  const store = {get(k) { try { return localStorage.getItem(k); } catch (e) { return null; } },
                 set(k, v) { try { localStorage.setItem(k, v); } catch (e) { /* private mode */ } }};

  let weekStart = sod(new Date()), state = null, insights = null, openId = null;
  const colorOf = id => (state && state.pages.find(p => p.id === id) || {}).color || "#999";

  /* ---------- views ---------- */
  function show(view) {
    document.querySelectorAll(".view").forEach(v => v.classList.toggle("on", v.id === "v-" + view));
    document.querySelectorAll(".nav").forEach(n => n.classList.toggle("on", n.dataset.view === view));
    store.set("view", view);
  }
  document.querySelectorAll("[data-view]").forEach(b => b.addEventListener("click", () => show(b.dataset.view)));
  document.querySelectorAll("[data-go]").forEach(b => b.addEventListener("click", () => show(b.dataset.go)));
  show(store.get("view") || "calendar");

  /* ---------- data ---------- */
  async function load() {
    try {
      const r = await fetch(`/api/state?start=${weekStart.getTime() / 1000}&days=7`, {cache: "no-store"});
      if (!r.ok) return;
      state = await r.json();
      renderStats(); renderWeek(); renderLegend();
      if (openId) refreshDrawer();
    } catch (e) { /* app restarting */ }
  }
  async function loadInsights(force) {
    try {
      const r = await fetch("/api/insights" + (force ? "?refresh=1" : ""), {cache: "no-store"});
      if (r.ok) { insights = await r.json(); renderStats(); renderFollowers(); }
    } catch (e) { /* offline */ }
  }

  /* ---------- stats ---------- */
  function ring(done, total) {
    const pct = total ? done / total : 0, r = 26, c = 2 * Math.PI * r;
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("viewBox", "0 0 64 64"); svg.setAttribute("class", "ring");
    svg.innerHTML = `<circle cx="32" cy="32" r="${r}" fill="none" stroke="var(--line)" stroke-width="7"/>
      <circle cx="32" cy="32" r="${r}" fill="none" stroke="var(--ok)" stroke-width="7" stroke-linecap="round"
       stroke-dasharray="${c * pct} ${c}" transform="rotate(-90 32 32)"/>
      <text x="32" y="37" text-anchor="middle" font-size="15" font-weight="700" fill="currentColor">${Math.round(pct * 100)}%</text>`;
    return svg;
  }
  function stat(k, v, s, lead) {
    return h("div", {class: "stat"}, lead || null, h("div", {}, h("div", {class: "k"}, k), h("div", {class: "v"}, v), h("div", {class: "s"}, s || "")));
  }
  function renderStats() {
    if (!state) return;
    const c = state.counts, n = state.next, total = state.posted_today + state.left_today;
    const ok = insights && insights.pages.length > 0;
    const box = $("#stats"); box.replaceChildren(
      stat("Today", `${state.posted_today}/${total}`, "posts published", ring(state.posted_today, total)),
      stat("Next post", n ? time12(n.run_at) : "—", n ? `${until(n.run_at)} · ${n.page_name || ""}` : "queue empty",
           n && n.path ? h("img", {src: `/thumb/${n.id}`, alt: "", onerror: e => e.target.remove()}) : null),
      stat("Queued", c.queued || 0, state.queue_ends ? `last: ${state.queue_ends}` : ""),
      stat("Published", c.done || 0, (c.failed ? `${c.failed} failed` : "no failures")),
      stat("Followers", ok ? insights.followers.toLocaleString() : "—",
           !insights ? "loading insights" : ok ? `across ${insights.pages.length} Page(s)` : "insights unavailable (hover)"),
      stat("Engagement", ok ? (insights.reactions + insights.comments).toLocaleString() : "—",
           ok ? `${insights.reactions} reactions · ${insights.comments} comments (recent)` : "")
    );
    if (insights && insights.errors.length) {
      const why = insights.errors.join("\n");
      box.children[4].title = why; box.children[5].title = why;
    }
  }

  /* ---------- calendar ---------- */
  function renderWeek() {
    const end = new Date(weekStart.getTime() + 6 * DAY);
    const f = d => d.toLocaleDateString("en-US", {month: "short", day: "numeric"});
    $("#range").textContent = `${f(weekStart)} – ${f(end)}, ${end.getFullYear()}`;
    const today = sod(new Date()).getTime(), cols = [];
    for (let i = 0; i < 7; i++) {
      const d = new Date(weekStart.getTime() + i * DAY), key = sod(d).getTime();
      const posts = state.posts.filter(p => sod(new Date(p.run_at * 1000)).getTime() === key);
      cols.push(h("div", {class: "day" + (key === today ? " today" : "")},
        h("div", {class: "dh"}, h("div", {class: "dn"}, d.toLocaleDateString("en-US", {weekday: "short"})),
          h("div", {class: "dd"}, d.getDate()), h("div", {class: "cnt"}, posts.length ? `${posts.length} post${posts.length > 1 ? "s" : ""}` : "")),
        h("div", {class: "cards"}, posts.length ? posts.map(card) : h("div", {class: "empty"}, "Nothing scheduled"))));
    }
    $("#week").replaceChildren(...cols);
  }
  function card(p) {
    const media = p.path
      ? h("div", {class: "th", style: `background-image:url('/thumb/${p.id}')`}, p.status !== "queued" ? h("span", {class: `badge b-${p.status}`}, p.status) : null)
      : h("div", {class: "ph"}, "Aa", p.status !== "queued" ? h("span", {class: `badge b-${p.status}`, style: "margin-left:6px"}, p.status) : null);
    return h("button", {class: "card" + (p.id === openId ? " sel" : ""), style: `--pc:${colorOf(p.page_id)}`, onclick: () => openDrawer(p.id)},
      media, h("div", {class: "meta"},
        h("div", {class: "row"}, h("span", {class: "pg"}, p.page_name || p.page_id), h("span", {}, time12(p.run_at))),
        h("div", {class: "cap"}, p.caption || p.file || "")));
  }
  function renderLegend() {
    $("#legend").replaceChildren(...state.pages.map(p => h("span", {}, h("i", {style: `background:${p.color}`}), `${p.name} · ${p.queued} queued`)));
    document.querySelectorAll("[data-page]").forEach(el => { el.style.background = colorOf(el.dataset.page); });
  }
  function renderFollowers() {
    if (!insights) return;
    insights.pages.forEach(p => { const el = document.querySelector(`[data-followers="${p.id}"]`); if (el) el.textContent = `${p.followers.toLocaleString()} followers`; });
  }
  $("#prev").onclick = () => { weekStart = new Date(weekStart.getTime() - 7 * DAY); load(); };
  $("#next").onclick = () => { weekStart = new Date(weekStart.getTime() + 7 * DAY); load(); };
  $("#today").onclick = () => { weekStart = sod(new Date()); load(); };

  /* ---------- drawer ---------- */
  async function api(url, body) {
    const r = await fetch(url, {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(body || {})});
    const data = await r.json().catch(() => ({}));
    if (!r.ok) alert(data.error || "Action failed.");
    return r.ok;
  }
  async function openDrawer(id) { openId = id; await refreshDrawer(); $("#drawer").classList.add("open"); $("#drawer").setAttribute("aria-hidden", "false"); renderWeek(); }
  function closeDrawer() { openId = null; $("#drawer").classList.remove("open"); $("#drawer").setAttribute("aria-hidden", "true"); const v = $("#dw-body video"); if (v) v.pause(); if (state) renderWeek(); }
  $("#dw-close").onclick = closeDrawer;
  document.addEventListener("keydown", e => { if (e.key === "Escape") closeDrawer(); });

  let drawnId = null, drawnStatus = null;
  async function refreshDrawer() {
    const r = await fetch(`/api/posts/${openId}`, {cache: "no-store"});
    if (!r.ok) { closeDrawer(); return; }
    const p = await r.json();
    if (drawnId === p.id && drawnStatus === p.status) return;  // keep video playing / edits typed
    drawnId = p.id; drawnStatus = p.status;
    const editable = p.status === "queued" || p.status === "failed";
    const eng = insights && insights.posts[p.id];
    $("#dw-title").textContent = p.kind === "video" ? "Video post" : p.kind === "photo" ? "Photo post" : "Text post";
    const preview = p.kind === "video" ? h("video", {src: `/media/${p.id}`, poster: `/thumb/${p.id}`, controls: true, preload: "none"})
      : p.kind === "photo" ? h("img", {class: "prev", src: `/media/${p.id}`, alt: ""}) : null;
    const cap = h("textarea", {id: "dw-cap", disabled: !editable}); cap.value = p.caption || "";
    const when = h("input", {type: "datetime-local", id: "dw-when", value: localInput(p.run_at), disabled: !editable});
    $("#dw-body").replaceChildren(...[
      preview,
      h("div", {class: "who"}, h("span", {class: "dot", style: `background:${colorOf(p.page_id)}`}), h("strong", {}, p.page_name || p.page_id),
        h("span", {class: `badge b-${p.status}`}, p.status)),
      h("label", {}, "Caption"), cap,
      h("label", {}, "Scheduled time"), when,
      h("dl", {},
        h("dt", {}, "File"), h("dd", {}, p.file || "—"),
        h("dt", {}, "Posted"), h("dd", {}, p.posted || "—"),
        eng ? [h("dt", {}, "Engagement"), h("dd", {}, `${eng.reactions} reactions · ${eng.comments} comments`)] : null,
        p.result ? [h("dt", {}, p.status === "done" ? "Facebook ID" : "Result"), h("dd", {}, p.result)] : null),
      p.fb_url ? h("a", {href: p.fb_url, target: "_blank", rel: "noopener"}, "View on Facebook ↗") : null
    ].filter(Boolean));
    const foot = [];
    if (editable) {
      foot.push(h("button", {class: "btn primary", onclick: async () => { if (await api(`/api/posts/${p.id}/now`)) { drawnStatus = null; load(); } }}, p.status === "failed" ? "Retry now" : "Post now"));
      foot.push(h("button", {class: "btn", onclick: async () => {
        if (await api(`/api/posts/${p.id}`, {caption: $("#dw-cap").value, run_at: $("#dw-when").value})) { drawnStatus = null; load(); }
      }}, "Save changes"));
    }
    if (p.status !== "posting") foot.push(h("button", {class: "btn danger", onclick: async () => {
      if (confirm(p.status === "done" ? "Remove from history? The post stays on Facebook, and this video could be queued again." : "Delete this post from the queue?"))
        { await api(`/api/posts/${p.id}/delete`); closeDrawer(); load(); }
    }}, "Delete"));
    $("#dw-foot").replaceChildren(...foot);
  }

  /* ---------- queue tools ---------- */
  $("#t-resched").onclick = async () => {
    const body = {page_id: $("#t-page").value, start: $("#t-start").value, gap: $("#t-gap").value};
    if (await api("/api/queue/reschedule", body)) { show("calendar"); weekStart = sod(new Date(body.start || Date.now())); load(); }
  };
  $("#t-clear").onclick = async () => {
    const page = $("#t-page");
    if (!confirm(`Delete all queued posts for ${page.value ? page.selectedOptions[0].text : "all Pages"}?`)) return;
    if (await api("/api/queue/clear", {page_id: page.value})) load();
  };

  /* ---------- live updates ---------- */
  // Live queue list: re-fetch the queue table every 3 s while that view is open.
  const box = $("#queue");
  setInterval(async () => {
    if (document.hidden) return;
    if ($("#v-list").classList.contains("on")) {
      try { const r = await fetch("/queue" + location.search, {cache: "no-store"}); if (r.ok) { const t = await r.text(); if (t !== box.innerHTML) box.innerHTML = t; } } catch (e) { /* restarting */ }
    }
  }, 3000);
  setInterval(() => { if (!document.hidden) load(); }, 5000);
  setInterval(() => loadInsights(false), 600000);
  document.addEventListener("visibilitychange", () => { if (!document.hidden) load(); });
  load(); loadInsights(false);
})();
</script>
</body></html>"""


QUEUE_TEMPLATE = """
 <h2>Queue <small class="live">live</small></h2>
 <div class="tabs">
  <a href="{{url_for('index')}}" class="{{'on' if not status}}">All</a>
  {% for s in ['queued','posting','done','skipped','failed'] %}
   <a href="{{url_for('index', status=s)}}" class="{{'on' if status==s}}">{{s}} ({{counts.get(s,0)}})</a>{% endfor %}
  <form class="inline" method="post" action="{{url_for('clear')}}"><input type="hidden" name="which" value="done">
   <button class="ghost">clear done</button></form>
 </div>
 <div class="wrap"><table>
  <tr><th>#</th><th>When</th><th>Page</th><th>Type</th><th>File / caption</th><th>Status</th><th>Result</th><th></th></tr>
  {% for p in posts %}
  <tr><td>{{p.id}}</td><td>{{p.run_at|ts}}</td><td>{{p.page_name or p.page_id}}</td><td>{{p.kind}}</td>
   <td>{{(p.path or '').split('\\\\')[-1]}}<br><small>{{(p.caption or '')[:80]}}</small></td>
   <td class="s-{{p.status}}">{{p.status}}</td><td class="res">{{p.result or ''}}</td>
   <td>{% if p.status=='failed' %}<form class="inline" method="post" action="{{url_for('retry', post_id=p.id)}}">
     <button class="ghost">retry</button></form>{% endif %}
    {% if p.status!='posting' %}<form class="inline" method="post" action="{{url_for('delete_post', post_id=p.id)}}">
     <button class="ghost">delete</button></form>{% endif %}</td></tr>
  {% else %}<tr><td colspan="8"><small>Queue empty.</small></td></tr>{% endfor %}
 </table></div>
"""


if __name__ == "__main__":
    init_db()
    threading.Thread(target=worker, daemon=True).start()
    # 127.0.0.1 only: tokens never exposed to your network.
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", 5000)), debug=False)
