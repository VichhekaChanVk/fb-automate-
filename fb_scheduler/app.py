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
TOKEN_DEAD = "(code 190)"  # Graph API: access token expired, revoked, or logged out

app = Flask(__name__)
app.secret_key = "local-only"


# ---------------------------------------------------------------- database

def db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def shift_missed(c, page_ids=None):
    """Posts whose time already passed (app off, or Page paused): push that Page's queue
    forward so it resumes from now with the same gaps, instead of firing them all at once."""
    now = time.time()
    rows = c.execute("SELECT page_id, MIN(run_at) AS first FROM posts WHERE status='queued' "
                     "GROUP BY page_id HAVING first < ?", (now - 60,)).fetchall()
    for r in rows:
        if page_ids is not None and r["page_id"] not in page_ids:
            continue
        c.execute("UPDATE posts SET run_at = run_at + ? WHERE page_id=? AND status='queued'",
                  (now - r["first"], r["page_id"]))
        print(f"[resume] Page {r['page_id']}: missed posts moved forward {(now - r['first']) / 3600:.1f} h")


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
        shift_missed(c)
        # Older databases: add the fingerprint column and fill it for files still on disk.
        cols = {r["name"] for r in c.execute("PRAGMA table_info(posts)")}
        if "file_hash" not in cols:
            c.execute("ALTER TABLE posts ADD COLUMN file_hash TEXT")
        if "token_error" not in {r["name"] for r in c.execute("PRAGMA table_info(pages)")}:
            # Set when Facebook rejects a Page's token; that Page's queue pauses until re-import.
            c.execute("ALTER TABLE pages ADD COLUMN token_error TEXT")
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
                    "WHERE p.status='queued' AND p.run_at <= ? AND pg.token_error IS NULL "
                    "ORDER BY p.run_at LIMIT 1",
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
                if status == "failed" and TOKEN_DEAD in result:
                    # Token expired or revoked: every post for this Page would fail the same way.
                    # Pause the Page and keep this post queued; re-importing a token resumes it.
                    c.execute("UPDATE pages SET token_error=? WHERE id=?", (result, row["page_id"]))
                    c.execute("UPDATE posts SET status='queued', result=NULL WHERE id=?", (row["id"],))
                    print(f"[worker] Page {row['page_id']} paused, token rejected: {result}")
                    continue
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
            # REPLACE also clears token_error, so a paused Page starts posting again.
            c.execute("INSERT OR REPLACE INTO pages(id, name, token) VALUES (?,?,?)",
                      (p["id"], p["name"], p["access_token"]))
        shift_missed(c, {p["id"] for p in found})
    flash(f"Imported {len(found)} Page(s).", "ok")
    return redirect(url_for("index"))


@app.post("/pages/add")
def pages_add():
    f = request.form
    with db() as c:
        c.execute("INSERT OR REPLACE INTO pages(id, name, token) VALUES (?,?,?)",
                  (f["page_id"].strip(), f["name"].strip() or f["page_id"].strip(), f["token"].strip()))
        shift_missed(c, {f["page_id"].strip()})
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
        pages = [dict(r) for r in c.execute("SELECT id, name, token_error FROM pages ORDER BY name")]
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


@app.post("/api/queue/retry_failed")
def api_retry_failed():
    """Re-queue failed posts at their original times, then slide the Page's queue so they
    go out from now on with the same gaps (instead of all at once)."""
    page_id = (request.get_json(silent=True) or {}).get("page_id")
    with db() as c:
        ids = {r[0] for r in c.execute("SELECT DISTINCT page_id FROM posts WHERE status='failed'"
                                       + (" AND page_id=?" if page_id else ""), (page_id,) if page_id else ())}
        n = c.execute("UPDATE posts SET status='queued', result=NULL WHERE status='failed'"
                      + (" AND page_id=?" if page_id else ""), (page_id,) if page_id else ()).rowcount
        shift_missed(c, ids)
    return jsonify(ok=True, retried=n)


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
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
/* ---------- tokens ---------- */
:root{
 --bg:#f6f7f9;--surface:#ffffff;--surface-2:#f1f3f6;--surface-3:#e8ebf0;--border:#e3e6eb;--border-strong:#cfd4dc;
 --text:#111827;--text-2:#374151;--muted:#5b6472;
 --primary:#2563eb;--primary-hover:#1d4ed8;--on-primary:#fff;--primary-soft:#eaf1ff;--ring:#2563eb;
 --ok:#15803d;--ok-soft:#e7f6ec;--warn:#b45309;--warn-soft:#fff4e0;--err:#c62828;--err-soft:#fdecec;--info:#1d4ed8;--info-soft:#eaf1ff;
 --shadow-1:0 1px 2px rgba(16,24,40,.05);--shadow-2:0 1px 3px rgba(16,24,40,.08),0 6px 16px rgba(16,24,40,.06);
 --shadow-3:0 12px 40px rgba(16,24,40,.18);--scrim:rgba(15,23,42,.45);
 --r-sm:8px;--r-md:12px;--r-lg:16px;--sidebar:240px;
 --ease-out:cubic-bezier(.2,.8,.2,1);--t-fast:150ms;--t-med:220ms}
:root[data-theme="dark"]{
 --bg:#0e1015;--surface:#161920;--surface-2:#1c2029;--surface-3:#252a35;--border:#2a2f3b;--border-strong:#3a4150;
 --text:#e8eaf0;--text-2:#c9cdd6;--muted:#a0a8b8;
 --primary:#60a5fa;--primary-hover:#93c5fd;--on-primary:#0b1220;--primary-soft:#1b2a44;--ring:#93c5fd;
 --ok:#4ade80;--ok-soft:#173225;--warn:#fbbf24;--warn-soft:#3a2d12;--err:#f87171;--err-soft:#3b1d1f;--info:#93c5fd;--info-soft:#1b2a44;
 --shadow-1:none;--shadow-2:0 1px 2px rgba(0,0,0,.4);--shadow-3:0 16px 48px rgba(0,0,0,.55);--scrim:rgba(0,0,0,.6)}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){
 --bg:#0e1015;--surface:#161920;--surface-2:#1c2029;--surface-3:#252a35;--border:#2a2f3b;--border-strong:#3a4150;
 --text:#e8eaf0;--text-2:#c9cdd6;--muted:#a0a8b8;
 --primary:#60a5fa;--primary-hover:#93c5fd;--on-primary:#0b1220;--primary-soft:#1b2a44;--ring:#93c5fd;
 --ok:#4ade80;--ok-soft:#173225;--warn:#fbbf24;--warn-soft:#3a2d12;--err:#f87171;--err-soft:#3b1d1f;--info:#93c5fd;--info-soft:#1b2a44;
 --shadow-1:none;--shadow-2:0 1px 2px rgba(0,0,0,.4);--shadow-3:0 16px 48px rgba(0,0,0,.55);--scrim:rgba(0,0,0,.6)}}
/* ---------- base ---------- */
*{box-sizing:border-box}html,body{margin:0}
body{background:var(--bg);color:var(--text);font:15px/1.5 "Plus Jakarta Sans",system-ui,"Segoe UI",sans-serif;
 -webkit-font-smoothing:antialiased;min-height:100dvh}
button,input,select,textarea{font:inherit;color:inherit}
button{cursor:pointer;touch-action:manipulation}
a{color:var(--primary)}
:focus-visible{outline:2px solid var(--ring);outline-offset:2px;border-radius:6px}
.sr-only{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap}
.skip{position:absolute;left:12px;top:-60px;z-index:100;background:var(--primary);color:var(--on-primary);padding:10px 14px;border-radius:var(--r-sm)}
.skip:focus{top:12px}
.num{font-variant-numeric:tabular-nums}
svg.i{width:20px;height:20px;flex:none;stroke:currentColor;fill:none;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}
svg.i-sm{width:16px;height:16px}
/* ---------- layout ---------- */
.app{display:grid;grid-template-columns:var(--sidebar) 1fr;min-height:100dvh}
.side{position:sticky;top:0;height:100dvh;background:var(--surface);border-right:1px solid var(--border);
 display:flex;flex-direction:column;padding:18px 14px;gap:4px;z-index:10}
.brand{display:flex;align-items:center;gap:10px;padding:4px 8px 18px;font-weight:700;font-size:16px;letter-spacing:-.01em}
.brand-mark{width:34px;height:34px;border-radius:10px;background:var(--primary);color:var(--on-primary);display:grid;place-items:center}
.nav{display:flex;align-items:center;gap:12px;min-height:44px;padding:0 12px;border:0;background:none;border-radius:var(--r-sm);
 color:var(--muted);font-weight:500;text-align:left;width:100%;text-decoration:none;transition:background var(--t-fast),color var(--t-fast)}
.nav:hover{background:var(--surface-2);color:var(--text)}
.nav[aria-current="page"]{background:var(--primary-soft);color:var(--primary);font-weight:600}
.nav .count{margin-left:auto;font-size:12px;font-weight:600;background:var(--surface-3);color:var(--text-2);border-radius:99px;padding:1px 8px}
.side-foot{margin-top:auto;display:flex;flex-direction:column;gap:6px;border-top:1px solid var(--border);padding-top:12px}
.conn{display:flex;align-items:center;gap:8px;font-size:12px;color:var(--muted);padding:0 12px;min-height:32px}
.conn .pulse{width:8px;height:8px;border-radius:50%;background:var(--ok);box-shadow:0 0 0 3px var(--ok-soft)}
.conn.off .pulse{background:var(--err);box-shadow:0 0 0 3px var(--err-soft)}
main{min-width:0;padding:24px 28px 48px}
.page-head{display:flex;align-items:flex-end;gap:16px;flex-wrap:wrap;margin-bottom:20px}
.page-head h1{font-size:24px;line-height:1.2;margin:0;letter-spacing:-.02em}
.page-head p{margin:4px 0 0;color:var(--muted);font-size:14px}
.grow{flex:1}
/* ---------- buttons ---------- */
.btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;min-height:40px;padding:0 16px;border-radius:var(--r-sm);
 border:1px solid var(--border-strong);background:var(--surface);color:var(--text);font-weight:600;font-size:14px;text-decoration:none;
 white-space:nowrap;transition:background var(--t-fast),border-color var(--t-fast),transform var(--t-fast)}
.btn:hover{background:var(--surface-2)}.btn:active{transform:scale(.98)}
.btn.primary{background:var(--primary);border-color:var(--primary);color:var(--on-primary)}.btn.primary:hover{background:var(--primary-hover);border-color:var(--primary-hover)}
.btn.danger{color:var(--err);border-color:var(--border)}.btn.danger:hover{background:var(--err-soft);border-color:var(--err)}
.btn.icon{width:40px;padding:0}.btn.ghost{border-color:transparent;background:transparent}.btn.ghost:hover{background:var(--surface-2)}
.btn[disabled]{opacity:.5;cursor:not-allowed}
.seg{display:inline-flex;border:1px solid var(--border-strong);border-radius:var(--r-sm);overflow:hidden;background:var(--surface)}
.seg .btn{border:0;border-radius:0;min-height:38px}.seg .btn+.btn{border-left:1px solid var(--border)}
.range{font-weight:600;font-size:15px;white-space:nowrap}
/* ---------- toasts ---------- */
.toasts{position:fixed;right:20px;bottom:20px;display:flex;flex-direction:column;gap:8px;z-index:60;max-width:min(380px,calc(100vw - 32px))}
.toast{display:flex;gap:10px;align-items:flex-start;background:var(--surface);border:1px solid var(--border);border-left:4px solid var(--ok);
 border-radius:var(--r-md);padding:12px 14px;box-shadow:var(--shadow-3);animation:toast-in var(--t-med) var(--ease-out)}
.toast.err{border-left-color:var(--err)}.toast svg{color:var(--ok)}.toast.err svg{color:var(--err)}
@keyframes toast-in{from{opacity:0;transform:translateY(8px)}}
/* ---------- stats ---------- */
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(168px,1fr));gap:12px;margin-bottom:20px}
@media (min-width:1360px){.stats{grid-template-columns:repeat(6,1fr)}}
.stat{background:var(--surface);border:1px solid var(--border);border-radius:var(--r-lg);padding:16px;box-shadow:var(--shadow-1);
 display:flex;gap:14px;align-items:center;min-height:96px}
.stat .ico{width:40px;height:40px;border-radius:10px;display:grid;place-items:center;flex:none}
.stat .k{font-size:13px;color:var(--muted);font-weight:500}
.stat .v{font-size:22px;font-weight:700;line-height:1.2;letter-spacing:-.02em;white-space:nowrap}
.stat .s{font-size:12px;color:var(--muted);margin-top:2px}
.stat img.th{width:48px;height:48px;border-radius:10px;object-fit:cover;flex:none;background:var(--surface-3)}
.t-blue{background:var(--info-soft);color:var(--info)}.t-green{background:var(--ok-soft);color:var(--ok)}
.t-amber{background:var(--warn-soft);color:var(--warn)}.t-rose{background:var(--err-soft);color:var(--err)}
.ring{width:56px;height:56px;flex:none}
/* ---------- filters ---------- */
.toolbar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:14px}
.chip{display:inline-flex;align-items:center;gap:8px;min-height:36px;padding:0 12px;border-radius:99px;border:1px solid var(--border-strong);
 background:var(--surface);color:var(--text-2);font-size:13px;font-weight:500;transition:background var(--t-fast),border-color var(--t-fast)}
.chip:hover{background:var(--surface-2)}.chip[aria-pressed="true"]{background:var(--text);color:var(--surface);border-color:var(--text)}
.dot{width:10px;height:10px;border-radius:50%;flex:none;display:inline-block}
.sep{width:1px;height:24px;background:var(--border);margin:0 4px}
/* ---------- calendar ---------- */
.cal{background:var(--surface);border:1px solid var(--border);border-radius:var(--r-lg);box-shadow:var(--shadow-1);overflow:auto;max-height:calc(100dvh - 120px)}
.week{display:grid;grid-template-columns:repeat(7,minmax(160px,1fr));min-width:1120px}
.day{border-right:1px solid var(--border);display:flex;flex-direction:column;min-height:460px}.day:last-child{border-right:0}
.dh{position:sticky;top:0;z-index:2;background:var(--surface);border-bottom:1px solid var(--border);padding:12px 12px 10px;
 display:flex;align-items:center;gap:10px}
.dh .dd{font-size:20px;font-weight:700;width:38px;height:38px;border-radius:50%;display:grid;place-items:center;letter-spacing:-.02em}
.dh .dn{font-size:12px;color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.04em}
.dh .cnt{margin-left:auto;font-size:12px;color:var(--muted)}
.day.today .dd{background:var(--primary);color:var(--on-primary)}.day.today .dn{color:var(--primary)}
.day.past{background:color-mix(in srgb,var(--surface-2) 45%,transparent)}
.cards{padding:10px;display:flex;flex-direction:column;gap:10px}
.card{flex:none;width:100%;display:block;text-align:left;padding:0;border:1px solid var(--border);border-radius:var(--r-md);
 background:var(--surface);overflow:hidden;box-shadow:var(--shadow-1);transition:border-color var(--t-fast),box-shadow var(--t-fast),transform var(--t-fast)}
.card:hover{border-color:var(--border-strong);box-shadow:var(--shadow-2);transform:translateY(-1px)}
.card[aria-current="true"]{border-color:var(--primary);box-shadow:0 0 0 2px var(--primary)}
.media{position:relative;aspect-ratio:16/10;background:var(--surface-3) center/cover no-repeat}
.media .play{position:absolute;left:8px;bottom:8px;width:26px;height:26px;border-radius:50%;background:rgba(0,0,0,.55);color:#fff;display:grid;place-items:center}
.media .play svg{width:12px;height:12px;fill:#fff;stroke:none}
.media.text{aspect-ratio:auto;height:52px;display:grid;place-items:center;color:var(--muted)}
.card .body{padding:10px 12px 12px;border-top:3px solid var(--pc,var(--border))}
.card .r1{display:flex;align-items:center;gap:6px;font-size:12px;flex-wrap:wrap}
.card .time{font-weight:700;font-size:13px;white-space:nowrap}
.card .pgline{display:flex;align-items:center;gap:6px;font-size:12px;margin-top:2px;min-width:0}
.card .pg{color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0}
.card .cap{font-size:13px;color:var(--text-2);margin-top:6px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;overflow-wrap:anywhere}
.status{display:inline-flex;align-items:center;gap:4px;font-size:11px;font-weight:600;border-radius:99px;padding:2px 8px;white-space:nowrap}
.status svg{width:12px;height:12px}
.st-queued{background:var(--info-soft);color:var(--info)}.st-done{background:var(--ok-soft);color:var(--ok)}
.st-failed{background:var(--err-soft);color:var(--err)}.st-posting{background:var(--warn-soft);color:var(--warn)}
.st-skipped{background:var(--surface-3);color:var(--muted)}
.media .status{position:absolute;top:8px;right:8px;box-shadow:var(--shadow-1)}
.alert{display:flex;gap:12px;align-items:flex-start;flex-wrap:wrap;background:var(--err-soft);color:var(--text);border:1px solid var(--err);
 border-radius:var(--r-md);padding:12px 16px;margin-bottom:12px}
.alert>svg{color:var(--err);margin-top:2px}.alert .msg{flex:1;min-width:220px}.alert .msg b{display:block}
.alert .msg small{color:var(--text-2);overflow-wrap:anywhere}.alert.warn{background:var(--warn-soft);border-color:var(--warn)}.alert.warn>svg{color:var(--warn)}
.empty-day{margin:10px;border:1px dashed var(--border-strong);border-radius:var(--r-md);padding:18px 8px;text-align:center;color:var(--muted);font-size:12px}
.more{font-size:12px;color:var(--muted);text-align:center;padding:2px 0 8px}
.skel{background:linear-gradient(90deg,var(--surface-2),var(--surface-3),var(--surface-2));background-size:200% 100%;animation:shimmer 1.2s infinite;border-radius:var(--r-md)}
@keyframes shimmer{to{background-position:-200% 0}}
/* ---------- drawer ---------- */
.scrim{position:fixed;inset:0;background:var(--scrim);opacity:0;pointer-events:none;transition:opacity var(--t-med);z-index:40}
.scrim.open{opacity:1;pointer-events:auto}
.drawer{position:fixed;top:0;right:0;width:440px;max-width:100vw;height:100dvh;background:var(--surface);border-left:1px solid var(--border);
 box-shadow:var(--shadow-3);transform:translateX(100%);transition:transform var(--t-med) var(--ease-out);z-index:50;display:flex;flex-direction:column}
.drawer.open{transform:none}
.dw-h{display:flex;align-items:center;gap:10px;padding:14px 16px 14px 20px;border-bottom:1px solid var(--border)}
.dw-h h2{font-size:16px;margin:0}
.dw-b{padding:20px;overflow-y:auto;flex:1}
.preview{width:100%;max-height:440px;border-radius:var(--r-md);background:#000;display:block;margin-bottom:16px;object-fit:contain}
.who{display:flex;align-items:center;gap:10px;margin-bottom:14px}
.avatar{width:36px;height:36px;border-radius:10px;display:grid;place-items:center;color:#fff;font-weight:700;font-size:14px;flex:none}
.who .nm{font-weight:600}.who .sub{font-size:12px;color:var(--muted)}
.meta{display:grid;grid-template-columns:120px 1fr;gap:8px 12px;margin:16px 0;font-size:14px}
.meta dt{color:var(--muted)}.meta dd{margin:0;overflow-wrap:anywhere}
.dw-f{padding:14px 20px;border-top:1px solid var(--border);display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.dw-f .danger{margin-left:auto}
/* ---------- forms & panels ---------- */
.grid{display:grid;gap:16px;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));align-items:start}
.panel{background:var(--surface);border:1px solid var(--border);border-radius:var(--r-lg);padding:20px;box-shadow:var(--shadow-1)}
.panel h2{font-size:16px;margin:0 0 4px}.panel .lead{color:var(--muted);font-size:13px;margin:0 0 12px}
.field{margin-top:14px}
label,.lbl{display:block;font-size:13px;font-weight:600;color:var(--text-2);margin-bottom:6px}
.help{font-size:12px;color:var(--muted);margin-top:6px}
input,textarea,select{width:100%;min-height:42px;padding:9px 12px;border:1px solid var(--border-strong);border-radius:var(--r-sm);
 background:var(--surface);transition:border-color var(--t-fast),box-shadow var(--t-fast)}
input:focus,textarea:focus,select:focus{outline:none;border-color:var(--primary);box-shadow:0 0 0 3px var(--primary-soft)}
textarea{min-height:84px;resize:vertical}
input[disabled],textarea[disabled]{background:var(--surface-2);color:var(--muted)}
.row2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
fieldset{border:0;margin:14px 0 0;padding:0}legend{font-size:13px;font-weight:600;color:var(--text-2);margin-bottom:6px;padding:0}
.checks{display:flex;flex-wrap:wrap;gap:8px}
.check{position:relative}.check input{position:absolute;opacity:0;width:1px;height:1px;min-height:0}
.check span{display:inline-flex;align-items:center;gap:8px;min-height:38px;padding:0 14px;border-radius:99px;border:1px solid var(--border-strong);font-size:14px;cursor:pointer}
.check input:checked+span{background:var(--primary-soft);border-color:var(--primary);color:var(--primary);font-weight:600}
.check input:focus-visible+span{outline:2px solid var(--ring);outline-offset:2px}
.actions{display:flex;gap:8px;margin-top:18px;flex-wrap:wrap}
.divider{height:1px;background:var(--border);margin:18px 0}
/* pages */
.plist{display:flex;flex-direction:column}
.prow{display:flex;align-items:center;gap:12px;padding:12px 0;border-bottom:1px solid var(--border)}.prow:last-child{border:0}
.prow .info{min-width:0;flex:1}.prow .nm{font-weight:600}.prow .sub{font-size:12px;color:var(--muted)}
.empty{text-align:center;color:var(--muted);padding:28px 8px}
/* list view (server fragment) */
#queue h2{font-size:16px;margin:0 0 10px}
table{width:100%;border-collapse:collapse;font-size:14px}th,td{text-align:left;padding:10px 8px;border-bottom:1px solid var(--border);vertical-align:top}
th{font-size:12px;color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.03em}td.res{max-width:320px;overflow-wrap:anywhere;color:var(--muted)}
.s-done{color:var(--ok)}.s-failed{color:var(--err)}.s-posting{color:var(--warn)}.s-skipped{color:var(--muted)}
.tabs{display:flex;gap:6px 14px;flex-wrap:wrap;align-items:center;margin-bottom:8px}.tabs a{text-decoration:none;font-size:14px}
.tabs a.on{font-weight:700;color:var(--text)}.wrap{overflow-x:auto}form.inline{display:inline}small{color:var(--muted)}
.ghost{border:0;background:none;color:var(--primary);cursor:pointer;padding:4px 8px;border-radius:6px;font-weight:600}.ghost:hover{background:var(--surface-2)}
.live{color:var(--ok);font-weight:500;font-size:12px}.live::before{content:"\25CF ";animation:blink 2s infinite}
@keyframes blink{50%{opacity:.35}}
.view{display:none}.view.on{display:block}
/* ---------- responsive ---------- */
@media (max-width:1100px){
 :root{--sidebar:76px}.brand span,.nav .lbl-t,.nav .count,.conn span{display:none}
 .side{align-items:center;padding:16px 8px}.brand{padding:4px 0 18px}.nav{justify-content:center;padding:0;width:52px}}
@media (max-width:767px){
 .app{grid-template-columns:1fr}
 .side{position:fixed;inset:auto 0 0 0;height:auto;flex-direction:row;justify-content:space-around;padding:6px 8px calc(6px + env(safe-area-inset-bottom));
  border-right:0;border-top:1px solid var(--border);z-index:30}
 .brand,.side-foot{display:none}.nav{flex-direction:column;gap:2px;width:auto;min-width:64px;min-height:52px;font-size:11px}
 .nav .lbl-t{display:block}
 main{padding:16px 16px 96px}.page-head h1{font-size:20px}
 .stats{grid-template-columns:1fr 1fr;gap:10px}
 .stat{flex-direction:column;align-items:flex-start;gap:8px;padding:12px;min-height:0}
 .stat .ico{width:32px;height:32px}.stat .ico svg{width:18px;height:18px}.ring{width:44px;height:44px}
 .stat img.th{width:36px;height:36px}.stat .v{font-size:20px}
 .cal{overflow:visible;max-height:none;border:0;background:none;box-shadow:none}
 .week{grid-template-columns:1fr;min-width:0;gap:12px}
 .day{border:1px solid var(--border);border-radius:var(--r-lg);min-height:0;background:var(--surface)}
 .dh{position:static;border-radius:var(--r-lg) var(--r-lg) 0 0}
 .card{display:grid;grid-template-columns:96px 1fr}.media{aspect-ratio:auto;height:100%;min-height:84px}
 .card .body{border-top:0;border-left:3px solid var(--pc,var(--border))}.media .status{display:none}
 .drawer{width:100vw}.toasts{bottom:84px;right:16px}.row2{grid-template-columns:1fr}.grid{grid-template-columns:1fr}}
@media (prefers-reduced-motion:reduce){*,*::before,*::after{animation:none!important;transition:none!important}}
</style></head>
<body>
<a class="skip" href="#main">Skip to content</a>
<svg width="0" height="0" style="position:absolute" aria-hidden="true">
 <symbol id="i-cal" viewBox="0 0 24 24"><rect x="3" y="4.5" width="18" height="16.5" rx="3"/><path d="M3 9.5h18M8 2.5v4M16 2.5v4"/></symbol>
 <symbol id="i-list" viewBox="0 0 24 24"><path d="M9 6h12M9 12h12M9 18h12M4 6h.01M4 12h.01M4 18h.01"/></symbol>
 <symbol id="i-plus" viewBox="0 0 24 24"><path d="M12 5v14M5 12h14"/></symbol>
 <symbol id="i-flag" viewBox="0 0 24 24"><path d="M5 21V4M5 4h11l-1.5 4L16 12H5"/></symbol>
 <symbol id="i-left" viewBox="0 0 24 24"><path d="M15 18l-6-6 6-6"/></symbol>
 <symbol id="i-right" viewBox="0 0 24 24"><path d="M9 18l6-6-6-6"/></symbol>
 <symbol id="i-x" viewBox="0 0 24 24"><path d="M18 6L6 18M6 6l12 12"/></symbol>
 <symbol id="i-clock" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></symbol>
 <symbol id="i-layers" viewBox="0 0 24 24"><path d="M12 3l9 5-9 5-9-5 9-5z"/><path d="M3 13l9 5 9-5"/></symbol>
 <symbol id="i-check" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M8.5 12.5l2.5 2.5 4.5-5"/></symbol>
 <symbol id="i-users" viewBox="0 0 24 24"><circle cx="9" cy="8" r="3.5"/><path d="M2.5 20a6.5 6.5 0 0113 0M16 4.5a3.5 3.5 0 010 7M21.5 20a6.5 6.5 0 00-4-6"/></symbol>
 <symbol id="i-heart" viewBox="0 0 24 24"><path d="M20.8 5.6a5 5 0 00-7.1 0L12 7.3l-1.7-1.7a5 5 0 10-7.1 7.1L12 21.5l8.8-8.8a5 5 0 000-7.1z"/></symbol>
 <symbol id="i-alert" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M12 7.5v5M12 16.5h.01"/></symbol>
 <symbol id="i-sun" viewBox="0 0 24 24"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></symbol>
 <symbol id="i-moon" viewBox="0 0 24 24"><path d="M21 12.8A9 9 0 1111.2 3a7 7 0 009.8 9.8z"/></symbol>
 <symbol id="i-auto" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M12 3v18" /><path d="M12 3a9 9 0 010 18z" fill="currentColor"/></symbol>
 <symbol id="i-refresh" viewBox="0 0 24 24"><path d="M21 12a9 9 0 01-15.5 6.2L3 16M3 12a9 9 0 0115.5-6.2L21 8M21 3v5h-5M3 21v-5h5"/></symbol>
 <symbol id="i-ext" viewBox="0 0 24 24"><path d="M14 4h6v6M20 4l-9 9M18 14v5a1 1 0 01-1 1H5a1 1 0 01-1-1V7a1 1 0 011-1h5"/></symbol>
 <symbol id="i-text" viewBox="0 0 24 24"><path d="M4 7V5h16v2M9 19h6M12 5v14"/></symbol>
 <symbol id="i-send" viewBox="0 0 24 24"><path d="M22 2L11 13M22 2l-7 20-4-9-9-4 20-7z"/></symbol>
 <symbol id="i-trash" viewBox="0 0 24 24"><path d="M3 6h18M8 6V4h8v2M19 6l-1 14H6L5 6"/></symbol>
</svg>

<div class="app">
 <nav class="side" aria-label="Main">
  <div class="brand"><span class="brand-mark"><svg class="i" aria-hidden="true"><use href="#i-send"/></svg></span><span>Auto-Poster</span></div>
  <a class="nav" href="#calendar" data-view="calendar"><svg class="i" aria-hidden="true"><use href="#i-cal"/></svg><span class="lbl-t">Calendar</span></a>
  <a class="nav" href="#list" data-view="list"><svg class="i" aria-hidden="true"><use href="#i-list"/></svg><span class="lbl-t">Queue</span><span class="count num" id="nav-queued"></span></a>
  <a class="nav" href="#create" data-view="create"><svg class="i" aria-hidden="true"><use href="#i-plus"/></svg><span class="lbl-t">Create</span></a>
  <a class="nav" href="#pages" data-view="pages"><svg class="i" aria-hidden="true"><use href="#i-flag"/></svg><span class="lbl-t">Pages</span><span class="count num">{{ pages|length }}</span></a>
  <div class="side-foot">
   <div class="conn" id="conn" role="status"><i class="pulse"></i><span id="conn-t">Connecting…</span></div>
   <button class="nav" id="theme" type="button" aria-label="Theme: system"><svg class="i" aria-hidden="true"><use href="#i-auto"/></svg><span class="lbl-t" id="theme-t">System theme</span></button>
  </div>
 </nav>

 <main id="main" tabindex="-1">
  <!-- ===== Calendar ===== -->
  <section class="view" id="v-calendar" aria-labelledby="h-cal">
   <div class="page-head">
    <div><h1 id="h-cal">Calendar</h1><p id="range-sub">Your scheduled and published posts</p></div>
    <span class="grow"></span>
    <div class="seg" role="group" aria-label="Week navigation">
     <button class="btn icon" id="prev" type="button" aria-label="Previous week"><svg class="i" aria-hidden="true"><use href="#i-left"/></svg></button>
     <button class="btn" id="today" type="button">Today</button>
     <button class="btn icon" id="next" type="button" aria-label="Next week"><svg class="i" aria-hidden="true"><use href="#i-right"/></svg></button>
    </div>
    <span class="range num" id="range" aria-live="polite"></span>
    <a class="btn primary" href="#create"><svg class="i" aria-hidden="true"><use href="#i-plus"/></svg>New posts</a>
   </div>
   <div id="alerts" role="status"></div>
   <div class="stats" id="stats" aria-label="Summary">
    <div class="stat skel" style="height:96px"></div><div class="stat skel" style="height:96px"></div>
    <div class="stat skel" style="height:96px"></div><div class="stat skel" style="height:96px"></div>
   </div>
   <div class="toolbar" id="filters" aria-label="Filters"></div>
   <div class="cal"><div class="week" id="week" aria-label="Week"></div></div>
  </section>

  <!-- ===== Queue list ===== -->
  <section class="view" id="v-list" aria-labelledby="h-list">
   <div class="page-head"><div><h1 id="h-list">Queue</h1><p>Every post, newest first. Updates live.</p></div></div>
   <div class="panel" id="queue">{{ queue_html|safe }}</div>
  </section>

  <!-- ===== Create ===== -->
  <section class="view" id="v-create" aria-labelledby="h-create">
   <div class="page-head"><div><h1 id="h-create">Create</h1><p>Schedule a whole folder, a single post, or re-plan the queue.</p></div></div>
   <div class="grid">
    <form class="panel" method="post" action="{{ url_for('bulk') }}">
     <h2>Bulk schedule a folder</h2><p class="lead">Videos already posted or queued are skipped automatically.</p>
     <div class="field"><label for="f-folder">Folder with videos or photos</label>
      <input id="f-folder" name="folder" required placeholder="C:\Users\you\Videos\ready"></div>
     <fieldset><legend>Pages</legend><div class="checks">
      {% for p in pages %}<label class="check"><input type="checkbox" name="page_ids" value="{{ p.id }}"><span><i class="dot" data-page="{{ p.id }}"></i>{{ p.name }}</span></label>{% else %}<small>Connect a Page first.</small>{% endfor %}
     </div></fieldset>
     <div class="field"><label for="f-mode">Distribution</label>
      <select id="f-mode" name="mode"><option value="split">Split files across Pages (each file posted once)</option>
       <option value="all">Every Page gets every file</option></select></div>
     <div class="row2">
      <div class="field"><label for="f-start">Start</label><input id="f-start" type="datetime-local" name="start" value="{{ now }}"></div>
      <div class="field"><label for="f-int">Minutes between posts</label><input id="f-int" type="number" name="interval" value="240" min="1" inputmode="numeric">
       <div class="help">Per Page. 240 = 6 posts a day.</div></div>
     </div>
     <div class="field"><label for="f-cap">Default caption</label><textarea id="f-cap" name="caption" placeholder="#cat #cats #catlover"></textarea></div>
     <div class="field"><label for="f-csv">Captions CSV <span style="font-weight:400;color:var(--muted)">(optional)</span></label>
      <input id="f-csv" name="captions_csv" placeholder="C:\Users\you\Videos\captions.csv">
      <div class="help">Columns: filename,caption. A .txt file with the same name as a video overrides both.</div></div>
     <div class="actions"><button class="btn primary" type="submit">Queue folder</button></div>
    </form>

    <form class="panel" method="post" action="{{ url_for('single_post') }}">
     <h2>Single post</h2><p class="lead">Leave the file empty for a text or link post.</p>
     <fieldset><legend>Pages</legend><div class="checks">
      {% for p in pages %}<label class="check"><input type="checkbox" name="page_ids" value="{{ p.id }}"><span><i class="dot" data-page="{{ p.id }}"></i>{{ p.name }}</span></label>{% else %}<small>Connect a Page first.</small>{% endfor %}
     </div></fieldset>
     <div class="field"><label for="s-path">Video or photo file</label><input id="s-path" name="path" placeholder="C:\Users\you\Videos\clip.mp4"></div>
     <div class="field"><label for="s-cap">Caption / message</label><textarea id="s-cap" name="caption"></textarea></div>
     <div class="field"><label for="s-link">Link <span style="font-weight:400;color:var(--muted)">(text posts only)</span></label><input id="s-link" name="link" type="url" placeholder="https://"></div>
     <div class="field"><label for="s-when">When</label><input id="s-when" type="datetime-local" name="run_at" value="{{ now }}"></div>
     <div class="actions"><button class="btn primary" type="submit">Queue post</button></div>
    </form>

    <div class="panel">
     <h2>Queue tools</h2><p class="lead">Re-plan everything that hasn't gone out yet.</p>
     <div class="field"><label for="t-page">Page</label>
      <select id="t-page"><option value="">All Pages</option>{% for p in pages %}<option value="{{ p.id }}">{{ p.name }}</option>{% endfor %}</select></div>
     <div class="row2">
      <div class="field"><label for="t-start">New start</label><input type="datetime-local" id="t-start" value="{{ now }}"></div>
      <div class="field"><label for="t-gap">Minutes between posts</label><input type="number" id="t-gap" value="240" min="1" inputmode="numeric"></div>
     </div>
     <div class="help">Keeps the current order and moves every queued post onto the new start and gap.</div>
     <div class="actions"><button class="btn primary" type="button" id="t-resched"><svg class="i i-sm" aria-hidden="true"><use href="#i-refresh"/></svg>Re-space queue</button></div>
     <div class="divider"></div>
     <h2 style="font-size:14px">Danger zone</h2><p class="lead">Published posts and history stay, so duplicates are still blocked.</p>
     <button class="btn danger" type="button" id="t-clear"><svg class="i i-sm" aria-hidden="true"><use href="#i-trash"/></svg>Delete all queued posts</button>
    </div>
   </div>
  </section>

  <!-- ===== Pages ===== -->
  <section class="view" id="v-pages" aria-labelledby="h-pages">
   <div class="page-head"><div><h1 id="h-pages">Pages</h1><p>Connected Facebook Pages and their tokens.</p></div></div>
   <div class="grid">
    <div class="panel">
     <h2>Connected Pages</h2><p class="lead">Followers refresh every 10 minutes.</p>
     <div class="plist">
      {% for p in pages %}
      <div class="prow"><span class="avatar" data-page="{{ p.id }}">{{ p.name[:1] }}</span>
       <div class="info"><div class="nm">{{ p.name }} <span class="status st-failed" data-paused="{{ p.id }}" hidden>Paused · token expired</span></div><div class="sub num"><span data-followers="{{ p.id }}">—</span> · ID {{ p.id }}</div></div>
       <form class="inline" method="post" action="{{ url_for('pages_delete', page_id=p.id) }}"
        onsubmit="return confirm('Remove {{ p.name }}? Its queued posts are deleted too.')"><button class="btn danger" type="submit">Remove</button></form></div>
      {% else %}<div class="empty">No Pages yet. Import them with a token.</div>{% endfor %}
     </div>
    </div>
    <div class="panel">
     <h2>Import or refresh tokens</h2><p class="lead">Use an extended User Access Token. Every Page you manage is added or updated.</p>
     <form method="post" action="{{ url_for('pages_import') }}">
      <div class="field"><label for="p-tok">User Access Token</label><input id="p-tok" name="user_token" required placeholder="EAAB…" autocomplete="off"></div>
      <div class="actions"><button class="btn primary" type="submit">Import Pages</button></div>
     </form>
     <details style="margin-top:16px"><summary style="cursor:pointer;color:var(--muted);font-size:14px">Add one Page manually</summary>
      <form method="post" action="{{ url_for('pages_add') }}">
       <div class="row2"><div class="field"><label for="m-id">Page ID</label><input id="m-id" name="page_id" required></div>
        <div class="field"><label for="m-nm">Name</label><input id="m-nm" name="name"></div></div>
       <div class="field"><label for="m-tok">Page Access Token</label><input id="m-tok" name="token" required autocomplete="off"></div>
       <div class="actions"><button class="btn" type="submit">Save Page</button></div>
      </form></details>
    </div>
   </div>
  </section>
 </main>
</div>

<div class="scrim" id="scrim"></div>
<aside class="drawer" id="drawer" role="dialog" aria-modal="true" aria-labelledby="dw-title" aria-hidden="true">
 <div class="dw-h"><h2 id="dw-title">Post</h2><span class="grow"></span>
  <button class="btn icon ghost" id="dw-close" type="button" aria-label="Close"><svg class="i" aria-hidden="true"><use href="#i-x"/></svg></button></div>
 <div class="dw-b" id="dw-body"></div>
 <div class="dw-f" id="dw-foot"></div>
</aside>

<div class="toasts" id="toasts" aria-live="polite">
 {% for cat, msg in get_flashed_messages(with_categories=true) %}
  <div class="toast {{ 'err' if cat=='err' else '' }}">{{ msg }}</div>{% endfor %}
</div>

<script>
(function () {
  const $ = s => document.querySelector(s), $$ = s => document.querySelectorAll(s);
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
  const icon = (id, cls) => { const s = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    s.setAttribute("class", "i " + (cls || "")); s.setAttribute("aria-hidden", "true");
    s.innerHTML = `<use href="#i-${id}"/>`; return s; };
  const time12 = t => new Date(t * 1000).toLocaleTimeString("en-US", {hour: "numeric", minute: "2-digit"});
  const full12 = t => new Date(t * 1000).toLocaleString("en-US", {weekday: "short", month: "short", day: "numeric", hour: "numeric", minute: "2-digit"});
  const sod = d => { const x = new Date(d); x.setHours(0, 0, 0, 0); return x; };
  const localInput = t => { const d = new Date(t * 1000); d.setMinutes(d.getMinutes() - d.getTimezoneOffset()); return d.toISOString().slice(0, 16); };
  const fmtN = n => Number(n || 0).toLocaleString("en-US");
  function until(t) {
    const m = Math.round((t * 1000 - Date.now()) / 60000);
    if (m <= 0) return "any moment now";
    if (m < 60) return `in ${m} min`;
    const hrs = Math.floor(m / 60), mm = m % 60;
    return hrs < 24 ? `in ${hrs} h ${mm} min` : `in ${Math.round(hrs / 24)} days`;
  }
  const store = {get(k) { try { return localStorage.getItem(k); } catch (e) { return null; } },
                 set(k, v) { try { localStorage.setItem(k, v); } catch (e) { /* private mode */ } }};
  const STATUS = {queued: ["Scheduled", "clock"], posting: ["Posting", "send"], done: ["Published", "check"],
                  failed: ["Failed", "alert"], skipped: ["Skipped", "x"]};
  const statusChip = s => h("span", {class: `status st-${s}`}, icon(STATUS[s] ? STATUS[s][1] : "clock"), STATUS[s] ? STATUS[s][0] : s);

  /* ---------- toasts ---------- */
  function toast(msg, err) {
    const t = h("div", {class: "toast" + (err ? " err" : "")}, icon(err ? "alert" : "check"), h("div", {}, msg));
    $("#toasts").append(t); setTimeout(() => t.remove(), 4500);
  }
  $$("#toasts .toast").forEach(t => { t.prepend(icon(t.classList.contains("err") ? "alert" : "check")); setTimeout(() => t.remove(), 6000); });

  /* ---------- theme ---------- */
  const THEMES = [["system", "auto", "System theme"], ["light", "sun", "Light theme"], ["dark", "moon", "Dark theme"]];
  function applyTheme(name) {
    const t = THEMES.find(x => x[0] === name) || THEMES[0];
    if (t[0] === "system") document.documentElement.removeAttribute("data-theme"); else document.documentElement.dataset.theme = t[0];
    $("#theme svg use").setAttribute("href", `#i-${t[1]}`); $("#theme-t").textContent = t[2];
    $("#theme").setAttribute("aria-label", `${t[2]}. Click to change`); store.set("theme", t[0]);
  }
  $("#theme").onclick = () => { const i = THEMES.findIndex(x => x[0] === (store.get("theme") || "system")); applyTheme(THEMES[(i + 1) % 3][0]); };
  applyTheme(store.get("theme") || "system");

  /* ---------- routing (deep links: #calendar #list #create #pages) ---------- */
  const VIEWS = ["calendar", "list", "create", "pages"];
  function route() {
    const v = VIEWS.includes(location.hash.slice(1)) ? location.hash.slice(1) : (store.get("view") || "calendar");
    $$(".view").forEach(s => s.classList.toggle("on", s.id === "v-" + v));
    $$(".nav[data-view]").forEach(n => { if (n.dataset.view === v) n.setAttribute("aria-current", "page"); else n.removeAttribute("aria-current"); });
    store.set("view", v);
  }
  window.addEventListener("hashchange", () => { route(); $("#main").focus({preventScroll: true}); });
  route();

  /* ---------- state ---------- */
  let weekStart = sod(new Date()), state = null, insights = null, openId = null, lastFocus = null;
  let pageFilter = store.get("pageFilter") || "", statusFilter = store.get("statusFilter") || "";
  const pageOf = id => (state && state.pages.find(p => p.id === id)) || {};
  const colorOf = id => pageOf(id).color || "var(--border-strong)";

  async function load() {
    try {
      const r = await fetch(`/api/state?start=${weekStart.getTime() / 1000}&days=7`, {cache: "no-store"});
      if (!r.ok) throw new Error(r.status);
      state = await r.json();
      $("#conn").classList.remove("off"); $("#conn-t").textContent = `Live · ${new Date().toLocaleTimeString("en-US", {hour: "numeric", minute: "2-digit"})}`;
      $("#nav-queued").textContent = state.counts.queued || "";
      renderAlerts(); renderStats(); renderFilters(); renderWeek(); paintPages();
      if (openId) refreshDrawer();
    } catch (e) {
      $("#conn").classList.add("off"); $("#conn-t").textContent = "Offline · retrying";
    }
  }
  async function loadInsights(force) {
    try {
      const r = await fetch("/api/insights" + (force ? "?refresh=1" : ""), {cache: "no-store"});
      if (r.ok) { insights = await r.json(); renderStats(); paintPages(); }
    } catch (e) { /* offline */ }
  }

  /* ---------- stats ---------- */
  function ring(done, total) {
    const pct = total ? done / total : 0, r = 24, c = 2 * Math.PI * r;
    const s = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    s.setAttribute("viewBox", "0 0 56 56"); s.setAttribute("class", "ring"); s.setAttribute("role", "img");
    s.setAttribute("aria-label", `${Math.round(pct * 100)} percent of today's posts published`);
    s.innerHTML = `<circle cx="28" cy="28" r="${r}" fill="none" stroke="var(--surface-3)" stroke-width="6"/>
      <circle cx="28" cy="28" r="${r}" fill="none" stroke="var(--ok)" stroke-width="6" stroke-linecap="round"
       stroke-dasharray="${c * pct} ${c}" transform="rotate(-90 28 28)"/>
      <text x="28" y="32" text-anchor="middle" font-size="12" font-weight="700" fill="currentColor">${Math.round(pct * 100)}%</text>`;
    return s;
  }
  function stat(lead, k, v, sub, title) {
    return h("div", {class: "stat", title: title || null}, lead,
      h("div", {style: "min-width:0"}, h("div", {class: "k"}, k), h("div", {class: "v num"}, v), h("div", {class: "s"}, sub || "")));
  }
  const tile = (id, tone) => h("span", {class: `ico ${tone}`}, icon(id));
  function renderStats() {
    if (!state) return;
    const c = state.counts, n = state.next, total = state.posted_today + state.left_today;
    const ok = insights && insights.pages.length > 0, why = insights && insights.errors.length ? insights.errors.join("\n") : null;
    $("#stats").replaceChildren(
      stat(ring(state.posted_today, total), "Today", `${state.posted_today} / ${total}`, "posts published"),
      stat(n && n.path ? h("img", {class: "th", src: `/thumb/${n.id}`, alt: "", onerror: e => e.target.replaceWith(tile("clock", "t-blue"))}) : tile("clock", "t-blue"),
        "Next post", n ? time12(n.run_at) : "—", n ? `${until(n.run_at)} · ${n.page_name || ""}` : "Queue is empty"),
      stat(tile("layers", "t-amber"), "Scheduled", fmtN(c.queued), state.queue_ends ? `Runs until ${state.queue_ends}` : "Nothing waiting"),
      stat(tile(c.failed ? "alert" : "check", c.failed ? "t-rose" : "t-green"), "Published", fmtN(c.done),
        c.failed ? `${c.failed} failed · open Queue to retry` : "No failures"),
      stat(tile("users", "t-blue"), "Followers", ok ? fmtN(insights.followers) : "—",
        !insights ? "Loading insights…" : ok ? `Across ${insights.pages.length} Page${insights.pages.length > 1 ? "s" : ""}` : "Insights unavailable · hover for why", why),
      stat(tile("heart", "t-rose"), "Engagement", ok ? fmtN(insights.reactions + insights.comments) : "—",
        ok ? `${fmtN(insights.reactions)} reactions · ${fmtN(insights.comments)} comments` : "Recent 20 posts", why));
  }

  /* ---------- alerts ---------- */
  async function retryFailed(btn) {
    btn.disabled = true;
    const d = await api("/api/queue/retry_failed", {}, r => `Re-queued ${r.retried} failed post${r.retried === 1 ? "" : "s"}.`);
    btn.disabled = false; if (d) load();
  }
  function renderAlerts() {
    const out = state.pages.filter(p => p.token_error).map(p => h("div", {class: "alert"}, icon("alert"),
      h("div", {class: "msg"}, h("b", {}, `${p.name} is paused: its Facebook token stopped working`),
        h("small", {}, `Posts stay scheduled. Import a new extended token to resume. Facebook said: ${p.token_error}`)),
      h("a", {class: "btn", href: "#pages"}, "Re-import token")));
    const failed = state.counts.failed || 0;
    if (failed) out.push(h("div", {class: "alert warn"}, icon("alert"),
      h("div", {class: "msg"}, h("b", {}, `${failed} post${failed === 1 ? "" : "s"} failed`),
        h("small", {}, "Fix the cause first (for an expired token, re-import it), then retry. They go out from now with the same gaps.")),
      h("button", {class: "btn", type: "button", onclick: e => retryFailed(e.currentTarget)}, icon("refresh", "i-sm"), "Retry all failed")));
    $("#alerts").replaceChildren(...out);
    $$("[data-paused]").forEach(el => { el.hidden = !pageOf(el.dataset.paused).token_error; });
  }

  /* ---------- filters ---------- */
  function chip(label, pressed, onclick, dotColor) {
    return h("button", {class: "chip", type: "button", "aria-pressed": String(pressed), onclick},
      dotColor ? h("i", {class: "dot", style: `background:${dotColor}`}) : null, label);
  }
  function renderFilters() {
    const set = (k, v) => { if (k === "p") { pageFilter = v; store.set("pageFilter", v); } else { statusFilter = v; store.set("statusFilter", v); } renderFilters(); renderWeek(); };
    $("#filters").replaceChildren(
      chip("All Pages", !pageFilter, () => set("p", "")),
      ...state.pages.map(p => chip(`${p.name} · ${p.queued}`, pageFilter === p.id, () => set("p", pageFilter === p.id ? "" : p.id), p.color)),
      h("span", {class: "sep", "aria-hidden": "true"}),
      ...[["", "Any status"], ["queued", "Scheduled"], ["done", "Published"], ["failed", "Failed"]].map(([v, l]) =>
        chip(l, statusFilter === v, () => set("s", v))));
  }

  /* ---------- calendar ---------- */
  const MAX_PER_DAY = 40;
  function renderWeek() {
    const end = new Date(weekStart.getTime() + 6 * DAY);
    const f = d => d.toLocaleDateString("en-US", {month: "short", day: "numeric"});
    $("#range").textContent = `${f(weekStart)} – ${f(end)}, ${end.getFullYear()}`;
    const today = sod(new Date()).getTime();
    const shown = state.posts.filter(p => (!pageFilter || p.page_id === pageFilter) && (!statusFilter || p.status === statusFilter));
    $("#range-sub").textContent = `${shown.length} post${shown.length === 1 ? "" : "s"} this week`;
    const cols = [];
    for (let i = 0; i < 7; i++) {
      const d = new Date(weekStart.getTime() + i * DAY), key = sod(d).getTime();
      const posts = shown.filter(p => sod(new Date(p.run_at * 1000)).getTime() === key);
      const label = d.toLocaleDateString("en-US", {weekday: "long", month: "long", day: "numeric"});
      cols.push(h("div", {class: "day" + (key === today ? " today" : key < today ? " past" : ""), role: "group", "aria-label": `${label}, ${posts.length} posts`},
        h("div", {class: "dh"}, h("span", {class: "dd num"}, d.getDate()),
          h("div", {}, h("div", {class: "dn"}, d.toLocaleDateString("en-US", {weekday: "short"})), key === today ? h("div", {style: "font-size:12px;color:var(--muted)"}, "Today") : null),
          h("span", {class: "cnt num"}, posts.length ? `${posts.length}` : "")),
        posts.length ? h("div", {class: "cards"}, posts.slice(0, MAX_PER_DAY).map(card),
            posts.length > MAX_PER_DAY ? h("div", {class: "more"}, `+${posts.length - MAX_PER_DAY} more · see Queue`) : null)
          : h("div", {class: "empty-day"}, key < today ? "Nothing posted" : "Nothing scheduled")));
    }
    $("#week").replaceChildren(...cols);
  }
  function card(p) {
    const media = p.path
      ? h("div", {class: "media", style: `background-image:url('/thumb/${p.id}')`},
          p.kind === "video" ? h("span", {class: "play"}, (() => { const s = document.createElementNS("http://www.w3.org/2000/svg", "svg"); s.setAttribute("viewBox", "0 0 24 24"); s.innerHTML = '<path d="M8 5v14l11-7z"/>'; return s; })()) : null,
          p.status !== "queued" ? statusChip(p.status) : null)
      : h("div", {class: "media text"}, icon("text"));
    return h("button", {class: "card", type: "button", style: `--pc:${colorOf(p.page_id)}`, "aria-current": p.id === openId ? "true" : null,
        "aria-label": `${STATUS[p.status] ? STATUS[p.status][0] : p.status} ${p.kind} post for ${p.page_name || "Page"} at ${time12(p.run_at)}. ${p.caption || p.file || ""}`,
        onclick: e => openDrawer(p.id, e.currentTarget)},
      media, h("div", {class: "body"},
        h("div", {class: "r1"}, h("span", {class: "time num"}, time12(p.run_at)), !p.path && p.status !== "queued" ? statusChip(p.status) : null),
        h("div", {class: "pgline"}, h("i", {class: "dot", style: `background:${colorOf(p.page_id)}`}), h("span", {class: "pg"}, p.page_name || p.page_id)),
        h("div", {class: "cap"}, p.caption || p.file || "No caption")));
  }
  function paintPages() {
    $$("[data-page]").forEach(el => { el.style.background = colorOf(el.dataset.page); });
    if (insights) insights.pages.forEach(p => { const el = document.querySelector(`[data-followers="${p.id}"]`); if (el) el.textContent = `${fmtN(p.followers)} followers`; });
  }
  $("#prev").onclick = () => { weekStart = new Date(weekStart.getTime() - 7 * DAY); load(); };
  $("#next").onclick = () => { weekStart = new Date(weekStart.getTime() + 7 * DAY); load(); };
  $("#today").onclick = () => { weekStart = sod(new Date()); load(); };

  /* ---------- drawer ---------- */
  async function api(url, body, okMsg) {
    try {
      const r = await fetch(url, {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(body || {})});
      const data = await r.json().catch(() => ({}));
      if (!r.ok) { toast(data.error || "That didn't work. Try again.", true); return null; }
      if (okMsg) toast(typeof okMsg === "function" ? okMsg(data) : okMsg);
      return data;
    } catch (e) { toast("The app isn't responding. Is it still running?", true); return null; }
  }
  async function openDrawer(id, from) {
    lastFocus = from || document.activeElement; openId = id; drawn = null;
    await refreshDrawer();
    $("#drawer").classList.add("open"); $("#scrim").classList.add("open"); $("#drawer").setAttribute("aria-hidden", "false");
    $("#dw-close").focus(); renderWeek();
  }
  function closeDrawer() {
    if (!openId) return;
    openId = null; drawn = null;
    $("#drawer").classList.remove("open"); $("#scrim").classList.remove("open"); $("#drawer").setAttribute("aria-hidden", "true");
    const v = $("#dw-body video"); if (v) v.pause();
    if (state) renderWeek();
    if (lastFocus && document.body.contains(lastFocus)) lastFocus.focus();
  }
  $("#dw-close").onclick = closeDrawer; $("#scrim").onclick = closeDrawer;
  document.addEventListener("keydown", e => {
    if (e.key === "Escape") closeDrawer();
    if (e.key === "Tab" && openId) {  // keep focus inside the open drawer
      const f = [...$("#drawer").querySelectorAll("button,a[href],input,textarea,video[controls]")].filter(x => !x.disabled);
      if (!f.length) return;
      if (e.shiftKey && document.activeElement === f[0]) { e.preventDefault(); f[f.length - 1].focus(); }
      else if (!e.shiftKey && document.activeElement === f[f.length - 1]) { e.preventDefault(); f[0].focus(); }
    }
  });

  let drawn = null;
  async function refreshDrawer() {
    let p;
    try { const r = await fetch(`/api/posts/${openId}`, {cache: "no-store"}); if (!r.ok) { closeDrawer(); return; } p = await r.json(); } catch (e) { return; }
    const sig = `${p.id}:${p.status}`;
    if (drawn === sig) return;  // keep the video playing and typed edits intact
    drawn = sig;
    const editable = p.status === "queued" || p.status === "failed";
    const eng = insights && insights.posts[p.id];
    $("#dw-title").textContent = p.kind === "video" ? "Video post" : p.kind === "photo" ? "Photo post" : "Text post";
    const preview = p.kind === "video" ? h("video", {class: "preview", src: `/media/${p.id}`, poster: `/thumb/${p.id}`, controls: true, preload: "none"})
      : p.kind === "photo" ? h("img", {class: "preview", src: `/media/${p.id}`, alt: p.caption || "Photo"}) : null;
    const cap = h("textarea", {id: "dw-cap", disabled: !editable, rows: "4"}); cap.value = p.caption || "";
    $("#dw-body").replaceChildren(...[
      preview,
      h("div", {class: "who"}, h("span", {class: "avatar", style: `background:${colorOf(p.page_id)}`}, (p.page_name || "?").slice(0, 1)),
        h("div", {style: "flex:1;min-width:0"}, h("div", {class: "nm"}, p.page_name || p.page_id), h("div", {class: "sub"}, full12(p.run_at))), statusChip(p.status)),
      h("div", {class: "field"}, h("label", {for: "dw-cap"}, "Caption"), cap,
        editable ? null : h("div", {class: "help"}, "Published posts can't be edited here.")),
      h("div", {class: "field"}, h("label", {for: "dw-when"}, "Scheduled time"),
        h("input", {type: "datetime-local", id: "dw-when", value: localInput(p.run_at), disabled: !editable})),
      h("dl", {class: "meta"},
        h("dt", {}, "File"), h("dd", {}, p.file || "—"),
        h("dt", {}, "Published"), h("dd", {}, p.posted || "—"),
        eng ? [h("dt", {}, "Engagement"), h("dd", {class: "num"}, `${fmtN(eng.reactions)} reactions · ${fmtN(eng.comments)} comments`)] : null,
        p.result ? [h("dt", {}, p.status === "done" ? "Facebook ID" : "Details"), h("dd", {style: p.status === "failed" ? "color:var(--err)" : ""}, p.result)] : null),
      p.fb_url ? h("a", {class: "btn", href: p.fb_url, target: "_blank", rel: "noopener"}, icon("ext", "i-sm"), "View on Facebook") : null
    ].filter(Boolean));
    const foot = [];
    if (editable) {
      foot.push(h("button", {class: "btn primary", type: "button", onclick: async e => {
        e.currentTarget.disabled = true;
        if (await api(`/api/posts/${p.id}/now`, {}, "Sending within 15 seconds.")) { drawn = null; load(); } else e.currentTarget.disabled = false;
      }}, icon("send", "i-sm"), p.status === "failed" ? "Retry now" : "Post now"));
      foot.push(h("button", {class: "btn", type: "button", onclick: async () => {
        if (await api(`/api/posts/${p.id}`, {caption: $("#dw-cap").value, run_at: $("#dw-when").value}, "Changes saved.")) { drawn = null; load(); }
      }}, "Save changes"));
    }
    if (p.status !== "posting") foot.push(h("button", {class: "btn danger", type: "button", onclick: async () => {
      const msg = p.status === "done" ? "Remove this post from history?\n\nIt stays on Facebook. The same video could then be queued again."
                                      : "Delete this post from the queue?";
      if (confirm(msg) && await api(`/api/posts/${p.id}/delete`, {}, "Post deleted.")) { closeDrawer(); load(); }
    }}, icon("trash", "i-sm"), "Delete"));
    $("#dw-foot").replaceChildren(...foot);
  }

  /* ---------- queue tools ---------- */
  $("#t-resched").onclick = async e => {
    const btn = e.currentTarget, body = {page_id: $("#t-page").value, start: $("#t-start").value, gap: $("#t-gap").value};
    btn.disabled = true;
    const r = await api("/api/queue/reschedule", body, d => `Re-spaced ${d.moved} post${d.moved === 1 ? "" : "s"}.`);
    btn.disabled = false;
    if (r) { weekStart = sod(new Date(body.start || Date.now())); location.hash = "#calendar"; load(); }
  };
  $("#t-clear").onclick = async () => {
    const page = $("#t-page"), name = page.value ? page.selectedOptions[0].text : "all Pages";
    if (!confirm(`Delete every queued post for ${name}?\n\nThis can't be undone. Published posts are kept.`)) return;
    if (await api("/api/queue/clear", {page_id: page.value}, d => `Deleted ${d.deleted} queued post${d.deleted === 1 ? "" : "s"}.`)) load();
  };

  /* ---------- live updates ---------- */
  // Live queue list: re-fetch the queue table every 3 s while that view is open.
  const box = $("#queue");
  setInterval(async () => {
    if (document.hidden || !$("#v-list").classList.contains("on")) return;
    try { const r = await fetch("/queue" + location.search, {cache: "no-store"}); if (r.ok) { const t = await r.text(); if (t !== box.innerHTML) box.innerHTML = t; } } catch (e) { /* restarting */ }
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
