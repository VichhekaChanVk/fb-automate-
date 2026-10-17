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
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path

import requests
from flask import Flask, flash, redirect, render_template_string, request, url_for

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
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M") if ts else ""


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


TEMPLATE = """<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Page Auto-Poster</title>
<style>
:root{--bg:#f5f6f8;--card:#fff;--text:#1c1e21;--muted:#65676b;--line:#dddfe2;--accent:#1877f2;
 --ok:#1b7f3b;--err:#c62828;--warn:#b26a00}
@media (prefers-color-scheme:dark){:root{--bg:#18191a;--card:#242526;--text:#e4e6eb;--muted:#b0b3b8;
 --line:#3a3b3c;--accent:#4e9bff;--ok:#5bd17f;--err:#ff6b6b;--warn:#ffb74d}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);
 font:14px/1.45 system-ui,Segoe UI,sans-serif}
main{max-width:1200px;margin:0 auto;padding:16px}h1{font-size:20px;margin:4px 0 16px}
h2{font-size:15px;margin:0 0 10px}.grid{display:grid;gap:16px;grid-template-columns:repeat(auto-fit,minmax(320px,1fr))}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px;margin-bottom:16px}
label{display:block;font-size:12px;color:var(--muted);margin:8px 0 3px}
input,textarea,select{width:100%;padding:7px 9px;border:1px solid var(--line);border-radius:6px;
 background:var(--bg);color:var(--text);font:inherit}textarea{min-height:60px}
button{background:var(--accent);color:#fff;border:0;border-radius:6px;padding:7px 14px;font:inherit;
 cursor:pointer;margin-top:10px}button.ghost{background:none;color:var(--accent);padding:2px 6px;margin:0}
.pages label.chk{display:flex;gap:6px;align-items:center;color:var(--text);font-size:14px;margin:3px 0}
.pages input{width:auto}.row{display:flex;gap:8px}.row>*{flex:1}
table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:6px;border-bottom:1px solid var(--line);
 vertical-align:top}th{font-size:12px;color:var(--muted)}td.res{max-width:320px;word-break:break-word;color:var(--muted)}
.s-done{color:var(--ok)}.s-failed{color:var(--err)}.s-posting{color:var(--warn)}.s-skipped{color:var(--muted)}
.flash{padding:8px 12px;border-radius:6px;margin-bottom:10px;border:1px solid var(--line)}
.flash.ok{color:var(--ok)}.flash.err{color:var(--err)}.tabs a{margin-right:12px;color:var(--accent)}
.tabs a.on{font-weight:600;text-decoration:none;color:var(--text)}.wrap{overflow-x:auto}
small{color:var(--muted)}form.inline{display:inline}
.live{color:var(--ok);font-weight:400}.live::before{content:"\\25CF ";animation:blink 2s infinite}
@keyframes blink{50%{opacity:.3}}
</style></head><body><main>
<h1>Page Auto-Poster</h1>
{% for cat, msg in get_flashed_messages(with_categories=true) %}<div class="flash {{cat}}">{{msg}}</div>{% endfor %}

<div class="grid">
 <div class="card">
  <h2>Your Pages ({{pages|length}})</h2>
  {% for p in pages %}
   <div>{{p.name}} <small>{{p.id}}</small>
    <form class="inline" method="post" action="{{url_for('pages_delete', page_id=p.id)}}"
     onsubmit="return confirm('Remove {{p.name}}?')"><button class="ghost">remove</button></form></div>
  {% else %}<small>No Pages yet. Import them below.</small>{% endfor %}
  <form method="post" action="{{url_for('pages_import')}}">
   <label>Import my Pages (paste User Access Token)</label>
   <input name="user_token" required placeholder="EAAB...">
   <button>Import</button>
  </form>
  <details><summary><small>Add one Page manually</small></summary>
   <form method="post" action="{{url_for('pages_add')}}">
    <div class="row"><div><label>Page ID</label><input name="page_id" required></div>
    <div><label>Name</label><input name="name"></div></div>
    <label>Page Access Token</label><input name="token" required>
    <button>Save</button>
   </form></details>
 </div>

 <div class="card">
  <h2>Bulk schedule a folder</h2>
  <form method="post" action="{{url_for('bulk')}}">
   <label>Folder with videos/photos</label><input name="folder" required placeholder="D:\\Videos\\ready">
   <label>Pages</label><div class="pages">
    {% for p in pages %}<label class="chk"><input type="checkbox" name="page_ids" value="{{p.id}}">{{p.name}}</label>{% endfor %}</div>
   <label>Distribution</label>
   <select name="mode"><option value="split">Split files across Pages (each file posted once)</option>
    <option value="all">Every Page gets every file</option></select>
   <div class="row"><div><label>Start</label><input type="datetime-local" name="start" value="{{now}}"></div>
    <div><label>Minutes between posts (per Page)</label><input type="number" name="interval" value="240" min="1"></div></div>
   <label>Default caption</label><textarea name="caption"></textarea>
   <label>Captions CSV (optional: filename,caption)</label><input name="captions_csv" placeholder="D:\\Videos\\captions.csv">
   <small>A .txt file with the same name as a video overrides both.</small><br>
   <button>Queue folder</button>
  </form>
 </div>

 <div class="card">
  <h2>Single post</h2>
  <form method="post" action="{{url_for('single_post')}}">
   <label>Pages</label><div class="pages">
    {% for p in pages %}<label class="chk"><input type="checkbox" name="page_ids" value="{{p.id}}">{{p.name}}</label>{% endfor %}</div>
   <label>Video/photo file (empty = text post)</label><input name="path" placeholder="D:\\Videos\\clip.mp4">
   <label>Caption / message</label><textarea name="caption"></textarea>
   <label>Link (text posts only)</label><input name="link">
   <label>When</label><input type="datetime-local" name="run_at" value="{{now}}">
   <button>Queue</button>
  </form>
 </div>
</div>

<div class="card" id="queue">{{ queue_html|safe }}</div>
</main>
<script>
// Live queue: re-fetch the queue every 3 s while this tab is visible.
(function () {
  const box = document.getElementById("queue");
  async function tick() {
    if (document.hidden) return;
    try {
      const r = await fetch("/queue" + location.search, {cache: "no-store"});
      if (r.ok) {
        const html = await r.text();
        if (html !== box.innerHTML) box.innerHTML = html;
      }
    } catch (e) { /* app restarting: try again next tick */ }
  }
  setInterval(tick, 3000);
  document.addEventListener("visibilitychange", tick);
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
