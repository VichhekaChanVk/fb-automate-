import re
import time


def flash_text(resp):
    return " | ".join(re.findall(r'class="flash \w+">([^<]*)', resp.get_data(as_text=True)))


def rows(app_mod, where="1=1"):
    with app_mod.db() as c:
        return [dict(r) for r in c.execute(f"SELECT * FROM posts WHERE {where} ORDER BY id")]


def bulk(client, folder, pages, mode="split"):
    data = {"folder": str(folder), "mode": mode, "start": "", "interval": "60", "caption": "cap",
            "page_ids": pages}
    return client.post("/bulk", data=data, follow_redirects=True)


# ---------------------------------------------------------------- pages / UI

def test_dashboard_and_live_queue_render(client):
    home = client.get("/").get_data(as_text=True)
    assert 'id="queue"' in home and "setInterval" in home
    frag = client.get("/queue").get_data(as_text=True)
    assert "<html" not in frag and "Queue" in frag


# ---------------------------------------------------------------- duplicates

def test_fingerprint_matches_renamed_copy(app_mod, videos):
    assert app_mod.fingerprint(videos / "v1.mp4") == app_mod.fingerprint(videos / "v1_copy.mp4")
    assert app_mod.fingerprint(videos / "v1.mp4") != app_mod.fingerprint(videos / "v2.mp4")


def test_bulk_skips_duplicates_in_folder_and_on_requeue(client, app_mod, videos):
    bulk(client, videos, ["A"])
    assert len(rows(app_mod)) == 2  # renamed copy skipped
    resp = bulk(client, videos, ["A"])
    assert "Queued 0 post(s)" in flash_text(resp)
    assert len(rows(app_mod)) == 2


def test_bulk_all_mode_is_per_page(client, app_mod, videos):
    bulk(client, videos, ["A"])
    bulk(client, videos, ["A", "B"], mode="all")
    assert len(rows(app_mod, "page_id='A'")) == 2
    assert len(rows(app_mod, "page_id='B'")) == 2


def test_bulk_skips_leave_no_schedule_gaps(client, app_mod, videos):
    bulk(client, videos, ["A"])
    times = [r["run_at"] for r in rows(app_mod)]
    assert round(times[1] - times[0]) == 3600


def test_worker_skips_video_already_done(app_mod, videos, monkeypatch):
    fp = app_mod.fingerprint(videos / "v1.mp4")
    with app_mod.db() as c:
        c.execute("INSERT INTO posts(page_id,kind,path,run_at,status,file_hash) VALUES ('A','video',?,0,'done',?)",
                  (str(videos / "v1.mp4"), fp))
        c.execute("INSERT INTO posts(page_id,kind,path,run_at,file_hash) VALUES ('A','video',?,0,?)",
                  (str(videos / "v1_copy.mp4"), fp))

    def no_upload(*_):
        raise AssertionError("duplicate must not be uploaded")

    monkeypatch.setattr(app_mod, "publish", no_upload)
    monkeypatch.setattr(app_mod, "WORKER_TICK", 0.01)
    monkeypatch.setattr(app_mod.time, "sleep", lambda _: (_ for _ in ()).throw(KeyboardInterrupt))
    try:
        app_mod.worker()
    except KeyboardInterrupt:
        pass
    dup = rows(app_mod, "id=2")[0]
    assert dup["status"] == "skipped" and "#1" in dup["result"]


def test_retry_only_failed(client, app_mod):
    with app_mod.db() as c:
        c.execute("INSERT INTO posts(page_id,kind,caption,run_at,status) VALUES ('A','text','a',0,'done')")
        c.execute("INSERT INTO posts(page_id,kind,caption,run_at,status) VALUES ('A','text','b',0,'failed')")
    client.post("/posts/1/retry")
    client.post("/posts/2/retry")
    assert [r["status"] for r in rows(app_mod)] == ["done", "queued"]


# ---------------------------------------------------------------- validation

def test_single_post_validation(client, app_mod):
    assert "Link must be a web address" in flash_text(client.post("/post", data={"page_ids": "A", "link": "hi"},
                                                                  follow_redirects=True))
    assert "needs a caption or a link" in flash_text(client.post("/post", data={"page_ids": "A"},
                                                                 follow_redirects=True))
    client.post("/post", data={"page_ids": "A", "caption": "hello"})
    client.post("/post", data={"page_ids": "A", "link": "https://example.com"})
    assert len(rows(app_mod)) == 2


# ---------------------------------------------------------------- restart

def test_restart_shifts_missed_posts_keeping_gaps(app_mod):
    now, hour = time.time(), 3600
    with app_mod.db() as c:
        for t in (now - 10 * hour, now - 6 * hour, now + 2 * hour):
            c.execute("INSERT INTO posts(page_id,kind,caption,run_at) VALUES ('A','text','x',?)", (t,))
        c.execute("INSERT INTO posts(page_id,kind,caption,run_at,status) VALUES ('A','text','m',?,'posting')",
                  (now - 11 * hour,))
    app_mod.init_db()
    queued = [r["run_at"] for r in rows(app_mod, "status='queued'")]
    assert abs(queued[0] - time.time()) < 5
    assert [round((t - queued[0]) / hour) for t in queued] == [0, 4, 12]
    assert rows(app_mod, "caption='m'")[0]["status"] == "failed"


# ---------------------------------------------------------------- Graph API import

def test_explain_no_pages_reports_missing_permissions(app_mod, monkeypatch):
    monkeypatch.setattr(app_mod, "graph_get_all",
                        lambda path, params, token: [{"permission": "pages_show_list", "status": "granted"}])
    msg = app_mod.explain_no_pages("t")
    assert "pages_manage_posts" in msg and "pages_show_list" not in msg.split(":")[1]
