import time


def add_post(app_mod, **kw):
    row = {"page_id": "A", "kind": "text", "caption": "c", "run_at": time.time() + 3600, "status": "queued"}
    row.update(kw)
    cols = ",".join(row)
    with app_mod.db() as c:
        return c.execute(f"INSERT INTO posts({cols}) VALUES ({','.join('?' * len(row))})",
                         tuple(row.values())).lastrowid


def test_state_returns_range_counts_and_page_colors(client, app_mod):
    now = time.time()
    add_post(app_mod, run_at=now + 3600)
    add_post(app_mod, run_at=now + 30 * 86400)  # outside the 7-day window
    add_post(app_mod, run_at=now - 60, status="done", posted_at=now - 30, result="123")
    data = client.get(f"/api/state?start={now - 3600}&days=7").get_json()
    assert len(data["posts"]) == 2
    assert data["counts"] == {"queued": 2, "done": 1}
    assert data["posted_today"] == 1
    assert data["next"]["caption"] == "c"
    assert all(p["color"].startswith("#") for p in data["pages"])
    done = [p for p in data["posts"] if p["status"] == "done"][0]
    assert done["fb_url"] == "https://www.facebook.com/123"
    assert "token" not in done and "file_hash" not in done


def test_edit_post_now_and_delete(client, app_mod):
    pid = add_post(app_mod)
    r = client.post(f"/api/posts/{pid}", json={"caption": "new", "run_at": "2030-01-01T09:30"})
    assert r.get_json()["caption"] == "new"
    assert client.post(f"/api/posts/{pid}/now").status_code == 200
    assert abs(client.get(f"/api/posts/{pid}").get_json()["run_at"] - time.time()) < 5
    client.post(f"/api/posts/{pid}/delete")
    assert client.get(f"/api/posts/{pid}").status_code == 404


def test_done_posts_cannot_be_edited_or_sent_again(client, app_mod):
    pid = add_post(app_mod, status="done", result="9")
    assert client.post(f"/api/posts/{pid}", json={"caption": "x"}).status_code == 400
    assert client.post(f"/api/posts/{pid}/now").status_code == 400


def test_reschedule_respaces_per_page_in_order(client, app_mod):
    for i, page in enumerate(["A", "A", "B", "A"]):
        add_post(app_mod, page_id=page, run_at=time.time() + i * 60)
    add_post(app_mod, status="done", run_at=1)
    r = client.post("/api/queue/reschedule", json={"start": "2030-01-01T08:00", "gap": 120})
    assert r.get_json()["moved"] == 4
    with app_mod.db() as c:
        a = [x[0] for x in c.execute("SELECT run_at FROM posts WHERE page_id='A' AND status='queued' ORDER BY id")]
        b = [x[0] for x in c.execute("SELECT run_at FROM posts WHERE page_id='B' AND status='queued'")]
        done = c.execute("SELECT run_at FROM posts WHERE status='done'").fetchone()[0]
    assert [round(t - a[0]) for t in a] == [0, 7200, 14400]
    assert b == [a[0]]
    assert done == 1


def test_clear_queue_keeps_history(client, app_mod):
    add_post(app_mod)
    add_post(app_mod, page_id="B")
    add_post(app_mod, status="done")
    assert client.post("/api/queue/clear", json={"page_id": "B"}).get_json()["deleted"] == 1
    assert client.post("/api/queue/clear", json={}).get_json()["deleted"] == 1
    with app_mod.db() as c:
        assert c.execute("SELECT status FROM posts").fetchall()[0][0] == "done"


def test_thumb_404_for_text_post(client, app_mod):
    assert client.get(f"/thumb/{add_post(app_mod)}").status_code == 404


def test_insights_totals_and_cache(client, app_mod, monkeypatch):
    add_post(app_mod, status="done", result="111", posted_at=time.time())
    calls = []

    def fake_get(path, params, token):
        calls.append(path)
        if path in ("A", "B"):
            return {"followers_count": 100}
        return {"reactions": {"summary": {"total_count": 5}}, "comments": {"summary": {"total_count": 2}}}

    monkeypatch.setattr(app_mod, "graph_get", fake_get)
    monkeypatch.setitem(app_mod._insights_cache, "at", 0.0)
    data = client.get("/api/insights").get_json()
    assert (data["followers"], data["reactions"], data["comments"]) == (200, 5, 2)
    n = len(calls)
    client.get("/api/insights")
    assert len(calls) == n  # served from cache
