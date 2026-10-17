import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "fb_scheduler"))

import app as fb_app  # noqa: E402


@pytest.fixture
def app_mod(tmp_path, monkeypatch):
    """Scheduler module wired to a throwaway database, never the real posts.db."""
    monkeypatch.setattr(fb_app, "DB_PATH", tmp_path / "test.db")
    fb_app.init_db()
    with fb_app.db() as c:
        c.execute("INSERT INTO pages(id, name, token) VALUES ('A', 'Page A', 'x'), ('B', 'Page B', 'y')")
    return fb_app


@pytest.fixture
def client(app_mod):
    app_mod.app.config["TESTING"] = True
    return app_mod.app.test_client()


@pytest.fixture
def videos(tmp_path):
    """Three files: two distinct 'videos' plus a renamed copy of the first."""
    d = tmp_path / "vids"
    d.mkdir()
    (d / "v1.mp4").write_bytes(b"video-one" * 1000)
    (d / "v2.mp4").write_bytes(b"video-two" * 1000)
    (d / "v1_copy.mp4").write_bytes(b"video-one" * 1000)
    return d
