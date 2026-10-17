import bulk_download as bd


def test_atempo_chain_stays_in_ffmpeg_range():
    assert bd.atempo_chain(1.5) == "atempo=1.5"
    assert bd.atempo_chain(3.0) == "atempo=2.0,atempo=1.5"
    assert bd.atempo_chain(0.25) == "atempo=0.5,atempo=0.5"


def test_normalize_urls():
    assert bd.normalize("youtube.com/@name") == "https://youtube.com/@name/videos"
    assert bd.normalize('"https://www.tiktok.com/@x"') == "https://www.tiktok.com/@x"


def test_find_videos_skips_edited_and_already_done(tmp_path):
    (tmp_path / "sub").mkdir()
    for name in ("a.mp4", "b.mkv", "sub/c.webm", "note.txt", "d.mp4", "d.edited.mp4"):
        (tmp_path / name).write_bytes(b"x")
    found = sorted(p.name for p in bd.find_videos([str(tmp_path)]))
    assert found == ["a.mp4", "b.mkv", "c.webm"]


def test_parser_defaults():
    args = bd.build_parser().parse_args(["edit", "x"])
    assert args.speed == 1.5 and args.flip is True and args.keep_original is False
    args = bd.build_parser().parse_args(["download", "u", "--no-edit"])
    assert args.no_edit is True
