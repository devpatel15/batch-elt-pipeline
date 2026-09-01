import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

import extract_gharchive as eg  # noqa: E402


def test_local_path_partitions_by_date_and_uses_unpadded_hour():
    hour = datetime(2026, 8, 6, 9, tzinfo=timezone.utc)
    path = eg.local_path(Path("raw"), hour)
    assert path == Path("raw") / "2026" / "08" / "06" / "9.json.gz"


def test_remote_url_matches_gharchive_naming():
    hour = datetime(2026, 8, 6, 9, tzinfo=timezone.utc)
    assert eg.remote_url(hour) == "https://data.gharchive.org/2026-08-06-9.json.gz"


def test_hour_range_is_half_open_and_hourly():
    start = datetime(2026, 8, 6, 0, tzinfo=timezone.utc)
    end = datetime(2026, 8, 6, 3, tzinfo=timezone.utc)
    hours = list(eg.hour_range(start, end))
    assert hours == [
        datetime(2026, 8, 6, 0, tzinfo=timezone.utc),
        datetime(2026, 8, 6, 1, tzinfo=timezone.utc),
        datetime(2026, 8, 6, 2, tzinfo=timezone.utc),
    ]


def test_fetch_hour_skips_when_local_size_matches_remote(tmp_path):
    hour = datetime(2026, 8, 6, 9, tzinfo=timezone.utc)
    dest = eg.local_path(tmp_path, hour)
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"1234567890")  # 10 bytes

    session = MagicMock()
    head_resp = MagicMock(status_code=200, headers={"Content-Length": "10"})
    session.head.return_value = head_resp

    result = eg.fetch_hour(hour, tmp_path, session, force=False)

    assert result.status == "skipped"
    session.get.assert_not_called()


def test_fetch_hour_redownloads_on_size_mismatch(tmp_path):
    hour = datetime(2026, 8, 6, 9, tzinfo=timezone.utc)
    dest = eg.local_path(tmp_path, hour)
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"stale")

    session = MagicMock()
    session.head.return_value = MagicMock(status_code=200, headers={"Content-Length": "999"})
    get_resp = MagicMock(status_code=200)
    get_resp.iter_content.return_value = [b"a" * 999]
    get_resp.raise_for_status.return_value = None
    session.get.return_value = get_resp

    result = eg.fetch_hour(hour, tmp_path, session, force=False)

    assert result.status == "downloaded"
    session.get.assert_called_once()
    assert dest.stat().st_size == 999


def test_fetch_hour_returns_not_found_on_404(tmp_path):
    hour = datetime(2026, 8, 6, 9, tzinfo=timezone.utc)
    session = MagicMock()
    session.get.return_value = MagicMock(status_code=404)

    result = eg.fetch_hour(hour, tmp_path, session, force=False)

    assert result.status == "not_found"


def test_run_raises_when_any_hour_fails(tmp_path, monkeypatch):
    hour = datetime(2026, 8, 6, 9, tzinfo=timezone.utc)

    def fake_fetch_hour(hour, raw_dir, session, force=False):
        return eg.FetchResult(hour, eg.local_path(raw_dir, hour), "failed")

    monkeypatch.setattr(eg, "fetch_hour", fake_fetch_hour)

    try:
        eg.run(hour, hour + eg.timedelta(hours=1), tmp_path)
        assert False, "expected RuntimeError"
    except RuntimeError:
        pass
