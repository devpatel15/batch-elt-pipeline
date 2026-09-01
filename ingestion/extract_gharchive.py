"""
Ingests hourly GH Archive files (https://www.gharchive.org/) into a local
raw landing zone, partitioned as raw/YYYY/MM/DD/HH.json.gz.

GH Archive publishes one gzipped JSON-lines file per hour, named
YYYY-MM-DD-H.json.gz (H has no leading zero, 0-23). Files are immutable
once published, so downloads are idempotent: an existing file with a
size matching the remote Content-Length is never re-fetched.

Usage:
    python extract_gharchive.py --hours 24
    python extract_gharchive.py --start 2026-08-01T00:00:00 --end 2026-08-02T00:00:00
    python extract_gharchive.py --hours 24 --force
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

GHARCHIVE_URL_TMPL = "https://data.gharchive.org/{year:04d}-{month:02d}-{day:02d}-{hour}.json.gz"

# GH Archive lags real time; hours newer than this are frequently not yet
# published, which would otherwise show up as spurious 404s on every run.
PUBLISH_DELAY = timedelta(hours=2)

MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 2
REQUEST_TIMEOUT_SECONDS = 30
CHUNK_SIZE = 1024 * 1024  # 1 MiB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("extract_gharchive")


@dataclass
class FetchResult:
    hour: datetime
    path: Path
    status: str  # "downloaded", "skipped", "not_found", "failed"
    bytes: int = 0


def hour_range(start: datetime, end: datetime):
    """Yield each UTC hour in [start, end), truncated to the hour."""
    current = start.replace(minute=0, second=0, microsecond=0)
    end = end.replace(minute=0, second=0, microsecond=0)
    while current < end:
        yield current
        current += timedelta(hours=1)


def local_path(raw_dir: Path, hour: datetime) -> Path:
    return raw_dir / f"{hour.year:04d}" / f"{hour.month:02d}" / f"{hour.day:02d}" / f"{hour.hour}.json.gz"


def remote_url(hour: datetime) -> str:
    return GHARCHIVE_URL_TMPL.format(year=hour.year, month=hour.month, day=hour.day, hour=hour.hour)


def _remote_content_length(url: str, session: requests.Session) -> int | None:
    resp = session.head(url, timeout=REQUEST_TIMEOUT_SECONDS, allow_redirects=True)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    length = resp.headers.get("Content-Length")
    return int(length) if length is not None else None


def fetch_hour(hour: datetime, raw_dir: Path, session: requests.Session, force: bool = False) -> FetchResult:
    dest = local_path(raw_dir, hour)
    url = remote_url(hour)

    if not force and dest.exists():
        remote_size = _remote_content_length(url, session)
        if remote_size is None:
            logger.info("not yet published, skipping: %s", url)
            return FetchResult(hour, dest, "not_found")
        if dest.stat().st_size == remote_size:
            logger.info("already present, skipping: %s", dest)
            return FetchResult(hour, dest, "skipped", bytes=remote_size)
        logger.warning("size mismatch for %s (local=%d remote=%d), re-downloading",
                        dest, dest.stat().st_size, remote_size)

    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, stream=True, timeout=REQUEST_TIMEOUT_SECONDS)
            if resp.status_code == 404:
                logger.info("not yet published, skipping: %s", url)
                return FetchResult(hour, dest, "not_found")
            resp.raise_for_status()

            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = dest.with_suffix(dest.suffix + ".part")
            total = 0
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        total += len(chunk)
            tmp_path.replace(dest)  # atomic on same filesystem

            logger.info("downloaded %s (%d bytes)", dest, total)
            return FetchResult(hour, dest, "downloaded", bytes=total)

        except requests.RequestException as exc:
            last_error = exc
            wait = RETRY_BACKOFF_SECONDS * attempt
            logger.warning("attempt %d/%d failed for %s: %s (retrying in %ds)",
                            attempt, MAX_RETRIES, url, exc, wait)
            time.sleep(wait)

    logger.error("giving up on %s after %d attempts: %s", url, MAX_RETRIES, last_error)
    return FetchResult(hour, dest, "failed")


def run(start: datetime, end: datetime, raw_dir: Path, force: bool = False) -> list[FetchResult]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    results: list[FetchResult] = []
    with requests.Session() as session:
        for hour in hour_range(start, end):
            results.append(fetch_hour(hour, raw_dir, session, force=force))

    counts: dict[str, int] = {}
    total_bytes = 0
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
        total_bytes += r.bytes

    logger.info(
        "run complete: %d hours requested | downloaded=%d skipped=%d not_found=%d failed=%d | %.1f MB new/verified",
        len(results),
        counts.get("downloaded", 0),
        counts.get("skipped", 0),
        counts.get("not_found", 0),
        counts.get("failed", 0),
        total_bytes / (1024 * 1024),
    )

    if counts.get("failed", 0) > 0:
        raise RuntimeError(f"{counts['failed']} hour(s) failed to download after retries")

    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest hourly GH Archive files into a raw landing zone.")
    parser.add_argument("--hours", type=int, default=24,
                         help="Number of trailing hours to ingest, ending 2h before now (default: 24).")
    parser.add_argument("--start", type=str, default=None,
                         help="ISO8601 UTC start datetime (inclusive). Overrides --hours.")
    parser.add_argument("--end", type=str, default=None,
                         help="ISO8601 UTC end datetime (exclusive). Defaults to now minus publish delay.")
    parser.add_argument("--raw-dir", type=str, default="raw",
                         help="Root directory for the partitioned raw landing zone (default: raw).")
    parser.add_argument("--force", action="store_true",
                         help="Re-download files even if already present locally.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    now = datetime.now(timezone.utc)
    end = datetime.fromisoformat(args.end) if args.end else now - PUBLISH_DELAY
    start = datetime.fromisoformat(args.start) if args.start else end - timedelta(hours=args.hours)

    logger.info("ingesting GH Archive hours from %s to %s (exclusive)", start, end)

    try:
        run(start, end, Path(args.raw_dir), force=args.force)
    except RuntimeError as exc:
        logger.error(str(exc))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
