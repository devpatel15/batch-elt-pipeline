"""
Loads a day's worth of already-ingested GH Archive files (raw/YYYY/MM/DD/H.json.gz)
into a BigQuery raw table, one partition per day.

`actor`/`repo`/`org` are consistent RECORD shapes across every GH Archive event
type, so they're given an explicit schema. `payload` varies wildly by event type
(a PushEvent payload looks nothing like a WatchEvent payload) - rather than fight
that with a STRUCT per event type, it's loaded as BigQuery's native JSON column
type and queried downstream with JSON_VALUE/JSON_QUERY in dbt.

Idempotent by construction: each run overwrites the target day's partition
(`table$YYYYMMDD`, WRITE_TRUNCATE) rather than appending, so re-running a load
for a day you've already loaded produces the same result instead of duplicates.

Usage:
    python load_to_bigquery.py --date 2026-08-06 --project my-gcp-project
"""

from __future__ import annotations

import argparse
import gzip
import logging
import sys
from datetime import date, datetime
from pathlib import Path

from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("load_to_bigquery")

DEFAULT_DATASET = "raw_gharchive"
DEFAULT_TABLE = "events"

TABLE_SCHEMA = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField(
        "actor",
        "RECORD",
        fields=[
            bigquery.SchemaField("id", "INT64"),
            bigquery.SchemaField("login", "STRING"),
            bigquery.SchemaField("display_login", "STRING"),
            bigquery.SchemaField("gravatar_id", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("avatar_url", "STRING"),
        ],
    ),
    bigquery.SchemaField(
        "repo",
        "RECORD",
        fields=[
            bigquery.SchemaField("id", "INT64"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("url", "STRING"),
        ],
    ),
    bigquery.SchemaField(
        "org",
        "RECORD",
        fields=[
            bigquery.SchemaField("id", "INT64"),
            bigquery.SchemaField("login", "STRING"),
            bigquery.SchemaField("gravatar_id", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("avatar_url", "STRING"),
        ],
    ),
    bigquery.SchemaField("payload", "JSON"),
    bigquery.SchemaField("public", "BOOL"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
]


def day_source_files(raw_dir: Path, day: date) -> list[Path]:
    day_dir = raw_dir / f"{day.year:04d}" / f"{day.month:02d}" / f"{day.day:02d}"
    if not day_dir.is_dir():
        return []
    return sorted(day_dir.glob("*.json.gz"), key=lambda p: int(p.name.split(".")[0]))


class ChainedGzipReader:
    """A read()-only file-like object that decompresses and concatenates
    multiple gzip files into one byte stream, without ever writing the
    decompressed content to disk.

    An earlier version wrote a full day's decompression (~2.5GB) to a temp
    file before uploading. That temp file was correctly deleted afterward,
    but on Windows/WSL2, Docker's dynamically-expanding virtual disk doesn't
    release blocks back to the host just because the file that used them was
    deleted - each load permanently inflated host disk usage by roughly the
    decompressed size, even though nothing was actually left behind logically.
    Streaming straight into the upload, a chunk at a time, avoids ever holding
    more than one chunk in memory and never touches disk at all.

    GH Archive hourly files aren't guaranteed to end in a trailing newline, so
    a newline is inserted between files whenever one is missing - otherwise
    the last line of one file and the first line of the next could glue into
    `...}{...`, which BigQuery's NDJSON parser rejects.
    """

    def __init__(self, paths: list[Path]):
        self._paths = list(paths)
        self._index = 0
        self._current: gzip.GzipFile | None = None
        self._last_byte = b"\n"  # pretend true so no leading newline is inserted
        self._pending_separator = False
        self._position = 0
        self._open_next()

    def tell(self) -> int:
        # google-resumable-media checks this is 0 before starting an upload.
        return self._position

    def seekable(self) -> bool:
        # Declared explicitly so the upload library doesn't assume it can
        # seek to determine size or retry a chunk - this stream is forward-only.
        return False

    def _open_next(self) -> bool:
        if self._current is not None:
            self._current.close()
            self._current = None
        if self._index >= len(self._paths):
            return False
        self._current = gzip.open(self._paths[self._index], "rb")
        self._index += 1
        return True

    def read(self, size: int = -1) -> bytes:
        out = bytearray()
        target = size if size is not None and size >= 0 else None

        while target is None or len(out) < target:
            if self._pending_separator:
                out += b"\n"
                self._last_byte = b"\n"
                self._pending_separator = False
                continue

            if self._current is None:
                break

            want = (target - len(out)) if target is not None else 1024 * 1024
            chunk = self._current.read(want)
            if chunk:
                out += chunk
                self._last_byte = chunk[-1:]
            else:
                ended_with_newline = self._last_byte == b"\n"
                has_more = self._open_next()
                if not has_more:
                    break
                if not ended_with_newline:
                    self._pending_separator = True

        self._position += len(out)
        return bytes(out)

    def close(self) -> None:
        if self._current is not None:
            self._current.close()
            self._current = None

    def __enter__(self) -> "ChainedGzipReader":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()


def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        logger.info("dataset %s not found, creating it", dataset_id)
        client.create_dataset(bigquery.Dataset(dataset_ref))


def ensure_table(client: bigquery.Client, dataset_id: str, table_id: str) -> bigquery.TableReference:
    table_ref = bigquery.DatasetReference(client.project, dataset_id).table(table_id)
    try:
        client.get_table(table_ref)
    except Exception:
        logger.info("table %s.%s not found, creating it", dataset_id, table_id)
        table = bigquery.Table(table_ref, schema=TABLE_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="created_at"
        )
        table.clustering_fields = ["type"]
        client.create_table(table)
    return table_ref


def load_day(client: bigquery.Client, dataset_id: str, table_id: str, day: date, raw_dir: Path) -> int:
    files = day_source_files(raw_dir, day)
    if not files:
        raise FileNotFoundError(f"no raw files found for {day} under {raw_dir}")

    ensure_dataset(client, dataset_id)
    table_ref = ensure_table(client, dataset_id, table_id)

    # time_partitioning/clustering are already set on the destination table by
    # ensure_table(); redeclaring them here (without an exact match) makes
    # BigQuery reject the load with an "Incompatible table partitioning" error.
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=TABLE_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    partition_table_id = f"{table_ref.table_id}${day.strftime('%Y%m%d')}"
    destination = bigquery.DatasetReference(table_ref.project, table_ref.dataset_id).table(partition_table_id)

    logger.info("streaming %d hourly files for %s directly into the BigQuery load job", len(files), day)
    with ChainedGzipReader(files) as stream:
        job = client.load_table_from_file(stream, destination, job_config=job_config)
    job.result()  # raises on failure

    logger.info("loaded %d rows into %s.%s$%s", job.output_rows, dataset_id, table_id, day.strftime("%Y%m%d"))
    return job.output_rows


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load a day of raw GH Archive files into BigQuery.")
    parser.add_argument("--date", type=str, required=True, help="Date to load, YYYY-MM-DD.")
    parser.add_argument("--project", type=str, default=None, help="GCP project ID (defaults to ADC project).")
    parser.add_argument("--dataset", type=str, default=DEFAULT_DATASET)
    parser.add_argument("--table", type=str, default=DEFAULT_TABLE)
    parser.add_argument("--raw-dir", type=str, default="raw")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    day = datetime.strptime(args.date, "%Y-%m-%d").date()

    client = bigquery.Client(project=args.project)
    logger.info("loading %s into %s.%s.%s", day, client.project, args.dataset, args.table)

    try:
        load_day(client, args.dataset, args.table, day, Path(args.raw_dir))
    except FileNotFoundError as exc:
        logger.error(str(exc))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
