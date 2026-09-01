import gzip
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

import load_to_bigquery as lb  # noqa: E402


def test_day_source_files_sorts_numerically_not_lexically(tmp_path):
    day_dir = tmp_path / "2026" / "08" / "06"
    day_dir.mkdir(parents=True)
    for hour in [2, 10, 1, 20, 9]:
        (day_dir / f"{hour}.json.gz").write_bytes(b"")

    files = lb.day_source_files(tmp_path, date(2026, 8, 6))

    assert [f.name for f in files] == ["1.json.gz", "2.json.gz", "9.json.gz", "10.json.gz", "20.json.gz"]


def test_day_source_files_returns_empty_list_when_day_missing(tmp_path):
    assert lb.day_source_files(tmp_path, date(2026, 8, 6)) == []


def _read_all_in_chunks(reader: "lb.ChainedGzipReader", chunk_size: int) -> bytes:
    out = bytearray()
    while True:
        chunk = reader.read(chunk_size)
        if not chunk:
            break
        out += chunk
    return bytes(out)


def test_chained_gzip_reader_joins_files_in_order(tmp_path):
    a = tmp_path / "0.json.gz"
    b = tmp_path / "1.json.gz"
    with gzip.open(a, "wb") as f:
        f.write(b'{"hour": 0}\n')
    with gzip.open(b, "wb") as f:
        f.write(b'{"hour": 1}\n')

    with lb.ChainedGzipReader([a, b]) as reader:
        result = reader.read(-1)

    assert result == b'{"hour": 0}\n{"hour": 1}\n'


def test_chained_gzip_reader_inserts_newline_when_source_lacks_trailing_one(tmp_path):
    # GH Archive files aren't guaranteed to end in a newline; without one, the
    # last line of file A and first line of file B would glue into `...}{...`.
    a = tmp_path / "0.json.gz"
    b = tmp_path / "1.json.gz"
    with gzip.open(a, "wb") as f:
        f.write(b'{"hour": 0}')  # no trailing newline
    with gzip.open(b, "wb") as f:
        f.write(b'{"hour": 1}\n')

    with lb.ChainedGzipReader([a, b]) as reader:
        result = reader.read(-1)

    assert result.splitlines() == [b'{"hour": 0}', b'{"hour": 1}']


def test_chained_gzip_reader_produces_same_bytes_regardless_of_chunk_size(tmp_path):
    # The real caller (BigQuery's resumable upload) reads in fixed-size
    # chunks, not all-at-once - the separator-insertion logic has to hold up
    # no matter where a chunk boundary happens to fall relative to a file
    # boundary.
    files = []
    for i in range(3):
        path = tmp_path / f"{i}.json.gz"
        with gzip.open(path, "wb") as f:
            f.write(f'{{"hour": {i}}}'.encode())  # no trailing newline on any file
        files.append(path)

    with lb.ChainedGzipReader(files) as reader:
        whole = reader.read(-1)

    for chunk_size in (1, 3, 7, 1024):
        with lb.ChainedGzipReader(files) as reader:
            chunked = _read_all_in_chunks(reader, chunk_size)
        assert chunked == whole, f"mismatch at chunk_size={chunk_size}"

    assert whole.splitlines() == [b'{"hour": 0}', b'{"hour": 1}', b'{"hour": 2}']


def test_chained_gzip_reader_handles_empty_file_list():
    with lb.ChainedGzipReader([]) as reader:
        assert reader.read(-1) == b""
