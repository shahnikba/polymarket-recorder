import gzip

import pmrec.flusher as flusher_mod
from pmrec.flusher import S3Flusher


class FakeS3:
    def __init__(self):
        self.uploads = []          # (local_path, bucket, key)

    def upload_file(self, path, bucket, key):
        # Record, and verify the file is valid gzip JSONL while it still exists.
        with gzip.open(path, "rb") as f:
            f.read()
        self.uploads.append((path, bucket, key))


def _patch_boto(monkeypatch):
    fake = FakeS3()
    monkeypatch.setattr(flusher_mod.boto3, "client", lambda *a, **k: fake)
    return fake


def test_key_is_date_partitioned(monkeypatch):
    _patch_boto(monkeypatch)
    f = S3Flusher("d", "bucket", "polymarket/raw", 60, None, True)
    key = f._key_for("frames_X.jsonl")
    parts = key.split("/")
    assert parts[0] == "polymarket" and parts[1] == "raw"
    assert key.endswith("frames_X.jsonl.gz")
    # .../YYYY/MM/DD/...
    assert parts[2].isdigit() and len(parts[2]) == 4


def test_change_me_bucket_is_noop(tmp_path, monkeypatch):
    _patch_boto(monkeypatch)
    pending = tmp_path / "pending"
    pending.mkdir()
    src = pending / "frames_a.jsonl"
    src.write_text('{"x":1}\n')
    f = S3Flusher(str(pending), "CHANGE_ME", "p", 60, None, True)
    f._flush_once()
    assert src.exists()            # left local, untouched


def test_flush_uploads_gzips_and_deletes(tmp_path, monkeypatch):
    fake = _patch_boto(monkeypatch)
    pending = tmp_path / "pending"
    pending.mkdir()
    src = pending / "frames_a.jsonl"
    src.write_text('{"x":1}\n{"y":2}\n')
    f = S3Flusher(str(pending), "real-bucket", "p", 60, None, True)
    f._flush_once()

    assert len(fake.uploads) == 1
    _, bucket, key = fake.uploads[0]
    assert bucket == "real-bucket" and key.endswith("frames_a.jsonl.gz")
    assert not src.exists()                       # deleted after upload
    assert list(pending.glob("*.gz")) == []       # temp gz cleaned up
