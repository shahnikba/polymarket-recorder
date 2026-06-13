from pmrec.config import Config


def test_defaults_when_no_file(tmp_path, monkeypatch):
    monkeypatch.delenv("PMREC_CONFIG", raising=False)
    cfg = Config.load(str(tmp_path / "does-not-exist.yaml"))
    assert cfg.s3.bucket == "CHANGE_ME"
    assert cfg.universe.target_size == 75
    assert cfg.capture.max_assets_per_connection == 50


def test_partial_yaml_overrides_only_given_keys(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text(
        "s3:\n  bucket: my-bucket\nuniverse:\n  target_size: 10\n"
    )
    cfg = Config.load(str(p))
    assert cfg.s3.bucket == "my-bucket"
    assert cfg.s3.prefix == "polymarket/raw"          # default preserved
    assert cfg.universe.target_size == 10
    assert cfg.universe.candidate_limit == 200        # default preserved


def test_env_points_at_config(tmp_path, monkeypatch):
    p = tmp_path / "elsewhere.yaml"
    p.write_text("s3:\n  bucket: from-env\n")
    monkeypatch.setenv("PMREC_CONFIG", str(p))
    cfg = Config.load()
    assert cfg.s3.bucket == "from-env"
