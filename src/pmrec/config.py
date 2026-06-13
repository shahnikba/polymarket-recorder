"""Configuration loading.

Secrets (AWS credentials) are NOT read from this file. Use the standard AWS
mechanisms instead: an IAM role on the EC2 instance (preferred on the server),
or ~/.aws/credentials / AWS_* env vars locally. The config only names the
bucket and prefix.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

import yaml


@dataclass
class Endpoints:
    gamma_base: str = "https://gamma-api.polymarket.com"
    clob_base: str = "https://clob.polymarket.com"
    ws_market: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


@dataclass
class UniverseConfig:
    # How many top-by-24h-volume markets to pull from Gamma as candidates.
    candidate_limit: int = 200
    # Final universe size after book-depth refinement (set equal to
    # candidate_limit to skip refinement entirely).
    target_size: int = 75
    # Re-run selection on this cadence (seconds).
    refresh_interval_s: int = 900  # 15 min
    # Drop markets resolving within this horizon (seconds). 0 = keep all.
    min_seconds_to_resolution: int = 0
    # Hard floors applied server-side via Gamma query params.
    min_volume_24h: float = 0.0
    min_liquidity: float = 0.0
    # Whether to refine the candidate set by fetching CLOB books and ranking
    # on real spread/depth rather than the Gamma liquidity estimate.
    refine_with_book_depth: bool = True


@dataclass
class CaptureConfig:
    # Max asset_ids per WebSocket connection. Sharded across this many.
    # NOTE: a per-connection subscription cap has been reported but its exact
    # value on the main CLOB market channel is unverified. Keep this modest
    # and shard; tune once you've confirmed behaviour against the live feed.
    max_assets_per_connection: int = 50
    ping_interval_s: float = 10.0          # docs: PING every 10s or server drops you
    stale_after_s: float = 30.0            # no messages for this long -> reconnect
    reconnect_base_s: float = 1.0
    reconnect_max_s: float = 30.0
    # Enable lifecycle / best-bid-ask events (requires custom_feature_enabled).
    custom_feature_enabled: bool = True


@dataclass
class ArchiveConfig:
    spool_dir: str = "./data/spool"        # frames being written right now
    pending_dir: str = "./data/pending"    # rotated files awaiting S3 upload
    rotate_interval_s: int = 300           # 5 min: roll the current file
    rotate_max_bytes: int = 256 * 1024 * 1024  # also roll if it gets big


@dataclass
class S3Config:
    bucket: str = "CHANGE_ME"
    prefix: str = "polymarket/raw"
    flush_interval_s: int = 60             # scan pending_dir and upload
    region: str | None = None
    delete_after_upload: bool = True


@dataclass
class RateLimitConfig:
    # Gamma unauthenticated is ~60 req/min. CLOB book snapshots share that
    # budget on reconnects, so cap the global REST rate well under it.
    rest_min_interval_s: float = 1.2       # ~50 req/min, headroom under 60


@dataclass
class Config:
    endpoints: Endpoints = field(default_factory=Endpoints)
    universe: UniverseConfig = field(default_factory=UniverseConfig)
    capture: CaptureConfig = field(default_factory=CaptureConfig)
    archive: ArchiveConfig = field(default_factory=ArchiveConfig)
    s3: S3Config = field(default_factory=S3Config)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)

    @staticmethod
    def load(path: str | None = None) -> "Config":
        path = path or os.environ.get("PMREC_CONFIG", "config.yaml")
        if not os.path.exists(path):
            # Fall back to all defaults so it runs out of the box for dev.
            return Config()
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
        return Config(
            endpoints=Endpoints(**raw.get("endpoints", {})),
            universe=UniverseConfig(**raw.get("universe", {})),
            capture=CaptureConfig(**raw.get("capture", {})),
            archive=ArchiveConfig(**raw.get("archive", {})),
            s3=S3Config(**raw.get("s3", {})),
            rate_limit=RateLimitConfig(**raw.get("rate_limit", {})),
        )
