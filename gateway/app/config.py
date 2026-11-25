"""Runtime configuration, read once from environment variables."""
import os
from dataclasses import dataclass
from functools import lru_cache


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    database_url: str
    redis_url: str
    dlr_secret: str
    admin_key: str
    carrier_url: str
    # Global limit we allow ourselves towards the carrier. The carrier limit is 50/s;
    # we stay 20% below it: a short event-loop pause releases several granted requests at once.
    carrier_rps: int
    carrier_timeout: float          # seconds before we give up on a submit
    max_attempts: int               # submit attempts before a message is FAILED
    backoff_base: float             # seconds
    backoff_cap: float              # seconds
    worker_concurrency: int         # max in-flight carrier submits per worker process
    lease_seconds: int              # how long a worker owns a message
    max_batch: int
    max_segments: int
    webhook_max_attempts: int
    webhook_timeout: float
    webhook_backoff_base: float
    webhook_backoff_cap: float

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=_env("DATABASE_URL", "postgresql://sms:sms@localhost:5432/sms"),
            redis_url=_env("REDIS_URL", "redis://localhost:6379/0"),
            dlr_secret=_env("DLR_SECRET", "dev-dlr-secret"),
            admin_key=_env("ADMIN_KEY", "dev-admin-key"),
            carrier_url=_env("CARRIER_URL", "http://localhost:9000"),
            carrier_rps=int(_env("CARRIER_RPS", "40")),
            carrier_timeout=float(_env("CARRIER_TIMEOUT", "3.0")),
            max_attempts=int(_env("MAX_ATTEMPTS", "6")),
            backoff_base=float(_env("BACKOFF_BASE", "1.0")),
            backoff_cap=float(_env("BACKOFF_CAP", "20.0")),
            worker_concurrency=int(_env("WORKER_CONCURRENCY", "150")),
            lease_seconds=int(_env("LEASE_SECONDS", "30")),
            max_batch=int(_env("MAX_BATCH", "1000")),
            max_segments=int(_env("MAX_SEGMENTS", "10")),
            webhook_max_attempts=int(_env("WEBHOOK_MAX_ATTEMPTS", "8")),
            webhook_timeout=float(_env("WEBHOOK_TIMEOUT", "5.0")),
            webhook_backoff_base=float(_env("WEBHOOK_BACKOFF_BASE", "2.0")),
            webhook_backoff_cap=float(_env("WEBHOOK_BACKOFF_CAP", "300.0")),
        )


@lru_cache
def get_settings() -> Settings:
    return Settings.from_env()
