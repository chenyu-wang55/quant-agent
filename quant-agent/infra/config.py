from __future__ import annotations

import os
from dataclasses import dataclass

_TRUE_VALUES = {"1", "true", "yes", "on"}


def env_text(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def env_flag(name: str, default: bool = False) -> bool:
    fallback = "1" if default else "0"
    return env_text(name, fallback).lower() in _TRUE_VALUES


def env_int(name: str, default: int, *, minimum: int | None = None) -> int:
    try:
        value = int(env_text(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def env_float(name: str, default: float, *, minimum: float | None = None) -> float:
    try:
        value = float(env_text(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


@dataclass(frozen=True)
class CoreSettings:
    database_url: str
    data_provider: str
    broker_adapter: str
    auth_disabled: bool
    test_mode: bool

    @classmethod
    def from_env(cls) -> "CoreSettings":
        return cls(
            database_url=env_text("DATABASE_URL", "sqlite:///./quant_agent_v2.db"),
            data_provider=env_text("DATA_PROVIDER", "yfinance").lower(),
            broker_adapter=env_text("QUANT_BROKER_ADAPTER").lower(),
            auth_disabled=env_flag("QUANT_AGENT_DISABLE_AUTH"),
            test_mode=env_flag("QUANT_AGENT_TEST_MODE"),
        )


@dataclass(frozen=True)
class ObservabilitySettings:
    log_level: str
    log_format: str
    log_file: str
    log_max_bytes: int
    log_backup_count: int
    database_size_alert_bytes: int
    consecutive_error_alert_count: int
    external_health_probe: bool

    @classmethod
    def from_env(cls) -> "ObservabilitySettings":
        return cls(
            log_level=env_text("QUANT_AGENT_LOG_LEVEL", "INFO").upper(),
            log_format=env_text("QUANT_AGENT_LOG_FORMAT", "json").lower(),
            log_file=env_text("QUANT_AGENT_LOG_FILE"),
            log_max_bytes=env_int("QUANT_AGENT_LOG_MAX_BYTES", 20 * 1024 * 1024, minimum=1),
            log_backup_count=env_int("QUANT_AGENT_LOG_BACKUP_COUNT", 10, minimum=1),
            database_size_alert_bytes=env_int(
                "QUANT_AGENT_DB_SIZE_ALERT_BYTES", 1024**3, minimum=1
            ),
            consecutive_error_alert_count=env_int(
                "QUANT_AGENT_CONSECUTIVE_ERROR_ALERT_COUNT", 2, minimum=1
            ),
            external_health_probe=env_flag("QUANT_AGENT_HEALTH_PROBE_EXTERNAL"),
        )
