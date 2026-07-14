from __future__ import annotations

import json
import logging
import re
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterator

from infra.config import ObservabilitySettings

_PASSWORD_QUERY = re.compile(r"([?&]pwd=)[^&\s\"]+", re.IGNORECASE)
_SECRET_FIELD = re.compile(
    r'((?:password|api[_-]?key|secret|authorization)["\']?\s*[:=]\s*["\']?)[^,}\s"\']+',
    re.IGNORECASE,
)
_BEARER = re.compile(r"(bearer\s+)[a-z0-9._~+/=-]+", re.IGNORECASE)
_STANDARD_LOG_RECORD_FIELDS = set(logging.makeLogRecord({}).__dict__)

correlation_id_var: ContextVar[str | None] = ContextVar("correlation_id", default=None)
run_id_var: ContextVar[str | None] = ContextVar("run_id", default=None)
order_id_var: ContextVar[str | None] = ContextVar("order_id", default=None)


def _redact_string(value: str) -> str:
    redacted = _PASSWORD_QUERY.sub(r"\1[REDACTED]", value)
    redacted = _BEARER.sub(r"\1[REDACTED]", redacted)
    redacted = _SECRET_FIELD.sub(r"\1[REDACTED]", redacted)
    return redacted


def _json_safe(value: Any) -> Any:
    if isinstance(value, str):
        return _redact_string(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return _redact_string(str(value))


class SensitiveQueryFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = _redact_string(record.msg)
        if isinstance(record.args, tuple):
            record.args = tuple(_json_safe(value) for value in record.args)
        elif isinstance(record.args, dict):
            record.args = {key: _json_safe(value) for key, value in record.args.items()}
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": _redact_string(record.getMessage()),
        }
        context_values = {
            "correlation_id": getattr(record, "correlation_id", None) or correlation_id_var.get(),
            "run_id": getattr(record, "run_id", None) or run_id_var.get(),
            "order_id": getattr(record, "order_id", None) or order_id_var.get(),
        }
        payload.update({key: value for key, value in context_values.items() if value})
        for key, value in record.__dict__.items():
            if key in _STANDARD_LOG_RECORD_FIELDS or key in payload or key.startswith("_"):
                continue
            payload[key] = _json_safe(value)
        if record.exc_info:
            payload["exception"] = _redact_string(self.formatException(record.exc_info))
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


@contextmanager
def log_context(
    *,
    correlation_id: str | None = None,
    run_id: str | None = None,
    order_id: str | None = None,
) -> Iterator[None]:
    tokens: list[tuple[ContextVar[str | None], Any]] = []
    if correlation_id is not None:
        tokens.append((correlation_id_var, correlation_id_var.set(correlation_id)))
    if run_id is not None:
        tokens.append((run_id_var, run_id_var.set(run_id)))
    if order_id is not None:
        tokens.append((order_id_var, order_id_var.set(order_id)))
    try:
        yield
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)


def configure_logging() -> None:
    settings = ObservabilitySettings.from_env()
    level = getattr(logging, settings.log_level, logging.INFO)
    formatter: logging.Formatter
    if settings.log_format == "json":
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")

    root = logging.getLogger()
    root.setLevel(level)
    if not root.handlers:
        root.addHandler(logging.StreamHandler())

    log_file = settings.log_file
    if log_file and not any(
        isinstance(handler, RotatingFileHandler)
        and Path(getattr(handler, "baseFilename", "")) == Path(log_file).expanduser().resolve()
        for handler in root.handlers
    ):
        path = Path(log_file).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        root.addHandler(
            RotatingFileHandler(
                path,
                maxBytes=settings.log_max_bytes,
                backupCount=settings.log_backup_count,
                encoding="utf-8",
            )
        )

    sensitive_filter = SensitiveQueryFilter()
    for handler in root.handlers:
        handler.setLevel(level)
        handler.setFormatter(formatter)
        if not any(isinstance(item, SensitiveQueryFilter) for item in handler.filters):
            handler.addFilter(sensitive_filter)
    uvicorn_access = logging.getLogger("uvicorn.access")
    if not any(isinstance(item, SensitiveQueryFilter) for item in uvicorn_access.filters):
        uvicorn_access.addFilter(sensitive_filter)
