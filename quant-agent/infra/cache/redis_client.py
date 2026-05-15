from __future__ import annotations

import os
from typing import Any


class CacheClient:
    """Simple cache abstraction with in-memory fallback for MVP."""

    def __init__(self) -> None:
        self._memory: dict[str, Any] = {}
        self._redis = None
        redis_url = os.getenv("REDIS_URL")
        if redis_url:
            try:
                import redis  # type: ignore

                self._redis = redis.from_url(redis_url)
            except Exception:
                self._redis = None

    def get(self, key: str) -> Any:
        if self._redis is not None:
            value = self._redis.get(key)
            return value
        return self._memory.get(key)

    def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        if self._redis is not None:
            self._redis.set(name=key, value=value, ex=ttl_seconds)
            return
        self._memory[key] = value
