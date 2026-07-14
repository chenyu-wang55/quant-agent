from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from time import sleep

import apps.api.dependencies as dependencies


def test_get_app_state_constructs_singleton_once_under_concurrency(monkeypatch) -> None:
    sentinel = object()
    calls = 0
    calls_lock = Lock()

    def build_state() -> object:
        nonlocal calls
        sleep(0.02)
        with calls_lock:
            calls += 1
        return sentinel

    with monkeypatch.context() as patch:
        patch.setattr(dependencies, "_APP_STATE", None)
        patch.setattr(dependencies, "AppState", build_state)
        with ThreadPoolExecutor(max_workers=16) as executor:
            results = list(executor.map(lambda _: dependencies.get_app_state(), range(64)))

        assert calls == 1
        assert all(result is sentinel for result in results)
