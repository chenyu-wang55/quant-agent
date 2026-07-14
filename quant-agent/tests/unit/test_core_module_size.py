from __future__ import annotations

from pathlib import Path

from scripts.check_core_module_size import DEFAULT_MAX_LINES, core_modules, module_size_violations


def test_core_api_worker_and_dashboard_modules_stay_within_size_limit() -> None:
    root = Path(__file__).resolve().parents[2]
    modules = core_modules(root)

    assert modules
    assert root / "apps/api/dependencies.py" in modules
    assert root / "apps/worker/main.py" in modules
    assert root / "apps/dashboard/main.py" in modules
    assert root / "apps/dashboard/home_page.py" not in modules
    assert module_size_violations(root, DEFAULT_MAX_LINES) == []
