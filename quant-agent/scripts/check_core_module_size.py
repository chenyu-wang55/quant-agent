from __future__ import annotations

import argparse
from pathlib import Path

DEFAULT_MAX_LINES = 850
TARGET_PATTERNS = (
    "apps/api/dependencies.py",
    "apps/api/state_*.py",
    "apps/worker/*.py",
    "apps/dashboard/main.py",
)


def core_modules(root: Path) -> list[Path]:
    files: set[Path] = set()
    for pattern in TARGET_PATTERNS:
        files.update(path for path in root.glob(pattern) if path.name != "__init__.py")
    return sorted(files)


def module_size_violations(root: Path, max_lines: int = DEFAULT_MAX_LINES) -> list[tuple[Path, int]]:
    if max_lines < 1:
        raise ValueError("max_lines must be positive")
    violations: list[tuple[Path, int]] = []
    for path in core_modules(root):
        line_count = len(path.read_text(encoding="utf-8").splitlines())
        if line_count > max_lines:
            violations.append((path, line_count))
    return violations


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prevent the API state, worker orchestration, and dashboard entrypoint from regrowing."
    )
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--max-lines", type=int, default=DEFAULT_MAX_LINES)
    args = parser.parse_args()
    root = args.root.expanduser().resolve()
    files = core_modules(root)
    if not files:
        print(f"no core modules found under {root}")
        return 2
    violations = module_size_violations(root, args.max_lines)
    if violations:
        for path, line_count in violations:
            print(f"{path.relative_to(root)}: {line_count} lines (limit {args.max_lines})")
        return 1
    print(f"core module size check passed: {len(files)} files, limit {args.max_lines} lines")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
