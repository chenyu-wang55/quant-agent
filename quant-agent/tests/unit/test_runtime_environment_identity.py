from importlib.metadata import distribution
from pathlib import Path

from packaging.specifiers import SpecifierSet

from scripts import preflight


def test_locked_packages_support_both_python_matrix_versions():
    lock = Path(__file__).resolve().parents[2] / "requirements.lock"
    incompatible = []
    for name in preflight._locked_versions(lock):
        constraint = distribution(name).metadata.get("Requires-Python")
        if constraint and any(version not in SpecifierSet(constraint) for version in ("3.11", "3.13")):
            incompatible.append((name, constraint))
    assert incompatible == [], "A common dependency lock must support both Python matrix versions"


def test_another_venv_with_same_python_binary_is_not_repository_environment(tmp_path, monkeypatch):
    # macOS venv symlinks can resolve to exactly the same framework interpreter.
    repo = tmp_path / "repo"
    expected = repo / ".venv/bin"
    expected.mkdir(parents=True)
    interpreter = tmp_path / "shared-python"
    interpreter.touch()
    (expected / "python").symlink_to(interpreter)
    monkeypatch.setattr(preflight.sys, "executable", str(interpreter))
    monkeypatch.setattr(preflight.sys, "base_prefix", str(tmp_path / "base"))
    monkeypatch.setattr(preflight.sys, "prefix", str(tmp_path / "other-venv"))
    assert preflight.check_python_environment(repo)[0].status == "fail"
    monkeypatch.setattr(preflight.sys, "prefix", str(repo / ".venv"))
    assert preflight.check_python_environment(repo)[0].status == "pass"
