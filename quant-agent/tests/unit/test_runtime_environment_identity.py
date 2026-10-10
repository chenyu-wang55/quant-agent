from scripts import preflight


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
