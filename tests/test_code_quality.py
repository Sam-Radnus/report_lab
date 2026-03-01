"""
Checks that every Python file in the project (excluding venv/) is free of
linting errors (via ruff) and syntax errors (via ast.parse).
"""
import ast
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

EXCLUDED_DIRS = {"venv", "__pycache__", ".ruff_cache", ".mypy_cache", ".pytest_cache"}


def _ruff_executable() -> str:
    # Prefer ruff on PATH (works when venv is activated), then fall back to the
    # project venv, then the venv next to sys.executable.
    if found := shutil.which("ruff"):
        return found
    for candidate in [
        ROOT / "venv" / "bin" / "ruff",
        Path(sys.executable).parent / "ruff",
    ]:
        if candidate.exists():
            return str(candidate)
    raise FileNotFoundError(
        "ruff not found. Activate the project venv or install ruff."
    )


def _project_python_files() -> list[Path]:
    return [
        p for p in ROOT.rglob("*.py")
        if not any(part in EXCLUDED_DIRS for part in p.parts)
    ]


def test_ruff_no_linting_errors():
    """All project Python files pass ruff linting."""
    result = subprocess.run(
        [_ruff_executable(), "check", "--extend-exclude", "venv", "."],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Ruff found linting errors:\n{result.stdout}"


def test_no_syntax_errors():
    """All project Python files parse without syntax errors."""
    errors = []
    for path in _project_python_files():
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            errors.append(f"{path.relative_to(ROOT)}: {exc}")

    assert not errors, "Syntax errors found:\n" + "\n".join(errors)
