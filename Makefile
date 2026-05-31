.PHONY: test test-unit test-integration fmt check build install docs-serve docs-build clean

install:
	uv sync --all-extras --group dev

fmt:
	uv run ruff format .
	uv run ruff check --fix .

check:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy omni_box

test-unit:
	uv run pytest -m unit

test-integration:
	uv run pytest -m integration

test:
	uv run pytest -m unit --cov=omni_box --cov-report=term --cov-fail-under=90 --cov-report=xml:coverage.xml

build:
	uv build

docs-serve:
	uv run python -c "import shutil; shutil.copy('CHANGELOG.md', 'docs/changelog.md')"
	uv run --no-dev --group docs zensical serve

docs-build:
	uv run python -c "import shutil; shutil.copy('CHANGELOG.md', 'docs/changelog.md')"
	uv run --no-dev --group docs zensical build --clean

clean:
	uv run python -c "import shutil, os, pathlib; [shutil.rmtree(p, ignore_errors=True) for p in ['.pytest_cache', '.mypy_cache', '.ruff_cache', 'dist', 'build', 'site'] if os.path.exists(p)]; [os.remove(p) for p in ['.coverage', 'coverage.xml'] if os.path.exists(p)]; [shutil.rmtree(p, ignore_errors=True) for p in pathlib.Path('.').rglob('__pycache__')]"
