.PHONY: install test build clean publish

install:
	uv venv
	uv pip install -e ".[dev]"

test:
	uv run pytest

typecheck:
	uv run mypy src

build: clean
	uv build

clean:
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info

publish: build
	uv publish

publish-test: build
	uv publish --publish-url https://test.pypi.org/legacy/
