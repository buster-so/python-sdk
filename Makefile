.PHONY: install test build clean publish

install:
	uv venv
	uv pip install -e ".[dev]"

test:
	uv run pytest

typecheck:
	uv run mypy src

lint:
	uv run ruff format src tests
	uv run ruff check --fix src tests

format:
	uv run ruff format src tests

build: clean
	uv build

clean:
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info

publish: build
	uv publish --token ${{ secrets.PYPI_TOKEN }}

publish-test: build
	uv publish --publish-url https://test.pypi.org/legacy/

version-dry-run:
	uv run semantic-release version --print

all:
	make format
	make build
	make typecheck
	make lint
	make test
	