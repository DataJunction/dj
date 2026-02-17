setup:
	uv sync --extra mcp --group test --extra pandas
check:
	uv run pre-commit run --all-files

lint:
	make check

test:
	uv run pytest -n auto --cov=datajunction --cov-report term-missing -vv tests/ --doctest-modules datajunction --without-integration --without-slow-integration ${PYTEST_ARGS}

dev-release:
	hatch version dev
	hatch build
	hatch publish
