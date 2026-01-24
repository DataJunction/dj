setup:
	uv sync --group test

docker-build:
	docker build .

test:
	uv run pytest --cov=datajunction_reflection -vv tests/ --doctest-modules datajunction_reflection --without-integration --without-slow-integration ${PYTEST_ARGS}

integration:
	uv run pytest --cov=datajunction_reflection -vv tests/ --doctest-modules datajunction_reflection --with-integration --with-slow-integration

clean:
	rm -rf .venv

spellcheck:
	codespell -L froms -S "*.json" datajunction_reflection docs/*rst tests templates

check:
	uv run pre-commit run --all-files

lint:
	make check

dev-release:
	hatch version dev
	hatch build
	hatch publish
