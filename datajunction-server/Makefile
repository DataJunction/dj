setup:
	uv sync --group test --extra transpilation

docker-build:
	docker build .
	docker compose build

docker-run:
	docker compose up

test:
	uv run pytest -n 4 --dist=loadscope --cov-fail-under=100 --cov=datajunction_server --cov-report term-missing -vv tests/ --doctest-modules datajunction_server --without-integration --without-slow-integration --ignore=datajunction_server/alembic/env.py ${PYTEST_ARGS}

integration:
	uv run pytest --cov=dj -vv tests/ --doctest-modules datajunction_server --with-integration --with-slow-integration --ignore=datajunction_server/alembic/env.py ${PYTEST_ARGS}

clean:
	rm -rf .venv

spellcheck:
	codespell -L froms -S "*.json" dj docs/*rst tests templates

check:
	uv run pre-commit run --all-files

lint:
	make check

dev-release:
	hatch version dev
	hatch build
	hatch publish
