setup:
	uv sync --group test

docker-build:
	docker build .
	docker compose build

docker-run:
	docker compose up

docker-run-with-postgres:
	docker compose -f docker-compose.yml -f docker-compose.postgres.yml up

docker-run-with-druid:
	docker compose -f docker-compose.yml -f docker-compose.druid.yml up

docker-run-with-cockroachdb:
	docker compose -f docker-compose.yml -f docker-compose.cockroachdb.yml up

test:
	uv run pytest --cov=djqs -vv tests/ --cov-report term-missing --doctest-modules djqs --without-integration --without-slow-integration ${PYTEST_ARGS}

integration:
	uv run pytest --cov=djqs -vv tests/ --cov-report term-missing --doctest-modules djqs --with-integration --with-slow-integration

clean:
	rm -rf .venv

spellcheck:
	codespell -L froms -S "*.json" djqs docs/*rst tests templates

check:
	uv run pre-commit run --all-files

lint:
	make check

dev-release:
	hatch version dev
	hatch build
	hatch publish
