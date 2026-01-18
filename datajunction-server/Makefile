pyenv: .python-version

.python-version: setup.cfg
	if [ -z "`pyenv virtualenvs | grep dj`" ]; then\
	    pyenv virtualenv dj;\
	fi
	if [ ! -f .python-version ]; then\
	    pyenv local dj;\
	fi
	pip install -r requirements/test.txt
	touch .python-version

docker-build:
	docker build .
	docker compose build

docker-run:
	docker compose up

test:
	pdm run pytest -n 4 --dist=loadscope --cov-fail-under=100 --cov=datajunction_server --cov-report term-missing -vv tests/ --doctest-modules datajunction_server --without-integration --without-slow-integration --ignore=datajunction_server/alembic/env.py ${PYTEST_ARGS}

integration:
	pdm run pytest --cov=dj -vv tests/ --doctest-modules datajunction_server --with-integration --with-slow-integration --ignore=datajunction_server/alembic/env.py ${PYTEST_ARGS}

clean:
	pyenv virtualenv-delete dj

spellcheck:
	codespell -L froms -S "*.json" dj docs/*rst tests templates

check:
	pdm run pre-commit run --all-files
	$(MAKE) check-schemas

lint:
	make check

generate-schemas:
	pdm run python scripts/generate-openapi.py -o ../openapi.json
	pdm run python scripts/generate-graphql.py -o datajunction_server/api/graphql/

check-schemas:
	$(MAKE) generate-schemas
	@echo "Checking for uncommitted schema changes..."
	git diff --exit-code ../openapi.json datajunction_server/api/graphql/schema.graphql || \
		(echo "ERROR: Schema files are out of date. Run 'make generate-schemas' and commit the changes." && exit 1)

dev-release:
	hatch version dev
	hatch build
	hatch publish
