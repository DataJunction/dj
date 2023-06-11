pyenv: .python-version

.python-version: setup.cfg
	if [ -z "`pyenv virtualenvs | grep djqs`" ]; then\
	    pyenv virtualenv djqs;\
	fi
	if [ ! -f .python-version ]; then\
	    pyenv local djqs;\
	fi
	pip install -r requirements/test.txt
	touch .python-version

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

test: pyenv
	pytest --cov=djqs -vv tests/ --doctest-modules djqs --without-integration --without-slow-integration ${PYTEST_ARGS}

integration: pyenv
	pytest --cov=djqs -vv tests/ --doctest-modules djqs --with-integration --with-slow-integration

clean:
	pyenv virtualenv-delete djqs

spellcheck:
	codespell -L froms -S "*.json" djqs docs/*rst tests templates

check:
	pre-commit run --all-files
