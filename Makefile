pyenv: .python-version

.python-version: setup.cfg
	if [ -z "`pyenv virtualenvs | grep dj`" ]; then\
	    pyenv virtualenv dj;\
	fi
	if [ ! -f .python-version ]; then\
	    pyenv local dj;\
	fi
	pip install -e '.[testing]'
	touch .python-version

docker-build:
	docker build .
	docker compose build

docker-run:
	docker compose up

docker-run-with-druid:
	docker compose -f docker-compose.yml -f docker-compose-druid.yml up

test: pyenv
	pytest --cov=src/dj -vv tests/ --doctest-modules src/dj --without-integration --without-slow-integration

integration: pyenv
	pytest --cov=src/dj -vv tests/ --doctest-modules src/dj --with-integration --with-slow-integration

clean:
	pyenv virtualenv-delete dj

spellcheck:
	codespell -L froms -S "*.json" src/dj docs/*rst tests templates

check:
	pre-commit run --all-files
