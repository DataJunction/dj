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

docker-run-with-druid:
	docker compose -f docker-compose.yml -f docker-compose-druid.yml up

test: pyenv
	pytest --cov=dj -vv tests/ --doctest-modules dj --without-integration --without-slow-integration

integration: pyenv
	pytest --cov=dj -vv tests/ --doctest-modules dj --with-integration --with-slow-integration

clean:
	pyenv virtualenv-delete dj

spellcheck:
	codespell -L froms -S "*.json" dj docs/*rst tests templates

check:
	pre-commit run --all-files
