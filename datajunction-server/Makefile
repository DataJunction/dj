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
	pdm run pytest -n auto --cov=datajunction_server -vv tests/ --doctest-modules datajunction_server --without-integration --without-slow-integration ${PYTEST_ARGS}

integration:
	pdm run pytest --cov=dj -vv tests/ --doctest-modules datajunction_server --with-integration --with-slow-integration

clean:
	pyenv virtualenv-delete dj

spellcheck:
	codespell -L froms -S "*.json" dj docs/*rst tests templates

check:
	pdm run pre-commit run --all-files

lint:
	make check

dev-release:
	hatch version dev
	hatch build
	hatch publish
