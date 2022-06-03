pyenv: .python-version

.python-version: setup.cfg
	if [ -z "`pyenv virtualenvs | grep datajunction`" ]; then\
	    pyenv virtualenv datajunction;\
	fi
	if [ ! -f .python-version ]; then\
	    pyenv local datajunction;\
	fi
	pip install -e '.[testing]'
	touch .python-version

docker-build:
	docker build .
	docker compose build

docker-run:
	docker compose up

test: pyenv
	pytest --cov=src/datajunction -vv tests/ --doctest-modules src/datajunction --without-integration --without-slow-integration

integration: pyenv
	pytest --cov=src/datajunction -vv tests/ --doctest-modules src/datajunction --with-integration --with-slow-integration

clean:
	pyenv virtualenv-delete datajunction

spellcheck:
	codespell -L froms -S "*.json" src/datajunction docs/*rst tests templates

check:
	pre-commit run --all-files
