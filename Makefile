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
	pdm run pytest --cov=dj --cov-report=html -vv tests/ --doctest-modules dj --without-integration --without-slow-integration ${PYTEST_ARGS}

integration:
	pdm run pytest --cov=dj -vv tests/ --doctest-modules dj --with-integration --with-slow-integration

clean:
	pyenv virtualenv-delete dj

spellcheck:
	codespell -L froms -S "*.json" dj docs/*rst tests templates

check:
	pdm run pre-commit run --all-files

version:
	@hatch version $(v)
	@git add __about__.py
	@git commit -m "Bumping to v$$(hatch version)"
	@git tag v$$(hatch version)
	@git push
	@git push --tags
	@hatch version

release:
	@hatch publish --build
