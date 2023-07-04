pyenv: .python-version

.python-version: setup.cfg
	if [ -z "`pyenv virtualenvs | grep djrs`" ]; then\
	    pyenv virtualenv 3.10 djrs;\
	fi
	if [ ! -f .python-version ]; then\
	    pyenv local djrs;\
	fi
	pdm install -G pandas
	touch .python-version

docker-build:
	docker build .

test: pyenv
	pdm run pytest --cov=djrs -vv tests/ --doctest-modules djrs --without-integration --without-slow-integration ${PYTEST_ARGS}

integration: pyenv
	pdm run pytest --cov=djrs -vv tests/ --doctest-modules djrs --with-integration --with-slow-integration

clean:
	pyenv virtualenv-delete djrs

spellcheck:
	codespell -L froms -S "*.json" djrs docs/*rst tests templates

check:
	pdm run pre-commit run --all-files
