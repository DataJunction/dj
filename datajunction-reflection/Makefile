pyenv: .python-version

.python-version: setup.cfg
	if [ -z "`pyenv virtualenvs | grep djrs`" ]; then\
	    pyenv virtualenv 3.10 djrs;\
	fi
	if [ ! -f .python-version ]; then\
	    pyenv local djrs;\
	fi
	pdm install
	touch .python-version

docker-build:
	docker build .

test: pyenv
	pdm run pytest --cov=datajunction_reflection -vv tests/ --doctest-modules datajunction_reflection --without-integration --without-slow-integration ${PYTEST_ARGS}

integration: pyenv
	pdm run pytest --cov=datajunction_reflection -vv tests/ --doctest-modules datajunction_reflection --with-integration --with-slow-integration

clean:
	pyenv virtualenv-delete djrs

spellcheck:
	codespell -L froms -S "*.json" datajunction_reflection docs/*rst tests templates

check:
	pdm run pre-commit run --all-files

lint:
	make check

dev-release:
	hatch version dev
	hatch build
	hatch publish
