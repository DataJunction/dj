check:
	pdm run pre-commit run --all-files

lint:
	make check

test:
	pdm run pytest --cov=datajunction --cov-report=html -vv tests/ --doctest-modules datajunction --without-integration --without-slow-integration ${PYTEST_ARGS}

dev-release:
	hatch version dev
	hatch build
	hatch publish
