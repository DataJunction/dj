check:
	pdm run pre-commit run --all-files

lint:
	make check

test:
	pdm run pytest -n auto --cov=datajunction -vv tests/ --doctest-modules datajunction --without-integration --without-slow-integration ${PYTEST_ARGS}

dev-release:
	hatch version dev
	hatch build
	hatch publish
