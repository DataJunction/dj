check:
	pdm run pre-commit run --all-files

test:
	pdm run pytest --cov=datajunction --cov-report=html -vv tests/ --doctest-modules datajunction --without-integration --without-slow-integration ${PYTEST_ARGS}

version:
	@poetry version $(v)
	@git add pyproject.toml
	@git commit -m "v$$(poetry version -s)"
	@git tag v$$(poetry version -s)
	@git push
	@git push --tags
	@poetry version

release:
	@poetry publish --build
