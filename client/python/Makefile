check:
	pdm run pre-commit run --all-files

test:
	pdm run pytest --cov=datajunction --cov-report=html -vv tests/ --doctest-modules datajunction --without-integration --without-slow-integration ${PYTEST_ARGS}

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
