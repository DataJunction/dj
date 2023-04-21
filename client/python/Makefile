check:
	poetry run pre-commit run --all-files

test:
	poetry run pytest tests/ ${PYTEST_ARGS}

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
