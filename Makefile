.DEFAULT_GOAL := help
SHELL := /bin/bash

####

.PHONY: help
help:
	@echo "Use \`make <target>' where <target> is one of"
	@grep -E '^\.PHONY: [a-zA-Z_-]+ .*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = "(: |##)"}; {printf "\033[36m%-30s\033[0m %s\n", $$2, $$3}'

####
.PHONY: install ## Install dataflows-ckan and its dependencies for development.
install:
	poetry install

.PHONY: format ## Format code.
format:
	poetry run python -m black
	poetry run python -m isort

.PHONY: test ## Test and produce coverage data.
test:
	poetry run python -m pytest -vvv --cov dataflows_ckan --cov-report term-missing || echo 'TESTING FAILED'

.PHONY: version ## Echo the current package version.
version:
	poetry version

.PHONY: build ## Build sdist and wheel distributions of the package.
build:
	poetry build

.PHONY: publish ## Publish the package to PyPI.
publish:
	poetry config pypi-token.pypi ${PYPI_API_KEY}
	poetry publish

.PHONY: deps ## Show the package's dependency tree.
deps:
	poetry show --tree
