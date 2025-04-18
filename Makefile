.PHONY: all test clean
all test clean:

.PHONY: venv
venv:
	poetry lock
	poetry install --with dev;

.PHONY: build
build: venv poetry-plugins

.PHONY: poetry-plugins
poetry-plugins:
	poetry self add "poetry-dynamic-versioning[plugin]"; \
    poetry self add "poetry-plugin-export";

.PHONY: setup
setup: venv
	poetry run pre-commit install;

.PHONY: lint
lint: format
	poetry run mypy platform_neuro_flow_api tests --show-error-codes

.PHONY: format
format:
ifdef CI
	poetry run pre-commit run --all-files --show-diff-on-failure
else
	poetry run pre-commit run --all-files
endif

.PHONY: test_unit
test_unit:
	poetry run pytest -vv --cov-config=pyproject.toml --cov=platform_neuro_flow_api --cov-report xml:.coverage-unit.xml tests/unit

.PHONY: test_integration
test_integration:
	poetry run pytest -vv --maxfail=3 --cov-config=pyproject.toml --cov=platform_neuro_flow_api --cov-report xml:.coverage-integration.xml tests/integration

.PHONY: docker_build
docker_build: .python-version dist
	PY_VERSION=$$(cat .python-version) && \
	docker build \
		-t platformneuroflowapi:latest \
		--build-arg PY_VERSION=$$PY_VERSION \
		.

.python-version:
	@echo "Error: .python-version file is missing!" && exit 1

.PHONY: dist
dist: build
	rm -rf build dist; \
	poetry export -f requirements.txt --without-hashes -o requirements.txt; \
	poetry build -f wheel;
