PYTEST_FLAGS=


.PHONY: setup
setup:
	pip install -U pip
	pip install -r requirements/test.txt
	pre-commit install

.PHONY: lint
lint: format
	mypy platform_neuro_flow_api tests --show-error-codes

.PHONY: format
format:
ifdef CI
	pre-commit run --all-files --show-diff-on-failure
else
	pre-commit run --all-files
endif

.PHONY: test_unit
test_unit:
	pytest -vv --cov=platform_neuro_flow_api --cov-report xml:.coverage-unit.xml tests/unit

.PHONY: test_integration
test_integration:
	pytest -vv --maxfail=3 --cov=platform_neuro_flow_api --cov-report xml:.coverage-integration.xml tests/integration

.PHONY: docker_build
docker_build:
	pip install build
	python -m build
	docker build -t platformneuroflowapi:latest .
