PYTEST_FLAGS=


setup:
	pip install -U pip
	pip install -r requirements/test.txt
	pre-commit install

lint: format
	mypy platform_neuro_flow_api tests --show-error-codes

format:
ifdef CI
	pre-commit run --all-files --show-diff-on-failure
else
	pre-commit run --all-files
endif

test_unit:
	pytest -vv --cov=platform_neuro_flow_api --cov-report xml:.coverage-unit.xml tests/unit

test_integration:
	pytest -vv --maxfail=3 --cov=platform_neuro_flow_api --cov-report xml:.coverage-integration.xml tests/integration

docker_build:
	pip install build
	python -m build
	docker build -t platformneuroflowapi:latest .
