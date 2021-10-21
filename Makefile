AWS_ACCOUNT_ID ?= 771188043543
AWS_REGION ?= us-east-1

AZURE_RG_NAME ?= dev
AZURE_ACR_NAME ?= crc570d91c95c6aac0ea80afb1019a0c6f

ARTIFACTORY_DOCKER_REPO ?= neuro-docker-local-public.jfrog.io
ARTIFACTORY_HELM_REPO ?= https://neuro.jfrog.io/artifactory/helm-local-public
ARTIFACTORY_HELM_VIRTUAL_REPO ?= https://neuro.jfrog.io/artifactory/helm-virtual-public

IMAGE_REPO_gke         = $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)
IMAGE_REPO_aws         = $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com
IMAGE_REPO_azure       = $(AZURE_ACR_NAME).azurecr.io
IMAGE_REPO_artifactory = $(ARTIFACTORY_DOCKER_REPO)

IMAGE_REGISTRY ?= artifactory

IMAGE_NAME = platformneuroflowapi
IMAGE_REPO = $(IMAGE_REPO_$(IMAGE_REGISTRY))/$(IMAGE_NAME)

HELM_CHART = platform-neuro-flow-api

TAG ?= latest

PYTEST_FLAGS=

setup:
	pip install -U pip
	pip install -r requirements/test.txt
	pre-commit install

lint: format
	mypy platform_neuro_flow_api tests

format:
ifdef CI_LINT_RUN
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
	docker build -t $(IMAGE_NAME):latest .

docker_push: docker_build
	docker tag $(IMAGE_NAME):latest $(IMAGE_REPO):$(TAG)
	docker push $(IMAGE_REPO):$(TAG)

	docker tag $(IMAGE_NAME):latest $(IMAGE_REPO):latest
	docker push $(IMAGE_REPO):latest

gke_login: docker_build
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet components update --version 204.0.0
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet components update --version 204.0.0 kubectl
	sudo chown circleci:circleci -R $$HOME
	@echo $(GKE_ACCT_AUTH) | base64 --decode > $(HOME)//gcloud-service-key.json
	gcloud auth activate-service-account --key-file $(HOME)/gcloud-service-key.json
	gcloud config set project $(GKE_PROJECT_ID)
	gcloud --quiet config set container/cluster $(GKE_CLUSTER_NAME)
	gcloud config set $(SET_CLUSTER_ZONE_REGION)
	gcloud auth configure-docker

aws_k8s_login:
	aws eks --region $(AWS_REGION) update-kubeconfig --name $(CLUSTER_NAME)

azure_k8s_login:
	az aks get-credentials --resource-group $(AZURE_RG_NAME) --name $(CLUSTER_NAME)

helm_install:
	curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get | bash -s -- -v $(HELM_VERSION)
	helm plugin install https://github.com/belitre/helm-push-artifactory-plugin --version 1.0.2
	@helm repo add neuro $(ARTIFACTORY_HELM_VIRTUAL_REPO) \
		--username ${ARTIFACTORY_USERNAME} \
		--password ${ARTIFACTORY_PASSWORD}
	helm repo update

_helm_fetch:
	rm -rf temp_deploy
	mkdir -p temp_deploy/$(HELM_CHART)
	cp -Rf deploy/$(HELM_CHART) temp_deploy/
	find temp_deploy/$(HELM_CHART) -type f -name 'values*' -delete
	helm dependency update temp_deploy/$(HELM_CHART)

_helm_expand_vars:
	export IMAGE_REPO=$(IMAGE_REPO); \
	export IMAGE_TAG=$(TAG); \
	export DOCKER_SERVER=$(ARTIFACTORY_DOCKER_REPO); \
	cat deploy/$(HELM_CHART)/values-template.yaml | envsubst > temp_deploy/$(HELM_CHART)/values.yaml

helm_deploy: _helm_fetch _helm_expand_vars
	helm upgrade $(HELM_CHART) temp_deploy/$(HELM_CHART) \
		-f deploy/$(HELM_CHART)/values-$(HELM_ENV).yaml \
		--set "image.repository=$(IMAGE_REPO)" \
		--set "postgres-db-init.migrations.image.repository=$(IMAGE_REPO)" \
		--namespace platform --install --wait --timeout 600s

artifactory_helm_push: _helm_fetch _helm_expand_vars
	helm package --app-version=$(TAG) --version=$(TAG) temp_deploy/$(HELM_CHART)
	helm push-artifactory $(HELM_CHART)-$(TAG).tgz $(ARTIFACTORY_HELM_REPO) \
		--username $(ARTIFACTORY_USERNAME) \
		--password $(ARTIFACTORY_PASSWORD)

artifactory_helm_deploy:
	helm upgrade $(HELM_CHART) neuro/$(HELM_CHART) \
		-f deploy/$(HELM_CHART)/values-$(HELM_ENV).yaml \
		--set "image.repository=$(IMAGE_REPO)" \
		--set "postgres-db-init.migrations.image.repository=$(IMAGE_REPO)" \
		--version $(TAG) --namespace platform --install --wait --timeout 600s
