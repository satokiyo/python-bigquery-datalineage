# for infra settings.
PROJECT_ID:=datalineage-demo
USER_ADDRESS:=<YOUR ADDRESS>
BILLING_ACCOUNT_ID:=<YOUR ACCOUNT_ID>
DATASET:=data_lineage_demo

PROJECT_EXIST_COUNT=$(shell gcloud projects list --filter=name:$(PROJECT_ID) --format="value(name)" | wc -l)

# for make run args.
PROJECT_NO=$(shell gcloud projects describe $(PROJECT_ID) --format='value(projectNumber)')
LOCATION:=us
table_id:=$(PROJECT_ID).$(DATASET).total_green_trips_22_21
FQN=bigquery:$(table_id)

.DEFAULT_GOAL:=help

.PHONY: build-infra
build-infra: ## build infra ## make build-infra
	$(MAKE) create-project
	$(MAKE) enable-billing-account
	$(MAKE) enable-api
	$(MAKE) add-iam-policy
	$(MAKE) create-bq-tables

.PHONY: clean-infra ## clean infra ## make clean-infra
clean-infra:
	$(MAKE) delete-project

.PHONY: run ## run python script ## make run
run:
	poetry run python src/main.py $(PROJECT_NO) $(LOCATION) $(FQN)


.PHONY: create-project
create-project:
	@if [ $(PROJECT_EXIST_COUNT) -gt 0 ]; then \
		echo project already exist!; \
	else \
		echo create project!; \
		gcloud projects create $(PROJECT_ID); \
		gcloud config set project $(PROJECT_ID); \
	fi

.PHONY: delete-project
delete-project:
	@if [ $(PROJECT_EXIST_COUNT) -gt 0 ]; then \
		echo delete project!; \
		gcloud projects delete $(PROJECT_ID); \
	else \
		echo project not found!; \
	fi

.PHONY: set-project
set-project: create-project
	gcloud config set project $(PROJECT_ID)

.PHONY: enable-api
enable-api: set-project
	gcloud services enable datacatalog.googleapis.com
	gcloud services enable bigquery.googleapis.com
	gcloud services enable datalineage.googleapis.com

.PHONY: add-iam-policy
add-iam-policy: set-project
	gcloud projects add-iam-policy-binding $(PROJECT_ID) --member user:$(USER_ADDRESS) --role roles/datacatalog.viewer
	gcloud projects add-iam-policy-binding $(PROJECT_ID) --member user:$(USER_ADDRESS) --role roles/datalineage.viewer
	gcloud projects add-iam-policy-binding $(PROJECT_ID) --member user:$(USER_ADDRESS) --role roles/bigquery.dataViewer

.PHONY: create-bq-tables
create-bq-tables: set-project
	bash create_tables.sh $(PROJECT_ID) $(DATASET)

.PHONY: enable-billing-account
enable-billing-account: set-project
	gcloud beta billing projects link $(PROJECT_ID) --billing-account $(BILLING_ACCOUNT_ID)

.PHONY: test
test: ## test package ## make test
	poetry run python -m pytest

.PHONY: test_tox
test_tox: ## test in multiple python versions ## make test_tox
	poetry run python -m tox

.PHONY: lint
lint: ## lint ## make lint
	@poetry run python -m flake8 ./src/ ./tests/ --exclude __init__.py --ignore E402,E501,W503

.PHONY: format
format: ## format ## make format
	@poetry run python -m black --diff --color ./src/ ./tests/
	@poetry run python -m black ./src/ ./tests/

.PHONY: isort
isort: ## isort ## make isort
	@poetry run python -m isort ./src/ ./tests/

.PHONY: type_check
type_check: ## type_check ## make type_check
	@poetry run python -m mypy ./src/ ./tests/

.PHONY: static_test
static_test: ## static_test ## make static_test
	@echo start lint
	-@$(MAKE) lint
	@echo start format
	-@$(MAKE) format
	@echo start isort
	-@$(MAKE) isort
	@echo start type_check
	-@$(MAKE) type_check
	@echo finish

help: ## print this message
	@echo "Example operations by makefile."
	@echo ""
	@echo "Usage: make SUB_COMMAND argument_name=argument_value"
	@echo ""
	@echo "Command list:"
	@echo ""
	@printf "\033[36m%-30s\033[0m %-50s %s\n" "[Sub command]" "[Description]" "[Example]"
	@grep -E '^[/a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | perl -pe 's%^([/a-zA-Z_-]+):.*?(##)%$$1 $$2%' | awk -F " *?## *?" '{printf "\033[36m%-30s\033[0m %-50s %s\n", $$1, $$2, $$3}'