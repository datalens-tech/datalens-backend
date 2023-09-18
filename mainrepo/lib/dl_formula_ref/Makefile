include ../../tools/common_makefile.mk


PROJECT_DIR = dl_formula_ref


.PHONY: tests-cleanup
tests-cleanup: tests-tier1-clean


.PHONY: cleanup
cleanup: docker-cleanup test-cleanup


# Generate dl_formula_ref.pot template file for translations
.PHONY: l10n-template
l10n-template:
	pybabel extract --no-location -F babel.ini -o ${PROJECT_DIR}/locales/dl_formula_ref.pot ${PROJECT_DIR}/


.PHONY: update-po-domain-formula-ref
update-po-domain-formula-ref:
	DOMAIN_NAME=dl_formula_ref PATH_MASK="formula_ref/" make update-po-domain


.PHONY: update-po
update-po:
	@echo "Running for 'formula_ref'"
	@make update-po-domain-formula-ref 2> /dev/null || true


.PHONY: translations-local
translations-local:
	ls -1 dl_formula_ref/locales/ \
	    | grep -v .pot \
	    | awk '{system("msgfmt ${PROJECT_DIR}/locales/"$$1"/LC_MESSAGES/dl_formula_ref.po -o ${PROJECT_DIR}/locales/"$$1"/LC_MESSAGES/dl_formula_ref.mo")}'


dl_formula_ref/locales/ru/LC_MESSAGES/dl_formula_ref.mo: dl_formula_ref/locales/ru/LC_MESSAGES/dl_formula_ref.po
	if which msgfmt >/dev/null; then make translations-local; else make translations-docker; fi


# Generate translation binary .mo files
.PHONY: translations
translations: dl_formula_ref/locales/ru/LC_MESSAGES/dl_formula_ref.mo


.PHONY: cloud-docs
cloud-docs: translations
	for locale in ru en; do locale_short="$$(printf "%s\n" "$$locale" | sed -r 's/_.*//')"; bi-formula-ref-doc generate --locale "$$locale" $${CLOUD_DOCS_REPO:-~/src/cloud/docs}/"$${locale_short}"/_api-ref/datalens/; done


.PHONY: test
test: translations
	cd ../../tools/local_dev && $(MAKE) test-bi-formula-ref


.PHONY: test_unit
test_unit:
	tox -e py39unit


# Create a development environment (start all the services required for testing the project)
devenv:
	docker-compose up --build


devenv-d:
	docker-compose up --build -d


VENV = venv-bi-formula-ref


.PHONY: init-venv
init-venv:
	@echo "Installing python dependencies..."
	@python3 -m venv $(VENV)
	@$(VENV)/bin/python -m pip install -Ue ".[develop]" -i https://pypi.yandex-team.ru/simple/

	@echo "Fixing sqlalchemy version..."
	@$(VENV)/bin/python -m pip uninstall sqlalchemy --yes
	@$(VENV)/bin/python -m pip install sqlalchemy==1.4.46

.PHONY: clean-venv
clean-venv:
	@echo "Cleaning python dependencies..."
	@rm -rf $(VENV)

.PHONY: generate-example-data
generate-example-data:
	@echo "Generating example_data..."
	@$(VENV)/bin/bi-formula-ref-exdata generate

	@echo "Formatting example_data..."
	@$(VENV)/bin/python -m json.tool dl_formula_ref/example_data.json >> dl_formula_ref/example_data_formatted.json
	@rm dl_formula_ref/example_data.json
	@mv dl_formula_ref/example_data_formatted.json dl_formula_ref/example_data.json

DOCS_SOURCE_PATH ?= docs-source

.PHONY: generate-docs-source
generate-docs-source:
	@echo "Generating en docs..."
	@$(VENV)/bin/bi-formula-ref-doc generate --locale en $(DOCS_SOURCE_PATH)/en/_api-ref/datalens

	@echo "Generating ru docs..."
	@$(VENV)/bin/bi-formula-ref-doc generate --locale ru $(DOCS_SOURCE_PATH)/ru/_api-ref/datalens


PACKAGE_NAME = dl_formula_ref
