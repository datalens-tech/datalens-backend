include ../../tools/common_makefile.mk


PROJECT_DIR = bi_formula_ref


.PHONY: tests-cleanup
tests-cleanup: tests-tier1-clean


.PHONY: cleanup
cleanup: docker-cleanup test-cleanup


# Generate bi_formula_ref.pot template file for translations
.PHONY: l10n-template
l10n-template:
	pybabel extract --no-location -F babel.ini -o ${PROJECT_DIR}/locales/bi_formula_ref.pot ${PROJECT_DIR}/


# Merge updated bi_formula_ref.pot template file into .po files
# TODO?: non-phony sub-commands?
.PHONY: update-po
update-po: l10n-template
	bi-formula-ref-doc locales \
	    | awk '{system("msgmerge ${PROJECT_DIR}/locales/"$$1"/LC_MESSAGES/bi_formula_ref.po ${PROJECT_DIR}/locales/bi_formula_ref.pot -o ${PROJECT_DIR}/locales/"$$1"/LC_MESSAGES/bi_formula_ref.po")}'


.PHONY: translations-docker
translations-docker:
	docker build --file Dockerfile.translations --tag bi_formula_ref_tst .
	docker run --rm --name bi_formula_ref_make_translations --volume "$$(pwd):/data" --volume "$$(pwd)/docker-compose/translations/entrypoint.sh:/entrypoint.sh" bi_formula_ref_tst


.PHONY: translations-local
translations-local:
	ls -1 bi_formula_ref/locales/ \
	    | grep -v .pot \
	    | awk '{system("msgfmt ${PROJECT_DIR}/locales/"$$1"/LC_MESSAGES/bi_formula_ref.po -o ${PROJECT_DIR}/locales/"$$1"/LC_MESSAGES/bi_formula_ref.mo")}'


bi_formula_ref/locales/ru_RU/LC_MESSAGES/bi_formula_ref.mo: bi_formula_ref/locales/ru_RU/LC_MESSAGES/bi_formula_ref.po
	if which msgfmt >/dev/null; then make translations-local; else make translations-docker; fi


# Generate translation binary .mo files
.PHONY: translations
translations: bi_formula_ref/locales/ru_RU/LC_MESSAGES/bi_formula_ref.mo


.PHONY: cloud-docs
cloud-docs: translations
	for locale in ru_RU en_US; do locale_short="$$(printf "%s\n" "$$locale" | sed -r 's/_.*//')"; bi-formula-ref-doc generate --locale "$$locale" $${CLOUD_DOCS_REPO:-~/src/cloud/docs}/"$${locale_short}"/_api-ref/datalens/; done


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
	@$(VENV)/bin/python -m json.tool bi_formula_ref/example_data.json >> bi_formula_ref/example_data_formatted.json
	@rm bi_formula_ref/example_data.json
	@mv bi_formula_ref/example_data_formatted.json bi_formula_ref/example_data.json

DOCS_SOURCE_PATH ?= docs-source

.PHONY: generate-docs-source
generate-docs-source:
	@echo "Generating en_US docs..."
	@$(VENV)/bin/bi-formula-ref-doc generate --locale en_US $(DOCS_SOURCE_PATH)/en/_api-ref/datalens

	@echo "Generating ru_RU docs..."
	@$(VENV)/bin/bi-formula-ref-doc generate --locale ru_RU $(DOCS_SOURCE_PATH)/ru/_api-ref/datalens


PACKAGE_NAME = bi_formula_ref
