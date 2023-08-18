
PYPI_REPO ?= yandex

.PHONY: build-cleanup build-pypi upload-pypi build upload cleanup secrets-update-force test

# Clean build artifacts
build-cleanup:
	rm -rf build dist

# Build Python package
build-pypi: build-cleanup
	python3 setup.py sdist bdist_wheel

# Build Python package
upload-pypi:
	twine upload -r ${PYPI_REPO} dist/*

build: build-pypi
upload: upload-pypi
cleanup: build-cleanup

secrets-update-force:
	yav-deploy --force --configs-path ./.secrets/ --deploy-root ./

.env: .secrets/defaults.conf .secrets/templates/env.j2
	make secrets-update-force

secrets-update: .env

test: secrets-update
	set -a && . ./.env && py.test -vvs ./sqlalchemy_metrika_api_tests
