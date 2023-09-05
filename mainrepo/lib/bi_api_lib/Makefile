include ../../tools/common_makefile.mk


PACKAGE_NAME = bi_api_lib


.PHONY: test
test:
	cd ../../tools/local_dev && $(MAKE) test-bi-api


.PHONY: tests-cleanup
tests-cleanup: tests-tier1-clean


# Clean up test artifacts
.PHONY: cleanup
cleanup: docker-cleanup tests-cleanup


# Create a development environment (start all the services required for bi-api)
.PHONY: devenv-d
devenv-d:
	docker-compose -f docker-compose.yml up --build --force-recreate -d

.PHONY: devenv
devenv:
	docker-compose -f docker-compose.yml up --build --force-recreate

devenv-status:
	docker-compose -f docker-compose.yml ps



.PHONY: build-clean
build-clean: build-tier1-clean

