# Include if exists
-include .env


include ../../tools/common_makefile.mk


.PHONY: tests-cleanup
tests-cleanup: tests-tier1-clean


.PHONY: cleanup
cleanup: docker-cleanup test-cleanup


force-cleanup-git:
	sudo chown "$$(whoami)" -R . && git clean -d -x -f


# Create a development environment (start all the services required for testing the project)
devenv-core-d:
	docker-compose -f docker-compose.core.yml up --build --force-recreate -d

devenv-core:
	docker-compose -f docker-compose.core.yml up --build --force-recreate

devenv-core-status:
	docker-compose -f docker-compose.core.yml ps



PACKAGE_NAME = bi_legacy_test_bundle
