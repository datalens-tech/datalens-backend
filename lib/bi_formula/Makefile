include ../../tools/common_makefile.mk


.PHONY: tests-cleanup
tests-cleanup: tests-tier1-clean


.PHONY: cleanup
cleanup: docker-cleanup test-cleanup


.PHONY: test
test:
	cd ../../tools/local_dev && $(MAKE) test-bi-formula


.PHONY: test_unit
test_unit:
	tox -e py39unit


# Create a development environment (start all the services required for testing the project)
devenv:
	docker-compose up --build


devenv-d:
	docker-compose up --build -d
