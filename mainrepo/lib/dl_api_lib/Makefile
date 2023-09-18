include ../../tools/common_makefile.mk


PACKAGE_NAME = dl_api_lib


.PHONY: test
test:
	cd ../../tools/local_dev && $(MAKE) test-bi-api


.PHONY: tests-cleanup
tests-cleanup: tests-tier1-clean


# Clean up test artifacts
.PHONY: cleanup
cleanup: docker-cleanup tests-cleanup


.PHONY: build-clean
build-clean: build-tier1-clean

