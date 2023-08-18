include ../../tools/common_makefile.mk

TOOLS_LOCAL_DEV = ../../tools/local_dev
DOCKER_ENV = $(TOOLS_LOCAL_DEV)/dl-docker-env.bash

.PHONY: devenv
devenv:
	docker-compose up --build

.PHONY: remake-devenv
remake-devenv:
	docker-compose stop
	docker-compose rm -fv us us-dc
	docker-compose build --pull
	docker-compose up -d

.PHONY: acceptance-tests-int-preprod
acceptance-tests-int-preprod:
	$(DOCKER_ENV) run --set-pwd -e DL_EXT_API_TESTS_RUN_AGAINST_INT_PREPROD=1 -- \
	 pytest -p no:warnings --maxfail=100 -v \
	 "bi_external_api_tests/int_preprod/test_acceptance_against_int_preprod.py"
