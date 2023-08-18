include ../../tools/common_makefile.mk


LOCAL_LIBS := lib/bi_testing lib/bi_cloud_integration lib/bi_configs lib/bi_utils lib/bi_constants lib/bi_core lib/bi_sqlalchemy_chyt lib/clickhouse-sqlalchemy lib/bi_sqlalchemy_clickhouse lib/bi_sqlalchemy_common lib/bi_app_tools


.PHONY: build-prepare test
test:
	docker-compose up -d --build
	docker-compose run --rm -e SKIP_DEPS_INSTALL=1 -v /tmp:/tmp -w /data/app/bi_billing pi -- tox


.PHONY: tests-cleanup
tests-cleanup: tests-tier1-clean


# Clean up test artifacts
.PHONY: cleanup
cleanup: docker-cleanup tests-cleanup


# Create a development environment (start all the services required for bi-api)
.PHONY: devenv
devenv: gen-parser
	docker-compose up --build


.PHONY: devenv-d
devenv-d:
	docker-compose up --build -d
