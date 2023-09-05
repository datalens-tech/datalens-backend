# Include if exists
-include .env


include ../../tools/common_makefile.mk


PACKAGE_NAME = bi_connector_oracle


devenv-d:
	docker-compose -f docker-compose.yml up --build --force-recreate -d

devenv:
	docker-compose -f docker-compose.yml up --build --force-recreate

devenv-status:
	docker-compose -f docker-compose.yml ps
