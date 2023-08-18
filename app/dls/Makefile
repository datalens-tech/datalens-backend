include ../../tools/common_makefile.mk


.PHONY: secrets-update-force
secrets-update-force:
	yav-deploy --force --configs-path ./.secrets/ --deploy-root ./


.env_int_beta: .secrets/defaults.conf .secrets/templates/.env_int_beta.j2
	make secrets-update-force


.env_int_prod: .secrets/defaults.conf .secrets/templates/.env_int_prod.j2
	make secrets-update-force


.env_ext_testing: .secrets/defaults.conf .secrets/templates/.env_ext_testing.j2
	make secrets-update-force


.env_ext_production: .secrets/defaults.conf .secrets/templates/.env_ext_production.j2
	make secrets-update-force


secrets-update: .env_int_beta .env_int_prod .env_ext_testing .env_ext_production
