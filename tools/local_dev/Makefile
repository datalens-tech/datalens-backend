-include .env

DEV_MACHINE_PKG_ROOT ?= ../..
DEV_MACHINE_UPLOAD_DEBUG ?= 0
TOOLS_VENV = .tools_venv
TOOLS_PY = $(TOOLS_VENV)/bin/python

export DEV_MACHINE
export DEV_MACHINE_UPLOAD_DEBUG
export DEV_MACHINE_PKG_ROOT
export TOOLS_VENV

.PHONY: docker-remote
docker-remote: secrets-update
	docker-compose run -e SKIP_DEPS_INSTALL=1 --rm pi bash

.PHONY: update-requirements
update-requirements:
	cd requirements; bash update_requirements.sh

.PHONY: docker-reinstall
docker-reinstall: secrets-update update-requirements
	docker-compose stop pi
	docker-compose rm -f pi
	docker rm $$(docker stop $$(docker ps -a -q --filter volume=local_dev_pi-venv --format="{{.ID}}")) || echo "nothing to stop"
	docker volume rm -f local_dev_pi-venv
	docker-compose build --pull pi
	docker-compose run --rm pi


# Run bash in the remote python container. Can be used to run tests/checks manually
.PHONY: remote-shell
remote-shell:
	docker-compose stop pi
	docker-compose run --rm -it --env SKIP_DEPS_INSTALL=1 pi bash


# Request secrets from vault and store it to env file
secrets/private/base_secrets.env: secrets/conf/defaults.conf secrets/conf/templates/base_secrets.env.j2
	yav-deploy --force --deploy-root ./secrets/private --configs-path ./secrets/conf


.PHONY: secrets-update
secrets-update: secrets/private/base_secrets.env


.PHONY: secrets-cleanup
secrets-cleanup:
	rm -rf .secrets/private/*


# Request secrets, disregard the current state.
.PHONY: secrets-update-force
secrets-update-force:
	make secrets-cleanup
	make secrets-update

#
# Python virtualenv for tools scripts
#

$(TOOLS_VENV): tools_venv/requirements.txt tools_venv/ensure_build_venv.sh
	bash tools_venv/ensure_build_venv.sh

tools-venv: $(TOOLS_VENV)

tools-venv-cleanup:
	rm -rf $(TOOLS_VENV)

tools-venv-force: tools-venv-cleanup tools-venv

#
# PyCharm files management
#

pycharm-prereq: export PYTHON_PATH = .
pycharm-prereq: tools-venv update-requirements

pycharm-uploads: pycharm-prereq
	$(TOOLS_PY) -m pycharm.ensure_uploads_config

pycharm-proj-struct: pycharm-prereq
	$(TOOLS_PY) -m pycharm.ensure_project_structure

pycharm: pycharm-uploads pycharm-proj-struct

upload-dm: pycharm-prereq
	$(TOOLS_PY) -m pycharm.upload_to_dev_machine

download-ea-docs:
	scp -r ${DEV_MACHINE}:mirror/docs/external_api_md/* ../../../docs/external_api/
