-include .env
export

.PHONY: push_gh_sync
push_gh_sync:
	bash push_gh_sync.sh


.PHONY: build
build:
	docker-compose build --pull pi2

.PHONY: remote
remote: secrets-update
	docker-compose run pi2 /bin/bash

dev_vm_init:
	ssh  "$$DEV_MACHINE_FQDN" -t "mkdir -p '/home/$$DEV_MACHINE_USER/.cache/pypoetry'"


d_rm_clean:
	docker rm $$(docker stop $$(docker ps -a -q --filter volume=local_dev_2_pi2-venv --format="{{.ID}}")) || echo "nothing to stop"
	docker volume rm local_dev_2_pi2-venv|| echo "nothing to remove"

reinstall: d_rm_clean build
	docker-compose run pi2 echo "done"


venv:
	bash ensure_venv.sh
	# source $$(realpath $$HOST_VENV_ROOT)/bin/activate

clean-venv:
	rm -rf $$HOST_VENV_ROOT

reset-venv: clean-venv venv

upload-dm: venv
	. $$(realpath $$HOST_VENV_ROOT)/bin/activate ; upload-to-dev-vm

pycharm-upload-config: venv
	. $$(realpath $$HOST_VENV_ROOT)/bin/activate ; pycharm-ensure-upload-config

pycharm-ensure-structure: venv
	. $$(realpath $$HOST_VENV_ROOT)/bin/activate ; pycharm-ensure-project-structure

pycharm: venv pycharm-ensure-structure pycharm-upload-config


# legacy cmd names from ../local_dev
tools-venv-force: reset-venv
tools-venv-cleanup: clean-venv
tools-venv: venv
pycharm-uploads: pycharm-upload-config
pycharm-proj-struct: pycharm-ensure-structure


# on host
# pip install yandex-passport-vault-client -i https://pypi.yandex-team.ru/simple

# Request secrets from vault and store it to env file
secrets/private/base_secrets.env: secrets/conf/defaults.conf secrets/conf/templates/base_secrets.env.j2
	yav-deploy --force --deploy-root ./secrets/private --configs-path ./secrets/conf


.PHONY: secrets-update
secrets-update: secrets/private/base_secrets.env


.PHONY: secrets-cleanup
secrets-cleanup:
	rm -rf ./secrets/private/*


# Request secrets, disregard the current state.
.PHONY: secrets-update-force
secrets-update-force:
	make secrets-cleanup
	make secrets-update

# End of secrets
