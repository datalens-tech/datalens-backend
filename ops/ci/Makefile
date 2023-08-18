include .env
export

SHELL := bash

rnd:
	FOO=`shuf -er -n20  {A..Z} {a..z} {0..9} | tr -d '\n'`; echo ">>$$FOO"

export_3rd_party_deps:
	set -x; TMP_DIR=`shuf -er -n20  {A..Z} {a..z} {0..9} | tr -d '\n'`; mkdir -p /tmp/$$TMP_DIR; \
	source $$HOST_VENV_ROOT/bin/activate; \
	echo $$BASE_IMG_CTX ; \
	poetry lock --no-update; \
	poetry export --without-hashes --with=dev --with=ci --format=requirements.txt > /tmp/$$TMP_DIR/requirements.txt ; \
	cat /tmp/$$TMP_DIR/requirements.txt | grep -v "@ file:///" > $$BASE_IMG_CTX/requirements_external.txt; rm -rf /tmp/$$TMP_DIR/ ;

dev_vm_init:
	ssh  "$$DEV_MACHINE_FQDN" -t "mkdir -p '/home/$$DEV_MACHINE_USER/.cache/pypoetry'"

d_rm_clean:
	docker rm $$(docker stop $$(docker ps -a -q --filter ancestor=local_dev_2-pi2 --format="{{.ID}}"))
	docker volume rm local_dev_2_pi2-venv
