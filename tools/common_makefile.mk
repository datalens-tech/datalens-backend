# Makefile pieces that can be conveniently shared between datalens/backend projects.


# Stop all related containers
.PHONY: docker-cleanup
docker-cleanup:
	docker-compose stop
	docker-compose rm -f


# Run interactive bash in the remote-python container
.PHONY: docker-remote
docker-remote:
	cd ../../tools/local_dev && $(MAKE) docker-remote


# Because importing some of the bi_core stuff unexpectedly imports many other packages
# (former `bi_core[db]`, now in the install_requires),
# all local dependencies deleted in `requirements_actual.txt`, but required by bi_core, have to be installed.
# Override in including makefiles as needed (pretty much just `LOCAL_LIBS := $(LOCAL_LIBS) lib/bi_formula`)
# WARNING: `lib/bi_common` depends on `bi-sqlalchemy-postgres yandex-bi-app-tools
# yandex-bi-configs`, and specifying them together breaks installation on pip>=20.2.
# Same with `bi_integration_tests` -> `lib/bi_cloud_integration`
LOCAL_LIBS = lib/bi_core lib/bi_testing lib/bi_sqlalchemy_chyt lib/clickhouse-sqlalchemy lib/sqlalchemy_metrika_api lib/statcommons
# See also: `local_dev/requirements/all_local_packages.lst`, documented in `local_dev/README.md`


requirements_actual.txt: requirements.txt
	python -c "import re; data = open('requirements.txt').read(); data = re.sub(r'\n(sqlalchemy-metrika-api|sqlalchemy-statface-api|statcommons|statinfra-chyt-sqlalchemy|statinfra-clickhouse-sqlalchemy|yandex-bi-core|yandex-bi-formula)[^\n]+', '', data); fobj = open('requirements_actual.txt', 'w'); fobj.write(data); fobj.close()"
	touch -r requirements.txt requirements_actual.txt
# ^ `touch` should make the clean+build undistinguishable from the original.


requirements_tox.txt: requirements_actual.txt
	( cat requirements_actual.txt; for path in $(LOCAL_LIBS); do printf -- "-e file:./../../%s#egg=%s\n" "$$path" "$$(sed -rn "s/ *name='([^']+)'.*/\1/ p" "../../$$path/setup.py")"; done; ) > requirements_tox.txt
	sed -i '/^certifi-yandex==/ d' requirements_tox.txt
	printf 'certifi-yandex\n' >> requirements_tox.txt


.PHONY: tests-tier1-clean
tests-tier1-clean:
	sudo rm -rf .pytest_cache .tox *.egg-info .coverage .coverage.*


.PHONY: build-tier1-clean
build-tier1-clean:
	rm -rf local_libs requirements_actual.txt requirements_tox.txt


.PHONY: build-tier1-prepare
build-tier1-prepare: build-tier1-clean requirements_actual.txt
	mkdir -p local_libs
	for path in $(LOCAL_LIBS); do cp -a "../$$path" ./local_libs/; done


image := $$(python -c "import json; import sys; f = open('app/.package.json'); r = json.load(f); sys.stdout.write(r['meta']['name']); f.close()")
version := "$$(sed -rn '/^[0-9]+\.[0-9a-z.]+$$/ { p; q; }' changelog.md)"
docker_repository = statinfra


.PHONY: build-tier0-suffixed
build-tier0-suffixed:
	ya package app/.package.json --docker --docker-repository=$(docker_repository) --docker-push --custom-version="$(version)tier0" --docker-network=host
	docker tag latest registry.yandex.net/$(docker_repository)/$(image)/$(version)
	docker push registry.yandex.net/$(docker_repository)/$(image):latest


.PHONY: build-tier0
build-tier0:
	ya package app/.package.json --docker --docker-repository=$(docker_repository) --docker-push --custom-version="$(version)" --docker-network=host
	docker tag registry.yandex.net/$(docker_repository)/$(image):$(version) registry.yandex.net/$(docker_repository)/$(image):latest
	docker push registry.yandex.net/$(docker_repository)/$(image):latest


.PHONY: build-cloud-preprod
build-cloud-preprod:
	../../tools/build-cloud-preprod

.PHONY: gen-parser
SHELL = /bin/bash
gen-parser:
	cd ../../docker_build && docker buildx bake -f bake.hcl gen_antlr

# Merge updated messages.pot template file into .po files
.PHONY: update-po
update-po:
	cd ../../docker_build && \
	./run-project-bake update-po --set update-po.args.PACKAGE_NAME=$(PACKAGE_NAME) --set update-po.contexts.src=$(PWD) --set update-po.output="type=local,dest=$(PWD)/$(PACKAGE_NAME)"


.PHONY: msgfmt
msgfmt:
	cd ../../docker_build && \
	./run-project-bake msgfmt --set msgfmt.args.PACKAGE_NAME=$(PACKAGE_NAME) --set msgfmt.contexts.src=$(PWD) --set msgfmt.output="type=local,dest=$(PWD)/$(PACKAGE_NAME)"


.PHONY: build-like-gh
build-like-gh:
	docker build ../.. -f Dockerfile.tier1 --progress=plain --tag bi-api-tier1-local:latest
