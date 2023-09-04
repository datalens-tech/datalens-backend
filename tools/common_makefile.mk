# Makefile pieces that can be conveniently shared between datalens/backend projects.

include ../../tools/common_makefile_i18n.mk


# Stop all related containers
.PHONY: docker-cleanup
docker-cleanup:
	docker-compose stop
	docker-compose rm -f


# Run interactive bash in the remote-python container
.PHONY: docker-remote
docker-remote:
	cd ../../tools/local_dev && $(MAKE) docker-remote


requirements_actual.txt: requirements.txt
	python -c "import re; data = open('requirements.txt').read(); data = re.sub(r'\n(sqlalchemy-metrika-api|sqlalchemy-statface-api|statcommons|statinfra-chyt-sqlalchemy|statinfra-clickhouse-sqlalchemy|yandex-bi-core|yandex-bi-formula)[^\n]+', '', data); fobj = open('requirements_actual.txt', 'w'); fobj.write(data); fobj.close()"
	touch -r requirements.txt requirements_actual.txt
# ^ `touch` should make the clean+build undistinguishable from the original.


.PHONY: tests-tier1-clean
tests-tier1-clean:
	sudo rm -rf .pytest_cache .tox *.egg-info .coverage .coverage.*


.PHONY: build-tier1-clean
build-tier1-clean:
	rm -rf local_libs requirements_actual.txt requirements_tox.txt


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
	cd ../../mainrepo/docker_build && docker buildx bake -f bake.hcl gen_antlr


.PHONY: build-like-gh
build-like-gh:
	docker build ../.. -f Dockerfile.tier1 --progress=plain --tag bi-api-tier1-local:latest
