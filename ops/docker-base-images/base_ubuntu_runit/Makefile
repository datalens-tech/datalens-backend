
build:
	docker build --pull -t registry.yandex.net/statinfra/base/ubuntu_runit .
	docker run --rm registry.yandex.net/statinfra/base/ubuntu_runit python -c 'import sys; print("Python runtest, version", sys.version)'
	docker push registry.yandex.net/statinfra/base/ubuntu_runit
