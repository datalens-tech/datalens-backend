
build:
	make --directory=../base_ubuntu_runit build
	docker build --pull -t registry.yandex.net/statinfra/base/bi_mess .
	docker run --rm registry.yandex.net/statinfra/base/bi_mess python -c 'import sys; print("Python runtest, version", sys.version)'
	docker push registry.yandex.net/statinfra/base/bi_mess
