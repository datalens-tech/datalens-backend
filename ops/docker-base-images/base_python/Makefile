
build:
	docker build -t registry.yandex.net/statinfra/base/python .
	docker run --rm registry.yandex.net/statinfra/base/python python -c 'import sys; print("Python runtest, version", sys.version)'
	docker push registry.yandex.net/statinfra/base/python
