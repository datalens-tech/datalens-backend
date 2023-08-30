# bi-formula

BI formula language translator & interpreter

[Formula Documentation](bi_formula/README.md)


## Databases initialization

Databases initialization implemented in entry point of Docker container `init-db`.
 After launch it tries to initialize databases. After initialization it starts HTTP-server on port 8000.
 It is used for synchronization with `tests` container.
 It waits for this port to be opened before launching test (implemented in entry point).


## Testing

Requirements:
- `docker`
- `docker-compose`

To run tests for Oracle Database you will need to download and save the following files:
- https://jing.yandex-team.ru/files/gstatsenko/instantclient-basiclite-linux.x64-18.3.0.0.0dbru.zip
- https://jing.yandex-team.ru/files/gstatsenko/instantclient-sqlplus-linux.x64-18.3.0.0.0dbru.zip

to `oracle-libs/bin/` inside the project root directory


### Networking

Compose file uses external network definition to provide IPv6 connectivity on Linux. So you need to create it manually.

```bash
docker network create --ipv6 --driver bridge --subnet=fd00:fdea::2:0/112 --gateway=fd00:fdea::2:1 bi-common
sudo ip6tables -t nat -A POSTROUTING -s fd00:fdea::2:0/112 -j MASQUERADE
```


### Running

Run:
```
make test
```


## Running tests with PyCharm in Docker

Service `remote-python` is used to execute tests/other Python launch configurations with PyCharm.
 It use dedicated virtualenv in `docker-data-volumes/remote-python/venvs` and activates it in entry point.
 Test databases URLs

Dependencies installation implemented in entry point.
 To skip installation set environment variable `SKIP_DEPS_INSTALL=1`.
 You should also do it for `pystest` & `python` launch configuration templates in PyCharm to speed-up launches.

> Before launching container run make target `docker-volumes` to prepare git-ignored directories structure.

To setup remote interpreter in PyCharm go to Preferences -> Project -> Project interpreter.
 Click Gear Icon -> Add -> docker-compose.
 Select main docker-compose file and service `remote-python`. Python interpreter path should be `/venvs/py35/bin/python`.
 Docker-compose files should be `[docker-compose.yml, docker-compose.devenv.yml]`.
 After you click `OK` Pycharm will launch docker-compose with some overrides, patch container, download and index virtualenv.

> After installation of new libraries in virtualenv open Edit dialog of Project interpreter and click `OK` to trigger virtualenv reindex.

> Database initialization does not triggered by container launch, so you should run `init-db` container manually.

> To launch interactive bash with activated environment use make target `docker-remote`


## Building and Uploading

Recommended: https://sandbox.yandex-team.ru/scheduler/22560/view

Manual:

Requires `[develop]` extra to be installed.

Requires the `yandex` repository to be configured in your `.pypirc` file:
see [wiki](https://wiki.yandex-team.ru/datalens/infrastructure/support/Nastrojjka-okruzhenija/#pypi).

Run:
```
make release
```


## Localization

### Requirements:

GNU/Linux will most likely have `gettext` installed.

Mac OS X (if the `msgfmt` command is not available):
```
brew install gettext
brew link gettext
```


### Management

Update `.po` files for fresh source code

```
make update-po
```

Build translation binaries (`.mo` files)

```
make translations
```

For generating documentation files see `formula-cli doc *` [command reference](bi_formula/doc/FORMULA-CLI.md)
