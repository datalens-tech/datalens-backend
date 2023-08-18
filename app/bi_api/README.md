# bi-api

## About

**Репозиторий для API Yandex.BI**

В директории mockup будут лежать примеры структур данных для разных сущностей внутри BI

Проект QLOUD живет тут: https://qloud.yandex-team.ru/projects/statbox/bi

Чуть подробнее надо читать тут: https://wiki.yandex-team.ru/users/dmifedorov/Kak-zhit-v-qloud
 
Пример YQL скрипта для грепа по логам приложения: **TODO**

Список отчетов для просмотра разной аналитики тут: **TODO**


## Installation & Requirement Management

To install the package run (from the directory where the code has been checked out to):
```
pip install -r requirements.txt
pip install -e .
```

If you need to run tests locally, then also install the `[tests]` extra requirements:
```
pip install -e .[tests]
```
They are not required to run the application and should not be included in product packages.

### Updating Requirements

You need to have the `pip-tools` installed.

The `requirements.txt` file is managed with the `pip-compile` tool (https://pypi.org/project/pip-tools/). Avoid editing it manually if possible.

If you want to add a new package to requirements, first add it to `setup.py` and then run:
```
./_aux/pip-compile-here --upgrade-package <package-name>
```

To synchronize your virtualenv with requirements.txt (install/upgrade/remove extra packages) run:
```
pip-sync requirements.txt --index-url https://pypi.yandex-team.ru/simple/
```

Note that these commands ignore the `pip-tools` package if it is installed in the same environment,
so that it is not included in requirement files or uninstalled when `pip-sync` is run


## CD/CI
In order to build new release and deploy in to testing environment:
  * Go to https://sandbox.yandex-team.ru/scheduler/11740/view
  * Push "Run Once" button
  * New build will be deployed to `int-testing` and `testing` environment


## Testing & Development

### Requirements
- `docker`: https://docs.docker.com/install/#releases
- `docker-compose`: `pip install docker-compose` (globally or inside a virtualenv)
- `make`
- "punch holes" for database connections (TODO: add more info about this)
- log in to the Yandex docker registry: https://wiki.yandex-team.ru/qloud/docker-registry/

### Running Tests in Docker

Run tests in docker:
```
make test
```

### Creating a Development Environment

Start the development environment
```
make devenv
```

This will create and start the services required for running and testing `bi-api`

See `docker-compose.yml` for info on exposed ports


### Cleanup

Stop and remove running test/devenv containers:
```
make docker-cleanup
```

Remove all test/devenv artifacts (including containers):
```
make cleanup
```

## Code Profiling

Collect profiling stats by using context manager
```
with utils.profiler_stats('cprofiler/my_stats'):
    # your code goes here
    ...
```
or decorator
```
@api_decorators.with_profiler_stats('cprofiler/my_stats')
def my_func():
    # your code goes here
    ...
```
A new `.stats` file will be created in the `cprofiler/my_stats` folder after it is done.

To visualize the data use snakeviz (`pip install snakeviz`):
```
snakeviz ./cprofiler/<file-or-dir-name>
```

Also, profiling of API handlers can be turned on by setting environment variable
```
PROFILE_REQUESTS=/path/to/stats_files
```
to a path where the server has write access (e.g. `/tmp/cprofiler`) and restarting the server.
Statistics files will be saved to the `PROFILE_REQUESTS` directory

You can limit the profiler to specific requests (e.g. a certain URL path or dataset ID)
by setting additional parameters:
```
PROFILE_REQ_CLASSES="DatasetVersionResult"
PROFILE_REQ_METHODS="POST"
PROFILE_REQ_PATH_RE="/api/v1/datasets/njJYHhj8oikj/versions/draft/result"
```
where `PROFILE_REQ_CLASSES` and `PROFILE_REQ_METHODS` are comma-separated lists
