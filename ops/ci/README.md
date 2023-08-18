# Collect and export 3rd party dependencies

If you add a new package, 3rd party dependency, please do
- Create (or reuse existing) venv with poetry==1.5.* and set path in .env>HOST_VENV_ROOT
- Invoke poetry lock and exporting 3rd party python dependencies.

    make export_3rd_party_deps

Due to the current status of yandex PyPI poetry needs to fetch a lot of data.
Locally we could keep poetry cache between locks.
(todo: stage cache to some s3 bucket during teamcity build and attach to build container )
