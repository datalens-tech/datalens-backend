# Dependencies
* docker
* [nvm](https://github.com/nvm-sh/nvm?tab=readme-ov-file#installing-and-updating)

# Prepare datalens-ui for development

* `task datalens-ui-repo-clone` - Clone repository near primary repository
* `nvm install # from datalens-ui directory` - Install correct node version
* `task datalens-ui-init` - Install dependencies

# Before tests

* `task datalens-ui-repo-update` - Update repository for latest changes:
* `task docker-build` - Build backend and frontend docker images
* `task docker-start` - Start dev containers

# Run tests

Run tests with `task test`

To run specific tests, use `task test PATTERN=<test name>`
To run with headfull browser, use `task test HEADFUL=true` or `task test-headful`

To read dev container logs, use `task docker-logs`
Or to read logs from specific container, use `task docker-logs-data-api` or `task docker-logs-control-api`

# After tests

* `task docker-stop` - Stop dev containers

