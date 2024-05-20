# How to build DataLens backend

## Prerequisites

Install or update docker (datalens requires Docker engine 19.03 or newer):

- [Install docker](https://docs.docker.com/get-docker/)
- [Install docker compose plugin](https://docs.docker.com/compose/install/)

## Build

Navigate to the repository:
```bash
git clone git@github.com:datalens-tech/datalens-backend.git && cd datalens-backend
```

Use the following commands to build images and set the desired tags:
```bash
./docker_build/run-project-bake dl_control_api --set "dl_control_api.tags=datalens-control-api:local"
./docker_build/run-project-bake dl_data_api --set "dl_data_api.tags=datalens-data-api:local"
```

## Run DataLens with new API images

1. Checkout https://github.com/datalens-tech/datalens
2. Replace the `image` section for `control-api` and `data-api` services in docker-compose with the images created in the previous steps
3. Follow the instruction in the [README](https://github.com/datalens-tech/datalens/blob/main/README.md) to launch DataLens
