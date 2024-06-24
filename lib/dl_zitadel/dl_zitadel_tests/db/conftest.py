import logging
import os
import time

import httpx
import pytest

import dl_zitadel


@pytest.fixture(name="zitadel_secrets_folder")
def fixture_zitadel_secrets_folder() -> str:
    current_file_path = os.path.abspath(__file__)
    current_dir_path = os.path.dirname(current_file_path)
    secrets_dir_path = os.path.join(current_dir_path, "../../docker-compose/zitadel/secrets")

    return secrets_dir_path


def wait_for_path(path: str) -> None:
    while not os.path.exists(path):
        logging.warning(f"Waiting for {path} to exist")
        time.sleep(1)


def read_secret_value(secret_path: str) -> str:
    wait_for_path(secret_path)

    with open(secret_path) as f:
        return f.read().strip()


@pytest.fixture(name="zitadel_base_url")
def fixture_zitadel_base_url() -> str:
    return "http://localhost:8085"


@pytest.fixture(name="project_id")
def fixture_project_id(zitadel_secrets_folder: str) -> str:
    return read_secret_value(f"{zitadel_secrets_folder}/DL_PROJECT_ID")


@pytest.fixture(name="app_client_id")
def fixture_app_client_id(zitadel_secrets_folder: str) -> str:
    return read_secret_value(f"{zitadel_secrets_folder}/DL_CLIENT_ID")


@pytest.fixture(name="app_client_secret")
def fixture_app_client_secret(zitadel_secrets_folder: str) -> str:
    return read_secret_value(f"{zitadel_secrets_folder}/DL_CLIENT_SECRET")


@pytest.fixture(name="charts_service_client_id")
def fixture_charts_service_client_id(zitadel_secrets_folder: str) -> str:
    return read_secret_value(f"{zitadel_secrets_folder}/CHARTS_SERVICE_CLIENT_ID")


@pytest.fixture(name="charts_service_client_secret")
def fixture_charts_service_client_secret(zitadel_secrets_folder: str) -> str:
    return read_secret_value(f"{zitadel_secrets_folder}/CHARTS_SERVICE_CLIENT_SECRET")


@pytest.fixture(name="bi_service_client_id")
def fixture_bi_service_client_id(zitadel_secrets_folder: str) -> str:
    return read_secret_value(f"{zitadel_secrets_folder}/BI_SERVICE_CLIENT_ID")


@pytest.fixture(name="bi_service_client_secret")
def fixture_bi_service_client_secret(zitadel_secrets_folder: str) -> str:
    return read_secret_value(f"{zitadel_secrets_folder}/BI_SERVICE_CLIENT_SECRET")


@pytest.fixture(name="charts_zitadel_sync_client")
def fixture_charts_zitadel_sync_client(
    zitadel_base_url: str,
    project_id: str,
    app_client_id: str,
    app_client_secret: str,
    charts_service_client_id: str,
    charts_service_client_secret: str,
) -> dl_zitadel.ZitadelSyncClient:
    return dl_zitadel.ZitadelSyncClient(
        base_client=httpx.Client(),
        base_url=zitadel_base_url,
        project_id=project_id,
        app_client_id=app_client_id,
        app_client_secret=app_client_secret,
        client_id=charts_service_client_id,
        client_secret=charts_service_client_secret,
    )


@pytest.fixture(name="charts_access_token")
def fixture_charts_access_token(charts_zitadel_sync_client: dl_zitadel.ZitadelSyncClient) -> str:
    result = charts_zitadel_sync_client.get_token()
    return result.access_token


@pytest.fixture(name="bi_zitadel_sync_client")
def fixture_bi_zitadel_sync_client(
    zitadel_base_url: str,
    project_id: str,
    app_client_id: str,
    app_client_secret: str,
    bi_service_client_id: str,
    bi_service_client_secret: str,
) -> dl_zitadel.ZitadelSyncClient:
    return dl_zitadel.ZitadelSyncClient(
        base_client=httpx.Client(),
        base_url=zitadel_base_url,
        project_id=project_id,
        app_client_id=app_client_id,
        app_client_secret=app_client_secret,
        client_id=bi_service_client_id,
        client_secret=bi_service_client_secret,
    )


@pytest.fixture(name="bi_zitadel_async_client")
def fixture_bi_zitadel_async_client(
    zitadel_base_url: str,
    project_id: str,
    app_client_id: str,
    app_client_secret: str,
    bi_service_client_id: str,
    bi_service_client_secret: str,
) -> dl_zitadel.ZitadelAsyncClient:
    return dl_zitadel.ZitadelAsyncClient(
        base_client=httpx.AsyncClient(),
        base_url=zitadel_base_url,
        project_id=project_id,
        app_client_id=app_client_id,
        app_client_secret=app_client_secret,
        client_id=bi_service_client_id,
        client_secret=bi_service_client_secret,
    )
