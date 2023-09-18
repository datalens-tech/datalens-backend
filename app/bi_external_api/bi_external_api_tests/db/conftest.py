import random
import time

import pytest

from bi_external_api.converter.workbook_ctx_loader import WorkbookContextLoader
from bi_external_api.domain.internal import datasets
from bi_external_api.testings import PGSubSelectDatasetFactory


@pytest.fixture()
def wb_ctx_loader(bi_ext_api_test_env_int_api_clients) -> WorkbookContextLoader:
    return WorkbookContextLoader(
        internal_api_clients=bi_ext_api_test_env_int_api_clients,
        use_workbooks_api=False,
    )


@pytest.fixture()
def pseudo_wb_path() -> str:
    return f"db_tests/wb_{int(time.time() * 1000)}_{random.randint(0, 9)}"


@pytest.fixture()
def pg_connection_def_params(
    bi_ext_api_test_env,
) -> dict:
    env = bi_ext_api_test_env
    return dict(
        type="postgres",
        host=env.DB_PG_HOST,
        port=env.DB_PG_PORT,
        username=env.DB_PG_USERNAME,
        password=env.DB_PG_PASSWORD,
        db_name=env.DB_PG_DB_NAME,
        raw_sql_level="subselect",
        cache_ttl_sec=None,
    )


@pytest.fixture()
async def pg_connection(
    pseudo_wb_path,
    bi_ext_api_test_env_bi_api_control_plane_client,
    pg_connection_def_params,
) -> datasets.ConnectionInstance:
    int_api_cli = bi_ext_api_test_env_bi_api_control_plane_client

    conn = await int_api_cli.create_connection(
        wb_id=pseudo_wb_path,
        name="pg_conn_main",
        conn_data=pg_connection_def_params,
    )
    yield conn
    await int_api_cli.delete_connection(conn.id)


@pytest.fixture(scope="function")
def dataset_factory(
    pseudo_wb_path,
    bi_ext_api_test_env_bi_api_control_plane_client,
    pg_connection,
) -> PGSubSelectDatasetFactory:
    return PGSubSelectDatasetFactory(
        wb_id=pseudo_wb_path,
        bi_api_cli=bi_ext_api_test_env_bi_api_control_plane_client,
        conn_id=pg_connection.id,
    )
