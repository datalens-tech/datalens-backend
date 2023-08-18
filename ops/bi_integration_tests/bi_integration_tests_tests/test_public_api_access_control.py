from __future__ import annotations

import pytest

from bi_integration_tests import datasets
from bi_integration_tests.common import RequestExecutor, setup_db_conn_and_dataset
from bi_integration_tests.request_executors.bi_api_client import BIAPIClient
from bi_testing.api_wrappers import Req
from bi_testing.dlenv import DLEnv
from bi_testing.utils import skip_outside_devhost, requires_materialization


@pytest.mark.parametrize("dl_env", [
    DLEnv.cloud_preprod,
    DLEnv.cloud_prod,
], indirect=True)
@skip_outside_devhost
@pytest.mark.asyncio
@requires_materialization
@pytest.mark.requires_materialization
async def test_public_api_access(
    dl_env,
    integration_tests_reporter,
    two_users_configuration,
    ext_passport_public_api_key,
    integration_tests_folder_id,
    integration_tests_postgres_1,
    ext_sys_requisites,
    workbook_id,
    tenant
):
    """
    Executes the following steps:
        1) Creates sample Postgre connection and dataset.
        2) Materializes the dataset and checks the materialization status in loop.
        3) Verifies that public API does not provide an access to the internal objects (dataset).
        4) Stops created materialization, deletes sample dataset and connection.
    """
    base_dir = "access_control_tests/public_api_access"
    # configure executor
    bi_api_client = BIAPIClient.create_client(
        base_url=ext_sys_requisites.DATALENS_API_LB_MAIN_BASE_URL,
        account_creds=two_users_configuration.user_1,
        folder_id=integration_tests_folder_id, logger=integration_tests_reporter,
        tenant=tenant
    )
    request_executor = RequestExecutor(bi_api_client)

    # create connection and dataset
    response = await setup_db_conn_and_dataset(
        setup_executor=request_executor,
        connection_settings=integration_tests_postgres_1,
        base_dir=base_dir,
        test_dataset=datasets.PG_SQL_FEATURES,
        workbook_id=workbook_id,
        admin_user_ids=[]
    )
    pg_dataset_id = response.json["id"]
    ds = response.json['dataset']

    public_bi_api_client = BIAPIClient.create_client(
        base_url=ext_sys_requisites.DATALENS_API_LB_MAIN_BASE_URL,
        folder_id=integration_tests_folder_id,
        logger=integration_tests_reporter,
        public_api_key=ext_passport_public_api_key,
        tenant=tenant
    )

    public_fields_request = Req("get", f"/public/api/data/v1/datasets/{pg_dataset_id}/fields", require_ok=False)
    public_response = await public_bi_api_client.execute_request(public_fields_request)
    assert public_response.status == 404

    response = await bi_api_client.execute_request(Req("get", f"/api/data/v1/datasets/{pg_dataset_id}/fields"))
    assert response.status == 200

    # cleanup: delete dataset and connection
    pg_conn_id = ds['sources'][0]['connection_id']
    await bi_api_client.execute_request(Req("delete", f"/api/v1/datasets/{pg_dataset_id}"))
    await bi_api_client.execute_request(Req("delete", f"/api/v1/connections/{pg_conn_id}"))
