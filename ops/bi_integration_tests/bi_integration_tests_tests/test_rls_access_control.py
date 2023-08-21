from __future__ import annotations

import pytest

from bi_integration_tests import datasets
from bi_integration_tests.common import setup_db_conn_and_dataset, create_executor
from bi_integration_tests.request_executors.bi_api_client import BIAPIClient
from bi_integration_tests.sales_table import upload_data_from_df
from bi_testing_ya.api_wrappers import Req
from bi_testing_ya.dlenv import DLEnv
from bi_testing.utils import skip_outside_devhost
from test_data.sales_dataset import read_superstore_csv_as_pandas_df


@pytest.mark.parametrize("dl_env", [
    DLEnv.cloud_preprod,
    DLEnv.cloud_prod,
    DLEnv.internal_preprod,
    DLEnv.internal_prod,
], indirect=True)
@skip_outside_devhost
@pytest.mark.asyncio
async def test_rls_access(
    dl_env,
    two_users_configuration,
    integration_tests_reporter,
    integration_tests_folder_id,
    integration_tests_postgres_1,
    ext_sys_requisites,
    workbook_id,
    tenant,
    dls_client_factory
):
    """
    Executes the following steps:
        1) Creates sample Postgre connection and dataset.
        2) Modifies RLS access rights for the first test user and checks the result.
        3) Repeats step 2 for the second test user with another RLS settings.
        4) Deletes created connection and dataset.
    """
    base_dir = "access_control_tests/rls_access"

    # uploading file, creating connection and dataset
    pandas_df = read_superstore_csv_as_pandas_df()

    # prepare PG data
    upload_data_from_df(pandas_df, integration_tests_postgres_1)

    # create connection and dataset
    bi_api_client = BIAPIClient.create_client(
        base_url=ext_sys_requisites.DATALENS_API_LB_MAIN_BASE_URL,
        folder_id=integration_tests_folder_id,
        account_creds=two_users_configuration.user_1,
        logger=integration_tests_reporter,
        tenant=tenant
    )
    request_executor = create_executor(bi_api_client, dls_client_factory(two_users_configuration.user_1.user_id))

    response = await setup_db_conn_and_dataset(
        setup_executor=request_executor,
        connection_settings=integration_tests_postgres_1,
        base_dir=base_dir,
        test_dataset=datasets.PG_SALES,
        workbook_id=workbook_id,
        admin_user_ids=[two_users_configuration.user_2.user_id]
    )

    pg_dataset_id = response.json["id"]
    pg_dataset_str_field_id = next(
        field["guid"] for field in response.json["dataset"]["result_schema"]
        if field["title"] == 'Row ID'
    )

    ds = response.json['dataset']

    # request to the dataset without rls
    data_request = Req(
        'post', f'/api/data/v1/datasets/{pg_dataset_id}/versions/draft/result',
        data_json={"columns": [pg_dataset_str_field_id]}
    )
    response = await bi_api_client.execute_request(data_request)
    # 8399 is an actual number of rows in the dataset
    row_count = response.json['result']['data']['Data']
    assert len(row_count) == 8399

    # configure rls for the selected field and first user
    sample_value = row_count[0][0]
    rls_config = f"\'{sample_value}\': {two_users_configuration.user_1.get_rls_user_name()}"
    rls_modification_request = Req(
        "put", f'/api/v1/datasets/{pg_dataset_id}/versions/draft/',
        data_json={'dataset': dict(ds, rls={pg_dataset_str_field_id: rls_config})}
    )
    resp = await bi_api_client.execute_request(rls_modification_request)
    ds = resp.json['dataset']

    response = await bi_api_client.execute_request(data_request)
    row_count = response.json['result']['data']['Data']

    # only one value is expected to be available
    assert len(row_count) == 1
    assert row_count[0][0] == sample_value

    # post request from the second user
    bi_api_client_2 = BIAPIClient.create_client(
        base_url=ext_sys_requisites.DATALENS_API_LB_MAIN_BASE_URL,
        folder_id=integration_tests_folder_id,
        account_creds=two_users_configuration.user_2,
        logger=integration_tests_reporter,
        tenant=tenant
    )

    response = await bi_api_client_2.execute_request(data_request)
    row_count = response.json['result']['data']['Data']
    # no data is available since configured rls knows nothing about the second permissions of the second user
    assert len(row_count) == 0

    # update rls config to grant full read access for the second user
    rls_config = rls_config + f'\n*: {two_users_configuration.user_2.get_rls_user_name()}'
    rls_modification_request.data_json['dataset'] = dict(ds, rls={pg_dataset_str_field_id: rls_config})
    await bi_api_client.execute_request(rls_modification_request)
    response = await bi_api_client_2.execute_request(data_request)
    row_count = response.json['result']['data']['Data']
    assert len(row_count) == 8399

    # cleanup: delete dataset and connection
    pg_conn_id = ds['sources'][0]['connection_id']
    await bi_api_client.execute_request(Req("delete", f"/api/v1/datasets/{pg_dataset_id}"))
    await bi_api_client.execute_request(Req("delete", f"/api/v1/connections/{pg_conn_id}"))
