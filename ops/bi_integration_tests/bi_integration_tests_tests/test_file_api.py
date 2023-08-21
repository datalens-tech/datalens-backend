import uuid

import pytest

import bi_integration_tests.request_executors.base
from bi_integration_tests import common, request_executors
from bi_integration_tests.request_executors.bi_api_client import BIAPIClient
from bi_testing import utils
from bi_testing_ya.dlenv import DLEnv
from test_data.sales_dataset import get_test_csv_file_contents

STATUS_TIMEOUT_SECONDS = 10
STATUS_RETRY_DELAY = 1


class FileUploaderWaitTimeoutError(Exception):
    pass


@pytest.mark.parametrize(
    "dl_env",
    [
        DLEnv.cloud_preprod,
        DLEnv.cloud_prod,
        DLEnv.internal_preprod,
        DLEnv.internal_prod,
    ],
    indirect=True
)
@utils.skip_outside_devhost
@pytest.mark.asyncio
async def test_file_api(
    dl_env,
    ext_sys_requisites,
    two_users_configuration,
    integration_tests_folder_id,
    integration_tests_reporter,
    tenant
):
    """
    Executes the following steps:
        1) upload file
        2) wait for file upload status
        3) check file source
        4) create connection
        5) wait for connection source status
        6) create dataset
        7) check dataset data
    """

    file_name = 'test.csv'
    base_dir = "access_control_tests/test_file_api"

    file_uploader_executor = request_executors.FileUploaderApiClient.from_settings(
        base_url=ext_sys_requisites.DATALENS_API_LB_UPLOADS_BASE_URL,
        account_creds=two_users_configuration.user_1,
        folder_id=integration_tests_folder_id,
        logger=integration_tests_reporter,
        tenant=tenant
    )

    file_status_executor = request_executors.FileUploaderApiClient.from_settings(
        base_url=ext_sys_requisites.DATALENS_API_LB_UPLOADS_STATUS_URL,
        account_creds=two_users_configuration.user_1,
        folder_id=integration_tests_folder_id,
        logger=integration_tests_reporter,
        tenant=tenant
    )

    bi_api_client = BIAPIClient.create_client(
        base_url=ext_sys_requisites.DATALENS_API_LB_MAIN_BASE_URL,
        account_creds=two_users_configuration.user_1,
        folder_id=integration_tests_folder_id,
        logger=integration_tests_reporter,
        tenant=tenant
    )
    dataset_executor = common.RequestExecutor(
        bi_api_client
    )

    file_content = get_test_csv_file_contents()
    # upload file
    response = await file_uploader_executor.post_file(file_content, file_name)

    file_id = response.json['file_id']

    # wait for file upload status
    await file_status_executor.wait_for_file_status(file_id)

    # check file source
    response = await file_status_executor.get_file_sources(file_id)
    conn_source_id = response.json["sources"][0]["source_id"]

    # create connection
    connection_name = str(uuid.uuid4())

    response = await bi_api_client.create_connection(
        data=bi_integration_tests.request_executors.base.FileConnectionData(
            name=connection_name,
            dir_path=base_dir,
            sources=[
                bi_integration_tests.request_executors.base.FileConnectionSourceData(
                    id=conn_source_id,
                    file_id=file_id,
                    title="test_title")
            ]
        )
    )
    connection_id = response.json["id"]

    # wait for connection source status
    await dataset_executor.wait_for_connection_source_status(connection_id=connection_id)

    # create dataset
    response = await dataset_executor.setup_dataset(
        dataset_json=common.get_csv_dataset_json(connection_id=connection_id, conn_source_id=conn_source_id),
        base_dir=base_dir,
        connection_name=connection_name,
    )
    dataset_id = response.json["id"]

    # check dataset data
    response = await bi_api_client.get_dataset_data(dataset_id)
    data = response.json['result']['data']['Data']
    assert len(data) == 4
