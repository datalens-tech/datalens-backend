import abc
import asyncio
import time
from typing import (
    Dict,
    List,
    Optional,
)
import uuid

import attr

from bi_dls_client.dls_client import DLSClient
from bi_integration_tests.constants import SOURCE_TYPE_FILE_S3_TABLE_STR
from bi_integration_tests.datasets import IntegrationTestDataset
from bi_integration_tests.report_formatting import ReportFormatter
from bi_integration_tests.request_executors.base import WaitTimeoutError
from bi_integration_tests.request_executors.bi_api_client import BIAPIClient
from bi_integration_tests.steps import file_form
from bi_testing_ya.api_wrappers import (
    Req,
    Resp,
)
from bi_testing_ya.cloud_tokens import AccountCredentials
from dl_api_commons.base_models import TenantDef


class TestSetupExecutor(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def create_dataset(
        self, base_dir, conn_id, connection_name, test_dataset, workbook_id, admin_user_ids
    ) -> Resp:
        raise NotImplementedError

    @abc.abstractmethod
    async def create_conn(self, base_dir, connection_name, connection_settings, workbook_id, admin_user_ids) -> str:
        raise NotImplementedError


@attr.s(auto_attribs=True, frozen=True)
class RequestExecutor(TestSetupExecutor):
    bi_api_client: BIAPIClient

    @classmethod
    def create_executor(
        cls,
        base_url: str,
        folder_id: str,
        logger: ReportFormatter,
        account_creds: Optional[AccountCredentials] = None,
        public_api_key: Optional[str] = None,
        tenant: Optional[TenantDef] = None,
    ) -> "RequestExecutor":
        bi_api_client = BIAPIClient.from_settings(
            base_url, folder_id, logger, account_creds, public_api_key, tenant=tenant
        )
        return cls(bi_api_client)

    async def update_dataset_from_validation_response(
        self,
        validation_response_dataset: dict,
        dataset_id: str,
        require_ok: bool = True,
    ) -> Resp:
        return await self.bi_api_client.update_dataset_from_validation_response(
            validation_response_dataset, dataset_id, require_ok
        )

    async def setup_dataset(
        self,
        dataset_json: dict,
        base_dir: str,
        connection_name: Optional[str] = None,
        workbook_id: Optional[str] = None,
    ) -> Resp:
        """Creates a dataset object for testing purposes."""
        dataset_val_request = Req("post", "/api/v1/datasets/validators/dataset", data_json=dataset_json)
        dataset_validation_response = await self.bi_api_client._request(dataset_val_request)

        if connection_name is None:
            connection_name = str(uuid.uuid4())

        dataset = dataset_validation_response.json["dataset"]
        dataset_response = await self.bi_api_client.create_dataset_from_validation_resp(
            dataset, f"test_dataset_{connection_name}", base_dir, workbook_id
        )
        return dataset_response

    async def create_conn(self, base_dir, connection_name, connection_settings, workbook_id, admin_user_ids) -> str:
        return await self.bi_api_client.create_conn(base_dir, connection_name, connection_settings, workbook_id)

    async def create_dataset(
        self, base_dir, conn_id, connection_name, test_dataset, workbook_id, admin_user_ids
    ) -> Resp:
        # creates dataset from connection
        dataset_json = get_sample_db_dataset_json(conn_id=conn_id, test_dataset=test_dataset)
        dataset_response = await self.setup_dataset(
            dataset_json=dataset_json,
            base_dir=base_dir,
            connection_name=connection_name,
            workbook_id=workbook_id,
        )
        return dataset_response

    async def wait_for_connection_source_status(
        self, connection_id: str, timeout_seconds: float = 10, retry_delay_seconds: float = 1
    ) -> Resp:
        deadline = time.time() + timeout_seconds

        while True:
            if time.time() > deadline:
                raise WaitTimeoutError("Connection source wait reached timeout")

            response = await self.bi_api_client.get_connection(connection_id)
            status = response.json["sources"][0]["status"]

            if status == "ready":
                return response

            self.bi_api_client.logger.row(f"Waiting for connection source status: {response.json}")
            await asyncio.sleep(retry_delay_seconds)


async def setup_db_conn_and_dataset(
    setup_executor: TestSetupExecutor,
    connection_settings: Dict[str, str],
    base_dir: str,
    test_dataset: IntegrationTestDataset,
    workbook_id: Optional[str],
    admin_user_ids: List[str],
) -> Resp:
    """Creates connection and dataset objects for testing purposes."""
    # creates connection
    connection_name = str(uuid.uuid4())
    conn_id = await setup_executor.create_conn(
        base_dir, connection_name, connection_settings, workbook_id, admin_user_ids
    )
    dataset_response = await setup_executor.create_dataset(
        base_dir, conn_id, connection_name, test_dataset, workbook_id, admin_user_ids
    )
    return dataset_response


def get_sample_db_dataset_json(conn_id: str, test_dataset: IntegrationTestDataset):
    """Returns sample DataLens DataBase dataset configuration in json format."""
    source_id = str(uuid.uuid4())
    return {
        "updates": [
            {
                "action": "add_source",
                "source": {
                    "id": source_id,
                    "title": test_dataset.table_name,
                    "source_type": test_dataset.source_type,
                    "connection_id": conn_id,
                    "parameters": {"schema_name": test_dataset.db_name, "table_name": test_dataset.table_name},
                },
            },
            {
                "action": "add_source_avatar",
                "source_avatar": {
                    "id": str(uuid.uuid4()),
                    "is_root": True,
                    "title": f"{test_dataset.db_name}.{test_dataset.table_name}",
                    "source_id": source_id,
                },
            },
        ]
    }


def prepare_csv_for_uploading(base_dir: str, csv_binary_content: bytes):
    return file_form(
        file_content=csv_binary_content,
        file_name=f"test_csv_{str(uuid.uuid4())}.csv",
        file_content_type="text/csv",
        other={
            "type": "csv",
            "dir_path": base_dir,
            "preview": "0",
            "hidden": "0",
            "sample_size": "100",
        },
    )


def get_csv_dataset_json(connection_id: str, conn_source_id: str):
    """Returns sample DataLens CSV dataset configuration in json format."""
    source_id = str(uuid.uuid4())
    source_avatar_id = str(uuid.uuid4())
    return {
        "updates": [
            {
                "action": "add_source",
                "source": {
                    "connection_id": connection_id,
                    "id": source_id,
                    "parameters": {
                        "origin_source_id": conn_source_id,
                    },
                    "source_type": SOURCE_TYPE_FILE_S3_TABLE_STR,
                    "title": "source_1",
                },
            },
            {
                "action": "add_source_avatar",
                "source_avatar": {"id": source_avatar_id, "source_id": source_id, "title": "avatar_1", "is_root": True},
            },
        ]
    }


def get_dataset_json_from_validation(
    validation_response_dataset: Dict, dataset_name: str = None, base_dir: str = None, workbook_id: Optional[str] = None
):
    """Returns dataset object in json format."""
    dataset_json = {"dataset": validation_response_dataset}

    if dataset_name is not None:
        dataset_json["name"] = dataset_name
    if base_dir is not None:
        dataset_json["dir_path"] = base_dir
    if workbook_id is not None:
        dataset_json["workbook_id"] = workbook_id

    return dataset_json


async def update_dataset(executor: RequestExecutor, dataset_id: str, update_request: Req) -> Resp:
    ds_validation_response = await executor.bi_api_client._request(update_request)
    if ds_validation_response.status == 200:
        await executor.update_dataset_from_validation_response(
            ds_validation_response.json["dataset"], dataset_id=dataset_id, require_ok=update_request.require_ok
        )

    return ds_validation_response


async def add_formula_fields_to_dataset(
    executor: RequestExecutor,
    dataset_id: str,
    title_to_formula: dict,
    require_ok=True,
) -> Resp:
    field_update_request = Req(
        "post",
        f"/api/v1/datasets/{dataset_id}/versions/draft/validators/schema",
        data_json={
            "updates": [
                get_add_formula_field_action_json(title=key, formula=formula)
                for key, formula in title_to_formula.items()
            ]
        },
        require_ok=require_ok,
    )

    response = await update_dataset(executor=executor, dataset_id=dataset_id, update_request=field_update_request)
    return response


def get_add_formula_field_action_json(title: str, formula: str):
    """Returns a json for an action to add formula field."""
    return {"action": "add_field", "field": {"guid": str(uuid.uuid4()), "title": title, "formula": formula}}


@attr.s(auto_attribs=True, frozen=True)
class DLSRequestExecutorDecorator(TestSetupExecutor):
    dls_client: DLSClient
    request_executor: RequestExecutor

    async def create_dataset(
        self, base_dir, conn_id, connection_name, test_dataset, workbook_id, admin_user_ids
    ) -> Resp:
        dataset_resp = await self.request_executor.create_dataset(
            base_dir, conn_id, connection_name, test_dataset, workbook_id, admin_user_ids
        )
        dataset_id = dataset_resp.json["id"]
        for user_id in admin_user_ids:
            self.dls_client.assign_admin_role(dataset_id, user_id=user_id, comment="burp")
        return dataset_resp

    async def create_conn(self, base_dir, connection_name, connection_settings, workbook_id, admin_user_ids) -> str:
        conn_id = await self.request_executor.create_conn(
            base_dir, connection_name, connection_settings, workbook_id, admin_user_ids
        )
        for user_id in admin_user_ids:
            self.dls_client.assign_admin_role(conn_id, user_id=user_id, comment="burp")
        return conn_id


def create_executor(bi_api_client: BIAPIClient, dls_client: Optional[DLSClient] = None) -> TestSetupExecutor:
    executor = RequestExecutor(bi_api_client)
    if dls_client is not None:
        return DLSRequestExecutorDecorator(dls_client, executor)
    else:
        return executor
