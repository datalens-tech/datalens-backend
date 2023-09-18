from typing import (
    Dict,
    Optional,
)
import uuid

import attr

from bi_integration_tests.report_formatting import ReportFormatter
import bi_integration_tests.request_executors.base as request_executors_base
from bi_integration_tests.request_executors.base import ConnectionData
from bi_testing_ya.api_wrappers import (
    Req,
    Resp,
)
from bi_testing_ya.cloud_tokens import AccountCredentials
from dl_api_commons.base_models import TenantDef


@attr.s(auto_attribs=True, frozen=True)
class BIAPIClient(request_executors_base.BaseRequestExecutor):
    @classmethod
    def create_client(
        cls,
        base_url: str,
        folder_id: str,
        logger: ReportFormatter,
        account_creds: Optional[AccountCredentials] = None,
        public_api_key: Optional[str] = None,
        tenant: Optional[TenantDef] = None,
    ) -> "BIAPIClient":
        return cls.from_settings(base_url, folder_id, logger, account_creds, public_api_key, tenant=tenant)

    async def execute_request(self, request: Req) -> Resp:
        return await self._request(request)

    async def update_dataset_from_validation_response(
        self,
        validation_response_dataset: dict,
        dataset_id: str,
        require_ok: bool = True,
    ) -> Resp:
        """Updates the dataset object using the information from validation response."""
        dataset_request = Req(
            "put",
            f"/api/v1/datasets/{dataset_id}/versions/draft",
            data_json=get_dataset_json_from_validation(validation_response_dataset=validation_response_dataset),
            require_ok=require_ok,
        )
        response = await self.execute_request(dataset_request)

        return response

    async def create_dataset_from_validation_resp(
        self, validation_response_dataset: dict, dataset_name: str, base_dir: str, workbook_id: Optional[str]
    ) -> Resp:
        """Creates the dataset object using the information from validation response."""
        dataset_request = Req(
            "post",
            "/api/v1/datasets/",
            data_json=get_dataset_json_from_validation(
                validation_response_dataset=validation_response_dataset,
                dataset_name=dataset_name,
                base_dir=base_dir,
                workbook_id=workbook_id,
            ),
        )
        response = await self.execute_request(dataset_request)

        return response

    async def validate_dataset(
        self,
        dataset_json: dict,
    ) -> Resp:
        """Creates a dataset object for testing purposes."""
        dataset_val_request = Req("post", "/api/v1/datasets/validators/dataset", data_json=dataset_json)
        dataset_validation_response = await self.execute_request(dataset_val_request)
        return dataset_validation_response

    async def create_conn(
        self,
        base_dir,
        connection_name,
        connection_settings,
        workbook_id,
    ):
        conn_request = Req(
            "post",
            "/api/v1/connections/",
            data_json=get_connection_json(
                base_dir=base_dir, name=connection_name, workbook_id=workbook_id, **connection_settings
            ),
        )
        connection_response = await self.execute_request(conn_request)
        conn_id = connection_response.json["id"]
        return conn_id

    async def create_connection(self, data: ConnectionData) -> Resp:
        request = Req(
            method="post",
            url="/api/v1/connections/",
            data_json=attr.asdict(data),
        )
        return await self._request(request)

    async def get_connection(self, connection_id: str) -> Resp:
        request = Req(
            method="get",
            url=f"/api/v1/connections/{connection_id}",
        )
        return await self._request(request)

    async def get_dataset_data(self, dataset_id: str) -> Resp:
        request = Req(
            method="post",
            url=f"/api/data/v1/datasets/{dataset_id}/versions/draft/result",
            data_json={"columns": ["int_value"]},
        )
        return await self.execute_request(request)


def get_connection_json(
    *,
    connection_type,
    base_dir,
    workbook_id=None,
    host,
    port,
    username,
    password,
    database,
    name=str(uuid.uuid4()),
    edit=False,
    **kwargs,
):
    """Returns sample connection settings in json format."""
    conn_json = {"host": host, "port": port, "username": username, "password": password, "db_name": database}
    if not edit:
        conn_json["type"] = connection_type
        conn_json["dir_path"] = base_dir
        conn_json["name"] = name

    if workbook_id is not None:
        conn_json["workbook_id"] = workbook_id

    return conn_json


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


def get_add_formula_field_action_json(title: str, formula: str):
    """Returns a json for an action to add formula field."""
    return {"action": "add_field", "field": {"guid": str(uuid.uuid4()), "title": title, "formula": formula}}
