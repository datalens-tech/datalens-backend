from functools import cached_property
from typing import (
    Optional,
    Any,
)

import attr
import requests

from dl_constants.api_constants import DLHeadersCommon
from bi_external_api.attrs_model_mapper.marshmallow import ModelMapperMarshmallow
from bi_external_api.domain.external import get_external_model_mapper
from bi_external_api.enums import ExtAPIType
from bi_external_api.grpc_proxy.common import GHeaders


@attr.s(kw_only=True)
class Request:
    headers: Optional[GHeaders] = attr.ib()
    method: str = attr.ib(default="post")
    data_json: Optional[dict[str, Any]] = attr.ib(default=None)


@attr.s(auto_attribs=True)
class Response:
    status_code: int
    text: Optional[str] = None
    json_resp: Optional[dict[str, Any]] = None

    @property
    def json_resp_safe(self) -> dict[str, Any]:
        if self.json_resp is None:
            return {}
        return self.json_resp


@attr.s
class CallError(Exception):
    response: Response = attr.ib()


@attr.s
class ExtApiClient:
    base_url: str = attr.ib()
    ext_api_type: Optional[ExtAPIType] = attr.ib(default=ExtAPIType.DC)

    authorization_header: Optional[str] = attr.ib(default=None, repr=False)
    request_id: Optional[str] = attr.ib(default=None)

    @cached_property
    def mapper(self) -> ModelMapperMarshmallow:
        return get_external_model_mapper(self.ext_api_type)

    def _make_request(self, request: Request) -> Response:
        result = requests.request(
            url=f"{self.base_url}/external_api/v0/workbook/rpc",
            method="post",
            headers={
                **(
                    {DLHeadersCommon.AUTHORIZATION_TOKEN.value: self.authorization_header}
                    if self.authorization_header is not None
                    else {}
                ),
                **(
                    {DLHeadersCommon.REQUEST_ID.value: self.request_id}
                    if self.request_id is not None
                    else {}
                ),
                **(request.headers or {}),
            },
            json=request.data_json,
        )
        result_json = None
        try:
            result_json = result.json()
        except Exception:
            pass

        result_text = None
        try:
            result_text = result.text
        except Exception:
            pass

        response = Response(
            status_code=result.status_code,
            json_resp=result_json,
            text=result_text,
        )
        if response.status_code != 200:
            raise CallError(response=response)

        return response

    def get_workbook(self, workbook_id: str, headers: Optional[GHeaders] = None) -> Response:
        payload = dict(
            kind="wb_read",
            workbook_id=workbook_id,
        )
        return self._make_request(Request(headers=headers, data_json=payload))

    def create_workbook(self, project_id: str, workbook_title: str, headers: Optional[GHeaders] = None) -> Response:
        payload = dict(
            kind="wb_create",
            project_id=project_id,
            workbook_title=workbook_title,
        )
        return self._make_request(Request(headers=headers, data_json=payload))

    def update_workbook(self, workbook_id: str, workbook: dict, headers: Optional[GHeaders] = None) -> Response:
        payload = dict(
            kind="wb_modify",
            workbook_id=workbook_id,
            workbook=workbook,
        )
        return self._make_request(Request(headers=headers, data_json=payload))

    def delete_workbook(self, workbook_id: str, headers: Optional[GHeaders] = None) -> Response:
        payload = dict(
            kind="wb_delete",
            workbook_id=workbook_id,
        )
        return self._make_request(Request(headers=headers, data_json=payload))

    def create_connection(
            self,
            workbook_id: str,
            connection_name: str,
            connection_params: dict,
            secret: dict,
            headers: Optional[GHeaders] = None,
    ) -> Response:
        payload = dict(
            kind="connection_create",
            workbook_id=workbook_id,
            secret=secret,
            connection=dict(
                name=connection_name,
                connection=connection_params,
            ),
        )
        return self._make_request(Request(headers=headers, data_json=payload))

    def get_connection(
            self,
            workbook_id: str,
            connection_name: str,
            headers: Optional[GHeaders] = None,
    ) -> Response:
        payload = dict(
            kind="connection_get",
            workbook_id=workbook_id,
            name=connection_name,
        )
        return self._make_request(Request(headers=headers, data_json=payload))

    def update_connection(
            self,
            workbook_id: str,
            connection_name: str,
            connection_params: dict,
            secret: dict,
            headers: Optional[GHeaders] = None,
    ) -> Response:
        payload = dict(
            kind="connection_modify",
            workbook_id=workbook_id,
            secret=secret,
            connection=dict(
                name=connection_name,
                connection=connection_params,
            ),
        )
        return self._make_request(Request(headers=headers, data_json=payload))

    def delete_connection(
            self,
            workbook_id: str,
            connection_name: str,
            headers: Optional[GHeaders] = None,
    ) -> Response:
        payload = dict(
            kind="connection_delete",
            workbook_id=workbook_id,
            name=connection_name,
        )
        return self._make_request(Request(headers=headers, data_json=payload))

    def advice_dataset_fields(
            self,
            workbook_id: str,
            connection_name: str,
            partial_dataset: dict,
            headers: Optional[GHeaders] = None,
    ) -> Response:
        payload = dict(
            kind="advise_dataset_fields",
            workbook_id=workbook_id,
            connection_name=connection_name,
            partial_dataset=partial_dataset,
        )
        return self._make_request(Request(headers=headers, data_json=payload))

    def list_workbooks(
            self,
            project_id: str,
            headers: Optional[GHeaders] = None,
    ) -> Response:
        payload = dict(
            kind="wb_list",
            project_id=project_id,
        )
        return self._make_request(Request(headers=headers, data_json=payload))
