from functools import cached_property
from typing import Any

import yaml
from aiohttp import web

from dl_api_commons.base_models import TenantDef
from dl_api_commons.aiohttp.aiohttp_wrappers import RequiredResource
from bi_external_api.aiohttp_services.base import BaseView, ExtAPIRequiredResource, SerializationType
from bi_external_api.attrs_model_mapper.marshmallow import ModelMapperMarshmallow
from bi_external_api.converter.workbook_ctx_loader import WorkbookContextLoader
from bi_external_api.domain.external import get_external_model_mapper
from bi_external_api.workbook_ops.facade import WorkbookOpsFacade


class BaseWorkbookOpsView(BaseView):
    @classmethod
    def get_required_resources(cls, method_name: str) -> frozenset[RequiredResource]:
        return super().get_required_resources(method_name).union(
            {ExtAPIRequiredResource.INT_API_CLIENTS}
        )

    def create_workbook_ops_facade(self, tenant_override: TenantDef) -> WorkbookOpsFacade:
        internal_api_clients = self.app_request.internal_api_clients_factory.get_internal_api_clients(tenant_override)

        return WorkbookOpsFacade(
            workbook_ctx_loader=WorkbookContextLoader(
                internal_api_clients=internal_api_clients,
                use_workbooks_api=self.app_config.use_workbooks_api,
            ),
            internal_api_clients=internal_api_clients,
            api_type=self.app_config.api_type,
            do_add_exc_message=self.app_config.do_add_exc_message,
        )

    @cached_property
    def model_mapper(self) -> ModelMapperMarshmallow:
        return get_external_model_mapper(self.app_config.api_type)

    def adopt_response(self, resp: Any, status_code: int = 200) -> web.Response:
        json_data: dict[str, Any]

        if isinstance(resp, dict):
            json_data = resp
        else:
            schema_cls = self.model_mapper.get_schema_for_attrs_class(type(resp))

            schema = schema_cls()
            json_data = schema.dump(resp)

        requested_resp_serialization_type = self.resp_serialization_type
        effective_resp_serialization_type: SerializationType

        if requested_resp_serialization_type is None:
            requested_req_serialization_type = self.req_serialization_type

            if requested_req_serialization_type is None:
                effective_resp_serialization_type = SerializationType.json
            else:
                effective_resp_serialization_type = requested_req_serialization_type
        else:
            effective_resp_serialization_type = requested_resp_serialization_type

        if effective_resp_serialization_type == SerializationType.json:
            return web.json_response(json_data, status=status_code)
        elif effective_resp_serialization_type == SerializationType.yaml:
            return web.Response(
                content_type="text/yaml",
                body=yaml.safe_dump(
                    json_data,
                    default_flow_style=False,
                    sort_keys=False,
                    allow_unicode=True,
                ),
                status=status_code,
            )
        else:
            raise AssertionError(f"Unexpected response serialization type: {effective_resp_serialization_type}")
