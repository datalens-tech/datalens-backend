import abc
import enum
import logging
from typing import Any, Type, ClassVar, TypeVar, Generic, Protocol

import attr
from aiohttp import web
from marshmallow import ValidationError

from dl_api_commons.base_models import TenantDef, TenantCommon
from bi_api_commons_ya_cloud.models import TenantDCProject
from bi_external_api.domain.external import rpc, rpc_dc, errors, ErrWorkbookOp
from bi_external_api.http_views.workbook_base import BaseWorkbookOpsView
from bi_external_api.internal_api_clients.exc_api import NotFoundErr, InvalidIDFormatError
from bi_external_api.workbook_ops.facade import WorkbookOpsFacade
from bi_external_api.workbook_ops.public_exceptions import PublicException, WorkbookOperationException

LOGGER = logging.getLogger(__name__)


class OperationRequestProtocol(Protocol):
    @classmethod
    @abc.abstractmethod
    def get_operation_kind(cls) -> enum.Enum:
        raise NotImplementedError()


_RPC_RQ_TV = TypeVar("_RPC_RQ_TV", bound=OperationRequestProtocol)
_RPC_RS_TV = TypeVar("_RPC_RS_TV")


class WorkbookRPCView(BaseWorkbookOpsView, Generic[_RPC_RQ_TV, _RPC_RS_TV]):
    endpoint_code = "WorkbookRPC"

    rq_base_cls: ClassVar[Type[_RPC_RQ_TV]]
    rs_base_cls: ClassVar[Type[_RPC_RS_TV]]

    @abc.abstractmethod
    def translate_operation_request_public_to_private(self, rq: _RPC_RQ_TV) -> rpc.WorkbookOpRequest:
        raise NotImplementedError()

    @abc.abstractmethod
    def translate_operation_response_private_to_public(self, rs: rpc.WorkbookOpResponse) -> _RPC_RS_TV:
        raise NotImplementedError()

    @abc.abstractmethod
    async def resolve_tenant_for_operation_request(self, rq: _RPC_RQ_TV) -> TenantDef:
        raise NotImplementedError()

    async def post(self) -> web.Response:
        try:
            rpc_req = await self._get_rpc_request()
        except ValidationError as validation_err:
            return self.adopt_response(
                dict(
                    kind="request_scheme_violation",
                    messages=validation_err.messages
                ),
                status_code=400
            )

        op_kind = rpc_req.get_operation_kind()

        try:
            LOGGER.info("Executing external API procedure", extra=dict(
                event_code="ext_api_rcp_execute_start",
                rpc_req_kind=op_kind.name,
                rpc_req_json_str=self.model_mapper.dump_external_model_to_str(rpc_req),
            ))
            tenant = await self.resolve_tenant_for_operation_request(rpc_req)

            wb_ops_facade = self.create_workbook_ops_facade(tenant_override=tenant)

            private_rpc_req = self.translate_operation_request_public_to_private(rpc_req)
            rpc_resp = await self._execute_rpc(wb_ops_facade, private_rpc_req)
        except PublicException as exc:
            LOGGER.info("Got public exception during RPC execution", exc_info=True)
            exc_data = exc.data
            if isinstance(exc_data, ErrWorkbookOp):
                req_id = self.app_request.rci.request_id
                exc_data = attr.evolve(exc_data, request_id=req_id)  # type: ignore

            LOGGER.exception("External API procedure execution failed", extra=dict(
                event_code="ext_api_rcp_execute_failure",
                rpc_req_kind=op_kind.name,
                rpc_resp_json_str=self.model_mapper.dump_external_model_to_str(exc_data),
            ))
            try:
                # TODO FIX: Try to handle in error handling middleware
                return self.adopt_response(exc_data, status_code=400)
            except Exception as exc_serialization_exc:
                LOGGER.exception("Got exception during public exception serialization")
                raise exc_serialization_exc
        except Exception:
            LOGGER.critical("External API procedure execution failed critically", extra=dict(
                event_code="ext_api_rcp_execute_critical_failure",
                rpc_req_kind=op_kind.name,
            ))
            raise

        LOGGER.info("External API procedure execution done", extra=dict(
            event_code="ext_api_rcp_execute_success",
            rpc_req_kind=op_kind.name,
            rpc_resp_json_str=self.model_mapper.dump_external_model_to_str(rpc_resp),
        ))
        return self.adopt_response(
            self.translate_operation_response_private_to_public(rpc_resp)
        )

    @classmethod
    async def _execute_rpc(cls, ops_facade: WorkbookOpsFacade, req: rpc.WorkbookOpRequest) -> Any:
        if isinstance(req, rpc.WorkbookReadRequest):
            return await ops_facade.read_workbook(req)
        if isinstance(req, rpc.WorkbookWriteRequest):
            return await ops_facade.write_workbook(req)
        if isinstance(req, rpc.WorkbookDeleteRequest):
            return await ops_facade.delete_workbook(req)
        if isinstance(req, rpc.TrueWorkbookCreateRequest):
            return await ops_facade.create_dc_workbook(req)
        if isinstance(req, rpc.FakeWorkbookCreateRequest):
            return await ops_facade.create_fake_workbook(req)
        if isinstance(req, rpc.AdviseDatasetFieldsRequest):
            return await ops_facade.advise_dataset_fields(req)
        if isinstance(req, rpc.ConnectionCreateRequest):
            return await ops_facade.create_connection(req)
        if isinstance(req, rpc.ConnectionModifyRequest):
            return await ops_facade.modify_connection(req)
        if isinstance(req, rpc.ConnectionDeleteRequest):
            return await ops_facade.delete_connection(req)
        if isinstance(req, rpc.ConnectionGetRequest):
            return await ops_facade.get_connection(req)
        if isinstance(req, rpc.WorkbookClusterizeRequest):
            return await ops_facade.clusterize_workbook(req)
        if isinstance(req, rpc.WorkbookListRequest):
            return await ops_facade.list_workbooks(req)

        raise AssertionError(f"Unexpected RPC request: {type(req)}")

    async def _get_rpc_request(self) -> _RPC_RQ_TV:
        req_body_dict = await self.get_request_dict()
        schema_cls = self.model_mapper.get_or_create_schema_for_attrs_class(self.rq_base_cls)
        schema = schema_cls()

        return schema.load(req_body_dict)


class WorkbookRPCViewPrivate(WorkbookRPCView[rpc.WorkbookOpRequest, rpc.WorkbookOpResponse]):
    rq_base_cls = rpc.WorkbookOpRequest
    rs_base_cls = rpc.WorkbookOpResponse

    async def resolve_tenant_for_operation_request(self, rq: rpc.WorkbookOpRequest) -> TenantDef:
        return TenantCommon()

    def translate_operation_request_public_to_private(self, rq: rpc.WorkbookOpRequest) -> rpc.WorkbookOpRequest:
        return rq

    def translate_operation_response_private_to_public(self, rs: rpc.WorkbookOpResponse) -> rpc.WorkbookOpResponse:
        return rs


class WorkbookRPCViewDoubleCloud(WorkbookRPCView[rpc_dc.DCOpRequest, rpc_dc.DCOpResponse]):
    rq_base_cls = rpc_dc.DCOpRequest
    rs_base_cls = rpc_dc.DCOpResponse

    @property
    def op_translator(self) -> rpc_dc.DoubleCloudPublicAPIOperationTranslator:
        return rpc_dc.DoubleCloudPublicAPIOperationTranslator()

    async def resolve_tenant_for_operation_request(self, rq: rpc_dc.DCOpRequest) -> TenantDef:
        may_be_project_id = rq.get_target_project_id()

        if may_be_project_id is not None:
            return TenantDCProject(project_id=may_be_project_id)

        may_be_workbook_id = rq.get_target_workbook_id()

        if may_be_workbook_id is not None:
            su_us_cli = self.app_request.internal_api_clients_factory.get_super_user_us_client()

            try:
                project_id = await su_us_cli.private_resolve_project_id_by_wb_id(workbook_id=may_be_workbook_id)
            except (NotFoundErr, InvalidIDFormatError):
                raise WorkbookOperationException(errors.ErrWorkbookOp(
                    message="Workbook was not found",
                    common_errors=[],
                    entry_errors=[],
                ))
            return TenantDCProject(project_id=project_id)

        raise AssertionError(f"Can not resolve project ID for operation {type(rq)!r}.")

    def translate_operation_request_public_to_private(self, rq: rpc_dc.DCOpRequest) -> rpc.WorkbookOpRequest:
        return self.op_translator.translate_op_rq(rq)

    def translate_operation_response_private_to_public(self, rs: rpc.WorkbookOpResponse) -> rpc_dc.DCOpResponse:
        return self.op_translator.translate_op_rs(rs)
