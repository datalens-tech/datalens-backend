import logging

from bi_external_api.workbook_ops.public_exceptions import WorkbookOperationException

from dl_api_commons.base_models import TenantDef
from bi_api_commons_ya_cloud.models import TenantYCOrganization
from bi_external_api.domain.external import rpc, rpc_unified_v0, object_model, errors
from bi_external_api.internal_api_clients.exc_api import NotFoundErr, InvalidIDFormatError
from .workbook_rpc import WorkbookRPCView

LOGGER = logging.getLogger(__name__)


class WorkbookRPCViewUnifiedNebiusIL(
    WorkbookRPCView[rpc_unified_v0.UnifiedV0OpRequest, rpc_unified_v0.UnifiedV0OpResponse]
):
    @property
    def op_translator(self) -> rpc_unified_v0.UnifiedV0APIOperationTranslator:
        return rpc_unified_v0.UnifiedV0APIOperationTranslator()

    def translate_operation_request_public_to_private(
            self,
            rq: rpc_unified_v0.UnifiedV0OpRequest
    ) -> rpc.WorkbookOpRequest:
        return self.op_translator.translate_op_rq(rq)

    def translate_operation_response_private_to_public(
            self,
            rs: rpc.WorkbookOpResponse
    ) -> rpc_unified_v0.UnifiedV0OpResponse:
        return self.op_translator.translate_op_rs(rs)

    async def resolve_tenant_for_operation_request(self, rq: rpc_unified_v0.UnifiedV0OpRequest) -> TenantDef:
        maybe_parent = rq.get_target_parent_object()

        if isinstance(maybe_parent, object_model.ParentOrganization):
            return TenantYCOrganization(org_id=maybe_parent.org_id)

        maybe_workbook_id = rq.get_target_workbook_id()

        if maybe_workbook_id is not None:
            su_us_cli = self.app_request.internal_api_clients_factory.get_super_user_us_client()

            try:
                org_id = await su_us_cli.private_resolve_org_id_by_wb_id(workbook_id=maybe_workbook_id)
            except (NotFoundErr, InvalidIDFormatError):
                raise WorkbookOperationException(errors.ErrWorkbookOp(
                    message="Workbook was not found",
                    common_errors=[],
                    entry_errors=[],
                ))
            return TenantYCOrganization(org_id=org_id)

        raise AssertionError(f"Can not resolve organization ID for operation {type(rq)!r}.")
