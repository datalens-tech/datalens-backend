from dl_api_commons.base_models import TenantDef, TenantCommon
from bi_external_api.domain.external import rpc, rpc_ya_team
from bi_external_api.http_views.workbook_rpc import WorkbookRPCView


class WorkbookRPCViewYaTeam(WorkbookRPCView[rpc_ya_team.YaTeamOpRequest, rpc_ya_team.YaTeamOpResponse]):
    rq_base_cls = rpc_ya_team.YaTeamOpRequest
    rs_base_cls = rpc_ya_team.YaTeamOpResponse

    @property
    def op_translator(self) -> rpc_ya_team.YaTeamPublicAPIOperationTranslator:
        return rpc_ya_team.YaTeamPublicAPIOperationTranslator()

    async def resolve_tenant_for_operation_request(
            self,
            rq: rpc_ya_team.YaTeamOpRequest
    ) -> TenantDef:
        return TenantCommon()

    def translate_operation_request_public_to_private(
            self,
            rq: rpc_ya_team.YaTeamOpRequest
    ) -> rpc.WorkbookOpRequest:
        return self.op_translator.translate_op_rq(rq)

    def translate_operation_response_private_to_public(
            self,
            rs: rpc.WorkbookOpResponse
    ) -> rpc_ya_team.YaTeamOpResponse:
        return self.op_translator.translate_op_rs(rs)
