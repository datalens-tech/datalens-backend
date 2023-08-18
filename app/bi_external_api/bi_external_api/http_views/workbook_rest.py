from aiohttp import web

from bi_external_api.domain.external.rpc import WorkbookReadRequest
from bi_external_api.http_views.workbook_base import BaseWorkbookOpsView


class WorkbookRestInstanceView(BaseWorkbookOpsView):
    endpoint_code = "WorkbookRestInstance"

    @property
    def current_workbook_id(self) -> str:
        return self.request.match_info["workbook_id"]

    async def get(self) -> web.Response:
        tenant = self.app_request.rci.tenant
        # Later will be implemented via WB ID resolution
        #  But it seems that this view will be removed at all
        assert tenant is not None, \
            "Tenant is not defined in RCI and custom resolving is not yet implemented for this handle"

        response = await self.create_workbook_ops_facade(tenant).read_workbook(WorkbookReadRequest(
            workbook_id=self.current_workbook_id,
        ))
        return self.adopt_response(response)
