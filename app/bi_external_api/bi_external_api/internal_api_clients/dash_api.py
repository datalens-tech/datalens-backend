from typing import Any

import attr

from bi_api_commons.client.base import Req
from bi_api_commons.client.common import CommonInternalAPIClient
from bi_external_api.domain.internal import dashboards
from bi_external_api.domain.internal.dl_common import EntrySummary, EntryScope
from bi_external_api.domain.internal.mapper import internal_models_mapper
from .constants import DashOpCode


@attr.s()
class APIClientDashboard(CommonInternalAPIClient):
    @property
    def api_prefix(self) -> str:
        if self._use_workbooks_api:
            return "/private/api/dash/v1"
        else:
            return "/v1"

    async def get_dashboard(self, id: str) -> dashboards.DashInstance:
        op_code = DashOpCode.GET

        dash_schema = internal_models_mapper.get_or_create_schema_for_attrs_class(dashboards.Dashboard)()

        req = Req(
            method="GET",
            url=f"{self.api_prefix}/dashboards/{id}",
            require_ok=False,
        )
        resp = await self.make_request(req)
        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

        with self.deserialization_err_handler(resp, op_code=op_code):
            dash = dash_schema.load(resp.json["data"])

        true_workbooks = self._use_workbooks_api

        summary: EntrySummary = EntrySummary.from_key(
            scope=EntryScope.dash,
            entry_id=id,
            key=resp.json["key"],
            true_workbook=true_workbooks,
            workbook_id=resp.json["workbookId"] if true_workbooks else None
        )

        return dashboards.DashInstance(
            summary=summary,
            dash=dash,
        )

    async def create_dashboard(self, dash: dashboards.Dashboard, *, workbook_id: str, name: str) -> EntrySummary:
        op_code = DashOpCode.CREATE

        data: dict[str, Any] = dict(
            data=self._serialize_dash(dash),
        )

        # Naming
        if self._use_workbooks_api:
            data.update(
                workbookId=workbook_id,
                name=name,
            )
        else:
            data.update(
                key=f"{workbook_id}/{name}",
            )

        req = Req(
            method="POST",
            url=f"{self.api_prefix}/dashboards",
            require_ok=False,
            data_json=data
        )
        resp = await self.make_request(req)

        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

        return EntrySummary(
            id=resp.json["entryId"],
            name=name,
            workbook_id=workbook_id,
            scope=EntryScope.dash,
        )

    async def modify_dashboard(self, dash: dashboards.Dashboard, *, dash_id: str) -> None:
        op_code = DashOpCode.MODIFY

        data = dict(
            mode="publish",
            data=self._serialize_dash(dash)
        )
        req = Req(
            method="POST",
            url=f"{self.api_prefix}/dashboards/{dash_id}",
            require_ok=False,
            data_json=data
        )
        resp = await self.make_request(req)
        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

    async def remove_dashboard(self, dash_id: str) -> None:
        op_code = DashOpCode.REMOVE

        resp = await self.make_request(
            Req(method="DELETE", url=f"{self.api_prefix}/dashboards/{dash_id}", require_ok=False)
        )
        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

    @staticmethod
    def _serialize_dash(dash: dashboards.Dashboard) -> dict[str, Any]:
        dash_schema = internal_models_mapper.get_or_create_schema_for_attrs_class(dashboards.Dashboard)()
        return dash_schema.dump(dash)
