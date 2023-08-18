from typing import Any

import attr

from bi_api_commons.client.base import Req
from bi_api_commons.client.common import CommonInternalAPIClient
from bi_external_api.domain.internal import charts as chart_mod
from bi_external_api.domain.internal.dl_common import EntrySummary, EntryScope
from bi_external_api.domain.internal.mapper import internal_models_mapper
from .constants import ChartOpCode


@attr.s()
class APIClientCharts(CommonInternalAPIClient):
    @property
    def api_prefix(self) -> str:
        if self._use_workbooks_api:
            return "/private/api/charts/v1"
        else:
            return "/api/charts/v1"

    async def get_chart(self, id: str) -> chart_mod.ChartInstance:
        op_code = ChartOpCode.GET

        chart_schema = internal_models_mapper.get_or_create_schema_for_attrs_class(chart_mod.Chart)()

        req = Req(
            method="GET",
            url=f"{self.api_prefix}/charts/{id}",
            require_ok=False,
        )
        resp = await self.make_request(req)
        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

        with self.deserialization_err_handler(resp, op_code=op_code):
            chart = chart_schema.load(resp.json["data"])

        true_workbooks = self._use_workbooks_api

        summary: EntrySummary = EntrySummary.from_key(
            scope=EntryScope.widget,
            entry_id=id,
            key=resp.json["key"],
            true_workbook=true_workbooks,
            workbook_id=resp.json["workbookId"] if true_workbooks else None
        )

        return chart_mod.ChartInstance(
            chart=chart,
            summary=summary,
        )

    async def create_chart(self, chart: chart_mod.Chart, *, workbook_id: str, name: str) -> EntrySummary:
        op_code = ChartOpCode.CREATE

        data: dict[str, Any] = dict(
            data=self._dump_chart(chart),
            template="datalens",
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
            url=f"{self.api_prefix}/charts",
            data_json=data,
            require_ok=False,
        )
        resp = await self.make_request(req)
        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

        return EntrySummary(
            id=resp.json["entryId"],
            name=name,
            workbook_id=workbook_id,
            scope=EntryScope.widget,
        )

    async def modify_chart(self, chart: chart_mod.Chart, *, chart_id: str) -> None:
        op_code = ChartOpCode.MODIFY

        data = dict(
            data=self._dump_chart(chart),
            mode="publish",
            template="datalens",
        )
        req = Req(
            method="POST",
            url=f"{self.api_prefix}/charts/{chart_id}",
            data_json=data,
            require_ok=False,
        )
        resp = await self.make_request(req)
        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

    async def remove_chart(self, chart_id: str) -> None:
        op_code = ChartOpCode.REMOVE

        resp = await self.make_request(
            Req(method="DELETE", url=f"{self.api_prefix}/charts/{chart_id}", require_ok=False)
        )
        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

    @classmethod
    def _dump_chart(cls, chart: chart_mod.Chart) -> dict[str, Any]:
        chart_schema = internal_models_mapper.get_or_create_schema_for_attrs_class(chart_mod.Chart)()
        return chart_schema.dump(chart)
