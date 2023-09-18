from __future__ import annotations

import attr

from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import (
    charts,
    dashboards,
)
from bi_external_api.domain.internal.dl_common import IntModelTags
from bi_external_api.internal_api_clients.charts_api import APIClientCharts
from bi_external_api.workbook_ops.wb_mod_steps.common import BaseWBModStep
from bi_external_api.workbook_ops.wb_mod_steps.utils import TaggedStringAttrReplacerProcessor
from bi_external_api.workbook_ops.wb_modification_context import WorkbookModificationContext


@attr.s()
class StepPersistCharts(BaseWBModStep):
    @property
    def charts_client(self) -> APIClientCharts:
        may_be_client = self._internal_api_clients.charts
        assert may_be_client is not None
        return may_be_client

    async def _execute_action(self) -> WorkbookModificationContext:
        work_copy_wb_ctx = self.wb_ctx_after_prev_step

        id_replacement_map: dict[str, str] = {}

        # Save created charts
        for ext_chart_to_create in self.plan.get_items_to_create(ext.ChartInstance):
            int_inst_to_create = work_copy_wb_ctx.resolve_chart_by_name(ext_chart_to_create.name)
            ephemeral_id = int_inst_to_create.summary.id

            created_inst_summary = await self.charts_client.create_chart(
                name=int_inst_to_create.summary.name, workbook_id=self.wb_id, chart=int_inst_to_create.chart
            )

            real_id = created_inst_summary.id

            id_replacement_map[ephemeral_id] = real_id
            work_copy_wb_ctx = work_copy_wb_ctx.replace_id(charts.ChartInstance, old=ephemeral_id, new=real_id)

        # Saving modified charts
        for ext_chart_to_modify in self.plan.get_items_to_rewrite(ext.ChartInstance):
            int_inst_to_modify = work_copy_wb_ctx.resolve_chart_by_name(ext_chart_to_modify.name)

            await self.charts_client.modify_chart(
                chart=int_inst_to_modify.chart,
                chart_id=int_inst_to_modify.summary.id,
            )

        # Remap chart IDs in dashboards
        chart_id_remap_processor = TaggedStringAttrReplacerProcessor(
            tag=IntModelTags.chart_id,
            string_replacement_mapping=id_replacement_map,
        )
        work_copy_wb_ctx = work_copy_wb_ctx.replace_entries(
            dashboards.DashInstance,
            [chart_id_remap_processor.process(orig_dash) for orig_dash in work_copy_wb_ctx.dashboards],
        )

        # Removing charts
        for inst_name_to_remove in self.plan.get_item_names_to_delete(ext.ChartInstance):
            inst_to_remove_id = self.wb_ctx_before_first_step.resolve_chart_by_name(inst_name_to_remove).summary.id
            await self.charts_client.remove_chart(inst_to_remove_id)

        return self._create_modified_wbm_ctx(new_working_wb_ctx=work_copy_wb_ctx)
