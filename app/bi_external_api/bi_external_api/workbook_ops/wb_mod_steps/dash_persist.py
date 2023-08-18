from __future__ import annotations

import attr

from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import dashboards
from bi_external_api.internal_api_clients.dash_api import APIClientDashboard
from bi_external_api.workbook_ops.wb_mod_steps.common import BaseWBModStep
from bi_external_api.workbook_ops.wb_modification_context import WorkbookModificationContext


@attr.s()
class StepPersistDashboards(BaseWBModStep):
    @property
    def dash_client(self) -> APIClientDashboard:
        may_be_client = self._internal_api_clients.dash
        assert may_be_client is not None
        return may_be_client

    async def _execute_action(self) -> WorkbookModificationContext:
        work_copy_wb_ctx = self.wb_ctx_after_prev_step

        id_replacement_map: dict[str, str] = {}

        # Save created dashboards
        for ext_dash_to_create in self.plan.get_items_to_create(ext.DashInstance):
            int_inst_to_create = work_copy_wb_ctx.resolve_dash_by_name(ext_dash_to_create.name)
            ephemeral_id = int_inst_to_create.summary.id

            created_inst_summary = await self.dash_client.create_dashboard(
                name=int_inst_to_create.summary.name,
                workbook_id=self.wb_id,
                dash=int_inst_to_create.dash
            )

            real_id = created_inst_summary.id

            id_replacement_map[ephemeral_id] = real_id
            work_copy_wb_ctx = work_copy_wb_ctx.replace_id(dashboards.DashInstance, old=ephemeral_id, new=real_id)

        # Saving modified dashboards
        for ext_dash_to_modify in self.plan.get_items_to_rewrite(ext.DashInstance):
            int_inst_to_modify = work_copy_wb_ctx.resolve_dash_by_name(ext_dash_to_modify.name)

            await self.dash_client.modify_dashboard(
                dash=int_inst_to_modify.dash,
                dash_id=int_inst_to_modify.summary.id,
            )

        # Removing dashboards
        for inst_name_to_remove in self.plan.get_item_names_to_delete(ext.DashInstance):
            inst_to_remove_id = self.wb_ctx_before_first_step.resolve_dash_by_name(inst_name_to_remove).summary.id
            await self.dash_client.remove_dashboard(inst_to_remove_id)

        return self._create_modified_wbm_ctx(new_working_wb_ctx=work_copy_wb_ctx)
