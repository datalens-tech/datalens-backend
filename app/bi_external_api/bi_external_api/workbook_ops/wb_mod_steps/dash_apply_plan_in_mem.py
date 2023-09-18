from __future__ import annotations

from typing import Optional

import attr

from bi_external_api.converter.converter_ctx import ConverterContext
from bi_external_api.converter.dash import DashboardConverter
from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import dashboards
from bi_external_api.domain.internal.dl_common import (
    EntryScope,
    EntrySummary,
)
from bi_external_api.workbook_ops.wb_mod_steps.common_apply_plan_in_mem import BaseApplyInMemPlanWBModStep


@attr.s()
class StepApplyPlanInMemoryDashboards(
    BaseApplyInMemPlanWBModStep[
        ext.DashInstance,
        dashboards.DashInstance,
        None,
    ]
):
    ext_inst_clz = ext.DashInstance
    int_inst_clz = dashboards.DashInstance

    async def _convert_instance_ext_to_int(
        self,
        ext_inst: ext.DashInstance,
        int_inst_id: str,
        prev_int_inst: Optional[dashboards.DashInstance],
        wb_ctx: WorkbookContext,
        converter_ctx: ConverterContext,
    ) -> tuple[dashboards.DashInstance, None]:
        converter = DashboardConverter(wb_context=wb_ctx)

        defaulted_ext_dash = converter.fill_ext_defaults(ext_inst.dashboard)
        int_dash = converter.convert_ext_to_int(defaulted_ext_dash)

        return (
            dashboards.DashInstance(
                summary=EntrySummary(
                    id=int_inst_id,
                    name=ext_inst.name,
                    scope=EntryScope.dash,
                    workbook_id=self.wb_id,
                ),
                dash=int_dash,
            ),
            None,
        )
