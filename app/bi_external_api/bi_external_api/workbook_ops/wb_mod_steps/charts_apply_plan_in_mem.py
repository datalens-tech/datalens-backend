from __future__ import annotations

from typing import (
    Mapping,
    Optional,
    Sequence,
)

import attr

from bi_external_api.converter.charts.ad_hoc_field_extra_resolver import AdHocFieldExtraResolver
from bi_external_api.converter.charts.chart_converter import BaseChartConverter
from bi_external_api.converter.converter_ctx import ConverterContext
from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import (
    charts,
    datasets,
)
from bi_external_api.domain.internal.dl_common import (
    EntryScope,
    EntrySummary,
)
from bi_external_api.workbook_ops.wb_mod_steps.common_apply_plan_in_mem import BaseApplyInMemPlanWBModStep


@attr.s()
class StepApplyPlanInMemoryCharts(
    BaseApplyInMemPlanWBModStep[
        ext.ChartInstance,
        charts.ChartInstance,
        None,
    ]
):
    ext_inst_clz = ext.ChartInstance
    int_inst_clz = charts.ChartInstance

    async def _convert_instance_ext_to_int(
        self,
        ext_inst: ext.ChartInstance,
        int_inst_id: str,
        prev_int_inst: Optional[charts.ChartInstance],
        wb_ctx: WorkbookContext,
        converter_ctx: ConverterContext,
    ) -> tuple[charts.ChartInstance, None]:
        charts_cli = self._internal_api_clients.charts
        ds_cp_cli = self._internal_api_clients.datasets_cp

        assert charts_cli is not None

        ad_hoc_field_resolver = AdHocFieldExtraResolver(
            wb_ctx,
            ds_cp_cli,
        )
        converter = BaseChartConverter(wb_ctx, converter_ctx)

        defaulted_ext_chart = converter.fill_defaults(ext_inst.chart)

        map_ds_id_to_actions: Mapping[str, Sequence[datasets.Action]] = {
            ds_id: artifact.actions for ds_id, artifact in self._wbm_ctx.map_ds_id_to_conv_artifacts.items()
        }

        datasets_with_applied_actions = await ad_hoc_field_resolver.resolve_updates(
            converter.convert_ext_chart_ad_hoc_fields_to_chart_actions(defaulted_ext_chart),
            map_ds_id_to_source_actions=map_ds_id_to_actions,
        )
        int_chart = converter.convert_chart_ext_to_int(
            defaulted_ext_chart, datasets_with_applied_actions=datasets_with_applied_actions
        )

        return (
            charts.ChartInstance(
                summary=EntrySummary(
                    id=int_inst_id,
                    name=ext_inst.name,
                    scope=EntryScope.widget,
                    workbook_id=self.wb_id,
                ),
                chart=int_chart,
            ),
            None,
        )
