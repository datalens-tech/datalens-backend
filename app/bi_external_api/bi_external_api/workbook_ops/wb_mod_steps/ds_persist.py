from __future__ import annotations

import attr

from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import datasets, charts, dashboards
from bi_external_api.domain.internal.dl_common import IntModelTags
from bi_external_api.workbook_ops.wb_mod_steps.common import BaseWBModStep
from bi_external_api.workbook_ops.wb_mod_steps.utils import TaggedStringAttrReplacerProcessor
from bi_external_api.workbook_ops.wb_modification_context import WorkbookModificationContext


@attr.s()
class StepPersistDatasets(BaseWBModStep):
    async def _execute_action(self) -> WorkbookModificationContext:
        work_copy_wb_ctx = self.wb_ctx_after_prev_step

        id_replacement_map: dict[str, str] = {}

        # Save created datasets
        for ext_ds_to_create in self.plan.get_items_to_create(ext.DatasetInstance):
            int_ds_inst_to_create = work_copy_wb_ctx.resolve_dataset_by_name(ext_ds_to_create.name)
            ephemeral_id = int_ds_inst_to_create.summary.id
            conv_artifacts = self._wbm_ctx.map_ds_id_to_conv_artifacts[ephemeral_id]

            created_ds_summary = await self._internal_api_clients.datasets_cp.create_dataset(
                name=int_ds_inst_to_create.summary.name,
                validation_resp=conv_artifacts.validation_response,
                workbook_id=self.wb_id,
            )

            real_id = created_ds_summary.id

            id_replacement_map[ephemeral_id] = real_id
            work_copy_wb_ctx = work_copy_wb_ctx.replace_id(datasets.DatasetInstance, old=ephemeral_id, new=real_id)

        # Saving modified datasets
        for ext_ds_to_modify in self.plan.get_items_to_rewrite(ext.DatasetInstance):
            int_ds_inst_to_modify = work_copy_wb_ctx.resolve_dataset_by_name(ext_ds_to_modify.name)
            conv_artifacts = self._wbm_ctx.map_ds_id_to_conv_artifacts[int_ds_inst_to_modify.summary.id]

            # Extracting revision ID from originally loaded dataset
            # If revision ID in request will not match stored one
            # dataset API will reject request
            int_ds_inst_before_first_step = self.wb_ctx_before_first_step.resolve_dataset_by_id(
                int_ds_inst_to_modify.summary.id
            )

            await self._internal_api_clients.datasets_cp.modify_dataset(
                validation_resp=conv_artifacts.validation_response,
                ds_id=int_ds_inst_to_modify.summary.id,
                backend_local_revision_id=int_ds_inst_before_first_step.backend_local_revision_id,
            )

        # Remap dataset IDs in charts/dashboards
        ds_id_remap_processor = TaggedStringAttrReplacerProcessor(
            tag=IntModelTags.dataset_id,
            string_replacement_mapping=id_replacement_map,
        )
        work_copy_wb_ctx = work_copy_wb_ctx.replace_entries(
            charts.ChartInstance,
            [
                ds_id_remap_processor.process(orig_chart)
                for orig_chart in work_copy_wb_ctx.charts
            ]
        )
        work_copy_wb_ctx = work_copy_wb_ctx.replace_entries(
            dashboards.DashInstance,
            [
                ds_id_remap_processor.process(orig_chart)
                for orig_chart in work_copy_wb_ctx.dashboards
            ]
        )

        # Removing datasets
        for ds_name_to_remove in self.plan.get_item_names_to_delete(ext.DatasetInstance):
            ds_to_remove_id = self.wb_ctx_before_first_step.resolve_dataset_by_name(ds_name_to_remove).summary.id
            await self._internal_api_clients.datasets_cp.delete_dataset(ds_to_remove_id)

        return self._create_modified_wbm_ctx(new_working_wb_ctx=work_copy_wb_ctx)
