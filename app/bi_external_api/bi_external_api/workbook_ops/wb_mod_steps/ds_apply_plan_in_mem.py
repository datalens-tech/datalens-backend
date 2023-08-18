from __future__ import annotations

from typing import Optional

import attr

from bi_external_api.converter.converter_ctx import ConverterContext
from bi_external_api.converter.main import DatasetConverter
from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import datasets
from bi_external_api.domain.internal.dl_common import EntrySummary, EntryScope
from bi_external_api.workbook_ops.wb_mod_steps.common_apply_plan_in_mem import BaseApplyInMemPlanWBModStep
from bi_external_api.workbook_ops.wb_modification_context import WorkbookModificationContext, DatasetConvArtifacts


@attr.s()
class StepApplyPlanInMemoryDatasets(
    BaseApplyInMemPlanWBModStep[
        ext.DatasetInstance,
        datasets.DatasetInstance,
        DatasetConvArtifacts,
    ]
):
    ext_inst_clz = ext.DatasetInstance
    int_inst_clz = datasets.DatasetInstance

    def _finalize_wb_modification_ctx(
            self,
            wb_ctx_with_applied_updates: WorkbookContext,
            map_entry_id_conversion_artifact: dict[str, DatasetConvArtifacts]
    ) -> WorkbookModificationContext:
        wbm = super()._finalize_wb_modification_ctx(wb_ctx_with_applied_updates, map_entry_id_conversion_artifact)

        original_map_ds_id_to_cov_artifacts = wbm.map_ds_id_to_conv_artifacts
        # TODO FIX: Clarify message
        assert not (original_map_ds_id_to_cov_artifacts.keys() & map_entry_id_conversion_artifact.keys())

        new_map_ds_id_to_conv_artifacts: dict[str, DatasetConvArtifacts] = {
            **original_map_ds_id_to_cov_artifacts,
            **map_entry_id_conversion_artifact,
        }
        return wbm.clone(map_ds_id_to_conv_artifacts=new_map_ds_id_to_conv_artifacts)

    async def _convert_instance_ext_to_int(
            self,
            *,
            ext_inst: ext.DatasetInstance,
            int_inst_id: str,
            prev_int_inst: Optional[datasets.DatasetInstance],
            wb_ctx: WorkbookContext,
            converter_ctx: ConverterContext,
    ) -> tuple[datasets.DatasetInstance, DatasetConvArtifacts]:
        converter = DatasetConverter(wb_ctx, converter_ctx)
        actions = converter.convert_public_dataset_to_actions(ext_inst.dataset)
        api_client = self._internal_api_clients.datasets_cp

        internal_dataset, validation_resp = await api_client.build_dataset_config_by_actions(actions)
        # TODO FIX: Check why restored instance is not equal
        # restored_dataset = converter.convert_internal_dataset_to_public_dataset(internal_dataset)
        # assert ext_dataset_instance.dataset == restored_dataset

        return datasets.DatasetInstance(
            dataset=internal_dataset,
            summary=EntrySummary(
                id=int_inst_id,
                name=ext_inst.name,
                scope=EntryScope.dataset,
                workbook_id=self.wb_id,
            )
        ), DatasetConvArtifacts(validation_response=validation_resp, actions=actions)
