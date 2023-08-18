from __future__ import annotations

from typing import Mapping, Sequence, Optional, Any

import attr

from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain.internal import datasets
from bi_external_api.workbook_ops.diff_tools import WorkbookTransitionPlan


@attr.s(frozen=True, hash=False, eq=False, order=False)
class DatasetConvArtifacts:
    validation_response: dict = attr.ib()
    actions: Sequence[datasets.Action] = attr.ib()


@attr.s(frozen=True)
class WorkbookModificationContext:
    wb_id: str = attr.ib()
    initial_wb_context: WorkbookContext = attr.ib()
    working_wb_context: WorkbookContext = attr.ib()
    transition_plan: WorkbookTransitionPlan = attr.ib()
    map_ds_id_to_conv_artifacts: Mapping[str, DatasetConvArtifacts] = attr.ib()

    @classmethod
    def create(
            cls,
            wb_id: str,
            plan: WorkbookTransitionPlan,
            initial_wb_context: WorkbookContext
    ) -> WorkbookModificationContext:
        return cls(
            wb_id=wb_id,
            initial_wb_context=initial_wb_context,
            working_wb_context=initial_wb_context,
            transition_plan=plan,
            map_ds_id_to_conv_artifacts={},
        )

    def clone(
            self,
            wb_ctx: Optional[WorkbookContext] = None,
            map_ds_id_to_conv_artifacts: Optional[Mapping[str, DatasetConvArtifacts]] = None,
    ) -> WorkbookModificationContext:
        updates: dict[str, Any] = {}

        if wb_ctx is not None:
            updates.update(
                working_wb_context=wb_ctx,
            )
        if map_ds_id_to_conv_artifacts is not None:
            updates.update(
                map_ds_id_to_conv_artifacts=map_ds_id_to_conv_artifacts,
            )
        return attr.evolve(self, **updates)
