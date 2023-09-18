from __future__ import annotations

import abc
from typing import (
    Mapping,
    Type,
)

import attr

from bi_external_api.converter.converter_ctx import ConverterContext
from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import (
    charts,
    dashboards,
    datasets,
)
from bi_external_api.domain.internal.dl_common import EntryInstance
from bi_external_api.exc_tooling import ExcComposer
from bi_external_api.internal_api_clients.main import InternalAPIClients
from bi_external_api.workbook_ops.diff_tools import WorkbookTransitionPlan
from bi_external_api.workbook_ops.exc_composer import WbErrHandlingCtx
from bi_external_api.workbook_ops.wb_modification_context import WorkbookModificationContext


@attr.s(frozen=True)
class BaseWBModStep(metaclass=abc.ABCMeta):
    _wbm_ctx: WorkbookModificationContext = attr.ib()
    _converter_ctx: ConverterContext = attr.ib()
    _internal_api_clients: InternalAPIClients = attr.ib()
    # TODO FIX: Consider passing in execute() args or ensure single shot execution
    _exc_composer: ExcComposer[WbErrHandlingCtx] = attr.ib()

    @property
    def wb_id(self) -> str:
        return self._wbm_ctx.wb_id

    @property
    def wb_ctx_after_prev_step(self) -> WorkbookContext:
        return self._wbm_ctx.working_wb_context

    @property
    def wb_ctx_before_first_step(self) -> WorkbookContext:
        return self._wbm_ctx.initial_wb_context

    @property
    def plan(self) -> WorkbookTransitionPlan:
        return self._wbm_ctx.transition_plan

    #
    # Interface
    #
    async def execute(self) -> WorkbookModificationContext:
        return await self._execute_action()

    @abc.abstractmethod
    async def _execute_action(self) -> WorkbookModificationContext:
        raise NotImplementedError()

    #
    # Helpers
    #
    def _remove_instances_from_wb_ctx_according_plan(
        self,
        wb_context: WorkbookContext,
        clz: Type[EntryInstance],
    ) -> WorkbookContext:
        map_inst_clz_int_to_ext: Mapping[Type[EntryInstance], Type[ext.EntryInstance]] = {
            datasets.DatasetInstance: ext.DatasetInstance,
            charts.ChartInstance: ext.ChartInstance,
            dashboards.DashInstance: ext.DashInstance,
        }

        return wb_context.remove_entries(
            clz,
            [
                wb_context.ref(name=item_name)
                for item_name in self.plan.get_item_names_to_delete(map_inst_clz_int_to_ext[clz])
            ],
        )

    def _create_modified_wbm_ctx(self, new_working_wb_ctx: WorkbookContext) -> WorkbookModificationContext:
        return self._wbm_ctx.clone(wb_ctx=new_working_wb_ctx)
