import abc
import contextlib
from typing import (
    ClassVar,
    Generic,
    Iterator,
    Optional,
    Type,
    TypeVar,
)

import attr
import shortuuid

from bi_external_api.converter.converter_ctx import ConverterContext
from bi_external_api.converter.workbook import (
    EntryRef,
    WorkbookContext,
)
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import dl_common
from bi_external_api.workbook_ops.exc_composer import WbErrHandlingCtx
from bi_external_api.workbook_ops.wb_mod_steps.common import BaseWBModStep
from bi_external_api.workbook_ops.wb_modification_context import WorkbookModificationContext

_EXT_INST_TV = TypeVar("_EXT_INST_TV", bound=ext.EntryInstance)
_INT_INST_TV = TypeVar("_INT_INST_TV", bound=dl_common.EntryInstance)
_CONVERSION_ARTIFACT_TV = TypeVar("_CONVERSION_ARTIFACT_TV")


@attr.s(frozen=True)
class BaseApplyInMemPlanWBModStep(
    BaseWBModStep, Generic[_EXT_INST_TV, _INT_INST_TV, _CONVERSION_ARTIFACT_TV], metaclass=abc.ABCMeta
):
    ext_inst_clz: ClassVar[Type[_EXT_INST_TV]]
    int_inst_clz: ClassVar[Type[_INT_INST_TV]]

    def create_entry_summary(self, *, id: str, name: str) -> dl_common.EntrySummary:
        return dl_common.EntrySummary(
            id=id,
            name=name,
            scope=self.int_inst_clz.SCOPE,
            workbook_id=self.wb_id,
        )

    @contextlib.contextmanager
    def instance_conversion_error_postpone(self, inst: _EXT_INST_TV) -> Iterator[None]:
        ctx = WbErrHandlingCtx(
            entry_name=inst.name,
        )
        with self._exc_composer.postponed_error(ctx):
            yield

    async def _execute_action(self) -> WorkbookModificationContext:
        work_copy_wb_ctx = self.wb_ctx_after_prev_step

        work_copy_wb_ctx = self._remove_instances_from_wb_ctx_according_plan(
            wb_context=work_copy_wb_ctx,
            clz=self.int_inst_clz,
        )

        map_entry_id_conversion_artifact: dict[str, _CONVERSION_ARTIFACT_TV] = {}

        int_instances_to_create: list[_INT_INST_TV] = []
        int_instances_to_update: list[_INT_INST_TV] = []

        # This errors will be added to workbook context
        #  to get correct exceptions during conversion of dependent entities
        taint_info_to_create_list: list[tuple[dl_common.EntrySummary, Exception]] = []
        taint_info_to_update_list: list[tuple[EntryRef, Exception]] = []

        # Creating internal configs for new instances
        for ext_inst_to_create in self.plan.get_items_to_create(self.ext_inst_clz):
            with self.instance_conversion_error_postpone(ext_inst_to_create):
                generated_id = f"ephemeral_{shortuuid.uuid()}"
                try:
                    int_inst_to_create, conversion_artifact = await self._convert_instance_ext_to_int(
                        ext_inst=ext_inst_to_create,
                        int_inst_id=generated_id,
                        prev_int_inst=None,
                        wb_ctx=work_copy_wb_ctx,
                        converter_ctx=self._converter_ctx,
                    )
                    map_entry_id_conversion_artifact[generated_id] = conversion_artifact
                    int_instances_to_create.append(int_inst_to_create)
                except Exception as exc:
                    taint_info_to_create_list.append(
                        (
                            self.create_entry_summary(id=generated_id, name=ext_inst_to_create.name),
                            exc,
                        )
                    )
                    # To be registered by exc composer
                    raise exc

        # Creating internal configs for modified instances
        for ext_inst_to_update in self.plan.get_items_to_rewrite(self.ext_inst_clz):
            with self.instance_conversion_error_postpone(ext_inst_to_update):
                existing_int_inst = self.wb_ctx_before_first_step.resolve_entry(
                    self.int_inst_clz,
                    self.wb_ctx_before_first_step.ref(name=ext_inst_to_update.name),
                )
                assert ext_inst_to_update.name == existing_int_inst.summary.name

                try:
                    int_inst_to_modify, conversion_artifact = await self._convert_instance_ext_to_int(
                        ext_inst=ext_inst_to_update,
                        int_inst_id=existing_int_inst.summary.id,
                        prev_int_inst=existing_int_inst,
                        wb_ctx=work_copy_wb_ctx,
                        converter_ctx=self._converter_ctx,
                    )
                    map_entry_id_conversion_artifact[existing_int_inst.summary.id] = conversion_artifact
                    int_instances_to_update.append(int_inst_to_modify)
                except Exception as exc:
                    taint_info_to_update_list.append((work_copy_wb_ctx.ref(id=existing_int_inst.summary.id), exc))
                    # To be registered by exc composer
                    raise exc

        # Adding to WB context created/modified instances
        work_copy_wb_ctx = (
            work_copy_wb_ctx.taint_existing_entries(self.int_inst_clz, taint_info_to_update_list)
            .replace_entries(self.int_inst_clz, int_instances_to_update)
            .add_entries(int_instances_to_create)
            .add_tainted_entries(taint_info_to_create_list)
        )

        return self._finalize_wb_modification_ctx(
            wb_ctx_with_applied_updates=work_copy_wb_ctx,
            map_entry_id_conversion_artifact=map_entry_id_conversion_artifact,
        )

    def _finalize_wb_modification_ctx(
        self,
        wb_ctx_with_applied_updates: WorkbookContext,
        map_entry_id_conversion_artifact: dict[str, _CONVERSION_ARTIFACT_TV],
    ) -> WorkbookModificationContext:
        return self._create_modified_wbm_ctx(new_working_wb_ctx=wb_ctx_with_applied_updates)

    @abc.abstractmethod
    async def _convert_instance_ext_to_int(
        self,
        *,
        ext_inst: _EXT_INST_TV,
        int_inst_id: str,
        prev_int_inst: Optional[_INT_INST_TV],
        wb_ctx: WorkbookContext,
        converter_ctx: ConverterContext,
    ) -> tuple[_INT_INST_TV, _CONVERSION_ARTIFACT_TV]:
        raise NotImplementedError()
