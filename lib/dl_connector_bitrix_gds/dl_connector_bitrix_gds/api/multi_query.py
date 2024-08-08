from typing import ClassVar

import attr

from dl_constants.enums import UserDataType
from dl_core.fields import (
    DirectCalculationSpec,
    ResultSchema,
)
import dl_formula.core.nodes as formula_nodes
from dl_query_processing.compilation.primitives import CompiledFormulaInfo
from dl_query_processing.enums import ExecutionLevel
from dl_query_processing.multi_query.factory import MultiQueryMutatorFactoryBase
from dl_query_processing.multi_query.mutators.base import MultiQueryMutatorBase
from dl_query_processing.multi_query.mutators.splitter_based import SplitterMultiQueryMutator
from dl_query_processing.multi_query.splitters.prefiltered import PrefilteredFieldMultiQuerySplitter
from dl_query_processing.multi_query.splitters.query_fork import QueryForkQuerySplitter


@attr.s
class BitrixGDSMultiQuerySplitter(PrefilteredFieldMultiQuerySplitter):
    expr_names: ClassVar[set[str]] = {"between", ">", ">=", "<", "<=", "=="}
    data_types: ClassVar[set[UserDataType]] = {UserDataType.datetime, UserDataType.date, UserDataType.genericdatetime}

    result_schema: ResultSchema = attr.ib(kw_only=True)

    def is_pre_filter(self, formula: CompiledFormulaInfo) -> bool:
        expr = formula.formula_obj.expr
        if not isinstance(expr, formula_nodes.OperationCall) or formula.original_field_id is None:
            return False
        field = self.result_schema.by_guid(formula.original_field_id)
        if (
            field.data_type in self.data_types
            and expr.name in self.expr_names
            and not (isinstance(field.calc_spec, DirectCalculationSpec) and field.source.startswith("UF_CRM_"))
        ):
            # FIXME: Refactor this
            return True
        return False


class BitrixGDSMultiQueryMutatorFactory(MultiQueryMutatorFactoryBase):
    def get_mutators(self) -> list[MultiQueryMutatorBase]:
        return [
            SplitterMultiQueryMutator(
                splitters=[
                    BitrixGDSMultiQuerySplitter(
                        crop_to_level_type=ExecutionLevel.compeng,
                        result_schema=self.result_schema,
                    ),
                    QueryForkQuerySplitter(),
                ],
            )
        ]
