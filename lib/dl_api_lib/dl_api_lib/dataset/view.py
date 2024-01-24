from __future__ import annotations

import asyncio
import logging
from typing import (
    TYPE_CHECKING,
    Type,
)

from dl_api_commons.base_models import RequestContextInfo
from dl_api_lib.dataset.base_wrapper import DatasetBaseWrapper
from dl_api_lib.query.formalization.query_formalizer import (
    DataQuerySpecFormalizer,
    TotalsSpecFormalizer,
    ValueDistinctSpecFormalizer,
    ValueRangeSpecFormalizer,
)
from dl_api_lib.query.registry import get_filter_formula_compiler_cls
from dl_constants.enums import (
    CalcMode,
    DataSourceRole,
    ProcessorType,
)
from dl_core.fields import BIField
from dl_core.us_connection_base import ClassicConnectionSQL
from dl_core.us_dataset import Dataset
from dl_core.us_manager.us_manager import USManagerBase
from dl_query_processing.enums import QueryType
from dl_query_processing.execution.exec_info import QueryExecutionInfo
from dl_query_processing.execution.executor import QueryExecutor
from dl_query_processing.execution.executor_base import QueryExecutorBase
from dl_query_processing.execution.primitives import ExecutedQuery
from dl_query_processing.legend.block_legend import BlockSpec


if TYPE_CHECKING:
    from dl_constants.types import TBIDataValue
    from dl_query_processing.compilation.filter_compiler import FilterFormulaCompiler


LOGGER = logging.getLogger(__name__)


class DatasetView(DatasetBaseWrapper):
    """
    A ``Dataset`` wrapper for performing data queries.

    The whole process can be broken down into the following steps:

    1. Formalization.
        Performed by subclasses of ``QuerySpecFormalizerBase``.

        Various lists defining the future query such as
        ``select_ids``, ``group_by_ids``, ``order_by_specs``, etc.
        are normalized (see more detailed info about that in formalizer descriptions)
        and combined into a single object, namely ``QuerySpec``.

        Result: ``QuerySpec`` instance

    2. Compilation.
        Performed by subclasses of ``RawQueryCompilerBase``;
        ``FormulaCompiler`` and ``FilterFormulaCompiler`` subclasses
        are used internally.

        Query compilation starts with a ``QuerySpec`` instance as input.
        The query spec tells the compiler which fields are to be used
        in which parts of the query. The compiler generates formula objects
        for each item in the spec with all the respective wrappers
        (custom operations for filters, ASC/DESC for ordering)
        and combines them in a ``CompiledQuery`` object,
        which somewhat resembles the structure of the original ``QuerySpec`` object,
        as will the other query representations in the subsequent steps.

        Result: ``CompiledQuery`` instance

    3. Planning.
        Performed by subclasses of ``ExecutionPlanner``.

        All parts of the compiled query are analyzed,
        as a result of which the following happens:
        - The total number of execution levels (nested sub-queries) is determined.
        - For each expression its top execution level is determined.
          It may be different for each formula (e.g. some filters are applied at source_db level,
          whole others - at various compeng levels).
        - For each expression a slicer configuration is gnerated.
          It will determine how it is cut up into parts for these execution levels.

        An example is described in detail in ``MainExecutionPlanner``

        Result: ``ExecutionPlan`` instance

    4. Slicing.
        Performed by ``DefaultQuerySlicer``.

        Every part of the query is cut into several formulas for each execution level
        using the configurations from the previous step (Planning).

        The resulting object still resembles the query classes from all previous steps
        with formulas organized into sections corresponding to the clauses
        of an SQL query. But it is worth noting that not all of these sections
        will be applied to the top-level query. Each "formula" in its every section
        (select, group_by, where, etc.) is really a collection of:
        - the top-level formula that will be applied to its corresponding clause
        - aliased lower level formulas that will go to the SELECT clause of querues
          executed at lower levels

        Result: ``SlicedQuery`` instance

    5. Separation.
        Performed by ``QuerySeparator``.

        The sliced query is separated into what will actually represent
        the real queries to be performed at each execution level.
        Multiple queries per execution level;
        multiple execution levels in the resulting object
        (as many as was determined at the Planning stage).

        Result: ``CompiledMultiLevelQuery`` instance

    6. Translation.
        TODO

    7. Execution.
        TODO

    8. Postprocessing.
        TODO
    """

    _SOURCE_DB_PROCESSOR_TYPE = ProcessorType.SOURCE_DB
    _COMPENG_PROCESSOR_TYPE = ProcessorType.ASYNCPG

    _verbose_logging = True

    def __init__(
        self,
        ds: Dataset,
        *,
        us_manager: USManagerBase,
        block_spec: BlockSpec,
        rci: RequestContextInfo,
    ):
        self._rci = rci
        self._query_type = block_spec.query_type  # FIXME: Remove
        self._compeng_semaphore = asyncio.Semaphore()
        super().__init__(
            ds=ds, block_spec=block_spec, us_manager=us_manager, debug_mode=self._rci.x_dl_debug_mode or False
        )

    def make_formalizer(self, query_type: QueryType) -> DataQuerySpecFormalizer:
        query_form_cls: Type[DataQuerySpecFormalizer]
        if query_type == QueryType.value_range:
            query_form_cls = ValueRangeSpecFormalizer
        elif query_type == QueryType.distinct:
            query_form_cls = ValueDistinctSpecFormalizer
        elif query_type == QueryType.totals:
            query_form_cls = TotalsSpecFormalizer
        else:
            query_form_cls = DataQuerySpecFormalizer
        return query_form_cls(
            verbose_logging=self._verbose_logging,
            dataset=self._ds,
            rci=self._rci,
            role=self.resolve_role(),
            dep_mgr_factory=self._ds_dep_mgr_factory,
            us_entry_buffer=self._us_manager.get_entry_buffer(),
            service_registry=self._service_registry,
        )

    def make_filter_compiler(self) -> FilterFormulaCompiler:
        backend_type = self.get_backend_type()
        filter_cmp_cls = get_filter_formula_compiler_cls(backend_type=backend_type)
        return filter_cmp_cls(formula_compiler=self.formula_compiler)

    def make_query_executor(self, allow_cache_usage: bool) -> QueryExecutorBase:
        return QueryExecutor(
            dataset=self._ds,
            avatar_alias_mapper=self._avatar_alias_mapper,
            compeng_processor_type=self._COMPENG_PROCESSOR_TYPE,
            source_db_processor_type=self._SOURCE_DB_PROCESSOR_TYPE,
            allow_cache_usage=allow_cache_usage,
            us_manager=self._us_manager,
            compeng_semaphore=self._compeng_semaphore,
        )

    @property
    def is_preview(self) -> bool:
        return self._query_type == QueryType.preview

    def resolve_role(self) -> DataSourceRole:
        return self._capabilities.resolve_source_role(for_preview=self.is_preview)

    def build_exec_info(self) -> QueryExecutionInfo:
        LOGGER.info(f"Select field IDs: {[spec.field_id for spec in self.query_spec.select_specs]}")

        assert (
            self._formula_compiler is not None and self.inspect_env is not None
        ), "perhaps the sources were not reloaded properly"

        translated_multi_query = self.compile_and_translate_query(query_spec=self.query_spec)

        role = self.resolve_role()

        target_connections: list[ClassicConnectionSQL] = []
        for avatar_id in translated_multi_query.get_base_root_from_ids():
            avatar = self._ds_accessor.get_avatar_strict(avatar_id=avatar_id)
            dsrc = self._get_data_source_strict(source_id=avatar.source_id, role=role)
            connection = dsrc.connection
            if connection not in target_connections:
                target_connections.append(connection)  # type: ignore  # 2024-01-24 # TODO: Argument 1 to "append" of "list" has incompatible type "ConnectionBase"; expected "ClassicConnectionSQL"  [arg-type]

        return QueryExecutionInfo(
            role=role,
            translated_multi_query=translated_multi_query,
            target_connections=target_connections,
        )

    def fast_get_expression_value_range(self) -> tuple[BIField, TBIDataValue, TBIDataValue]:
        """Try to get fast (cached or pre-defined) range from data source"""

        assert len(self.query_spec.select_specs) == 2, "Fast value range is supported only for single field (MIN+MAX)"
        select_field_ids = {item_spec.field_id for item_spec in self.query_spec.select_specs}
        assert len(select_field_ids) == 1, "Got more than one field ID for range"
        field_id = next(iter(select_field_ids))
        field = self._ds.result_schema.by_guid(field_id)
        if field.calc_mode != CalcMode.direct:
            return field, None, None

        avatar_id = field.avatar_id
        if avatar_id is None:
            return field, None, None

        source_ids = self._ds_accessor.get_data_source_id_list()
        if len(source_ids) != 1:
            return field, None, None

        avatar = self._ds_accessor.get_avatar_strict(avatar_id=avatar_id)
        dsrc = self._get_data_source_strict(source_id=avatar.source_id, role=self.resolve_role())

        min_value, max_value = dsrc.get_expression_value_range(field.source)

        return field, min_value, max_value

    async def get_data_async(
        self,
        exec_info: QueryExecutionInfo,
        allow_cache_usage: bool = True,
    ) -> ExecutedQuery:
        executor = self.make_query_executor(
            allow_cache_usage=allow_cache_usage,
        )
        return await executor.execute_query(exec_info)
