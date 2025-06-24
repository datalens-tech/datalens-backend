from __future__ import annotations

from itertools import chain
import logging
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Optional,
    Sequence,
)

from dl_api_lib.dataset.dialect import resolve_dialect_name
from dl_api_lib.query.formalization.query_formalizer import SimpleQuerySpecFormalizer
from dl_api_lib.query.formalization.query_formalizer_base import QuerySpecFormalizerBase
from dl_api_lib.query.registry import (
    get_compeng_dialect,
    is_compeng_enabled,
)
from dl_api_lib.service_registry.service_registry import ApiServiceRegistry
from dl_constants.enums import (
    DataSourceRole,
    SourceBackendType,
)
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.components.dependencies.factory import ComponentDependencyManagerFactory
from dl_core.components.dependencies.factory_base import ComponentDependencyManagerFactoryBase
from dl_core.components.editor import DatasetComponentEditor
from dl_core.components.ids import (
    AvatarId,
    FieldId,
    FieldIdValidator,
)
from dl_core.data_source.base import DataSource
from dl_core.data_source.collection import (
    DataSourceCollection,
    DataSourceCollectionFactory,
)
from dl_core.dataset_capabilities import DatasetCapabilities
from dl_core.exc import (
    FieldNotFound,
    ReferencedUSEntryNotFound,
)
from dl_core.us_dataset import Dataset
from dl_core.us_manager.us_manager import USManagerBase
from dl_core.utils import shorten_uuid
from dl_formula.core.dialect import DialectCombo
from dl_formula.core.dialect import StandardDialect as D
from dl_formula.core.dialect import from_name_and_version
import dl_formula.core.exc as formula_exc
from dl_formula.definitions.scope import Scope
from dl_formula.inspect.env import InspectionEnvironment
from dl_formula.parser.base import FormulaParser
from dl_model_tools.typed_values import BIValue
from dl_query_processing.column_registry import ColumnRegistry
from dl_query_processing.compilation.formula_compiler import FormulaCompiler
from dl_query_processing.compilation.primitives import (
    CompiledMultiQuery,
    CompiledMultiQueryBase,
    CompiledQuery,
)
from dl_query_processing.compilation.query_compiler import QueryCompiler
from dl_query_processing.compilation.query_mutator import (
    ExtendedAggregationQueryMutator,
    OptimizingQueryMutator,
    QueryMutator,
)
from dl_query_processing.compilation.specs import QuerySpec
from dl_query_processing.enums import (
    QueryType,
    SelectValueType,
)
import dl_query_processing.exc
from dl_query_processing.legend.block_legend import BlockSpec
from dl_query_processing.legend.field_legend import Legend
from dl_query_processing.multi_query.mutators.base import MultiQueryMutatorBase
from dl_query_processing.translation.multi_level_translator import MultiLevelQueryTranslator
from dl_query_processing.translation.primitives import TranslatedMultiQueryBase


if TYPE_CHECKING:
    from dl_core.components.ids import FieldIdGenerator
    from dl_core.db.elements import SchemaColumn
    from dl_core.fields import BIField
    from dl_query_processing.compilation.filter_compiler import FilterFormulaCompiler


LOGGER = logging.getLogger(__name__)


class AvatarAliasMapper:
    def __init__(self) -> None:
        self.next_idx = 1
        self.avatar_map: dict[AvatarId, str] = {}

    def __call__(self, avatar_id: AvatarId) -> str:
        try:
            shorten_uuid(avatar_id)
        except ValueError:
            # FIXME: the subquery aliases, such as `qq_i0_0_0`, aren't mapped consistently;
            # so have to discern those somehow, and skip them here.
            # For testing, just try
            # return 's' + avatar_id
            return avatar_id

        result = self.avatar_map.get(avatar_id)
        if result is not None:
            return result
        avatar_idx = self.next_idx
        self.next_idx += 1
        result = f"t{avatar_idx}"
        LOGGER.debug(f"AvatarAliasMapper: mapping {avatar_id!r} -> {result!r}")
        self.avatar_map[avatar_id] = result
        return result


INTERNALLY_APPLIED_WRAPPERS = {
    SelectValueType.array_prefix,
}


class DatasetBaseWrapper:
    _validation_mode: ClassVar[bool] = False
    _verbose_logging: ClassVar[bool] = False

    def __init__(
        self,
        ds: Dataset,
        *,
        us_manager: USManagerBase,
        block_spec: Optional[BlockSpec] = None,
        function_scopes: int = Scope.EXPLICIT_USAGE,
        debug_mode: bool = False,
    ):
        self._ds = ds
        self._us_manager = us_manager
        service_registry = self._us_manager.get_services_registry()
        assert isinstance(service_registry, ApiServiceRegistry)
        self._service_registry: ApiServiceRegistry = service_registry
        self._ds_accessor = DatasetComponentAccessor(dataset=ds)
        self._ds_editor = DatasetComponentEditor(dataset=ds)
        self._dsrc_coll_factory = DataSourceCollectionFactory(us_entry_buffer=self._us_manager.get_entry_buffer())
        self._function_scopes = function_scopes
        self._capabilities = DatasetCapabilities(
            dataset=self._ds,
            dsrc_coll_factory=self._dsrc_coll_factory,
        )
        self._ds_dep_mgr_factory = self.make_dep_mgr_factory()
        if block_spec is None:
            block_spec = BlockSpec(block_id=0, legend=Legend(items=[]), parent_block_id=None)
        self._formalizer = self.make_formalizer(query_type=block_spec.query_type)
        self._avatar_alias_mapper = AvatarAliasMapper()

        self._has_sources = False
        self.dialect: DialectCombo = D.DUMMY

        # field-dependent stuff (helper mappings)
        self.inspect_env: Optional[InspectionEnvironment] = None
        self._column_reg: Optional[ColumnRegistry] = None
        self._formula_compiler: Optional[FormulaCompiler] = None
        self._query_spec: Optional[QuerySpec] = None

        self._reload_sources()
        self._reload_formalized_specs(block_spec=block_spec)
        self._id_generator: FieldIdGenerator = self.make_id_generator()
        self._id_validator = FieldIdValidator()

        self._debug_mode = debug_mode

    @property
    def query_spec(self) -> QuerySpec:
        assert self._query_spec is not None
        return self._query_spec

    @property
    def formula_compiler(self) -> FormulaCompiler:
        assert self._formula_compiler is not None
        return self._formula_compiler

    def make_dep_mgr_factory(self) -> ComponentDependencyManagerFactoryBase:
        return ComponentDependencyManagerFactory(dataset=self._ds)

    def make_formalizer(self, query_type: QueryType) -> QuerySpecFormalizerBase:
        return SimpleQuerySpecFormalizer(
            dataset=self._ds,
            us_entry_buffer=self._us_manager.get_entry_buffer(),
            verbose_logging=self._verbose_logging,
        )

    def _get_dataset_parameter_values(self) -> dict[str, BIValue]:
        result = self._ds_accessor.get_parameter_values()

        if self._query_spec is not None:
            result.update(
                self._ds_accessor.get_parameter_values_from_specs(
                    parameter_value_specs=self._query_spec.parameter_value_specs,
                )
            )

        return result

    def _get_data_source_coll_strict(self, source_id: str) -> DataSourceCollection:
        dsrc_coll_spec = self._ds_accessor.get_data_source_coll_spec_strict(source_id=source_id)
        dsrc_coll = self._dsrc_coll_factory.get_data_source_collection(
            spec=dsrc_coll_spec,
            dataset_parameter_values=self._get_dataset_parameter_values(),
            dataset_template_enabled=self._ds_accessor.get_template_enabled(),
        )
        return dsrc_coll

    def _get_data_source_strict(self, source_id: str, role: Optional[DataSourceRole] = None) -> DataSource:
        if role is None:
            role = self.resolve_role()
        assert role is not None
        dsrc_coll = self._get_data_source_coll_strict(source_id=source_id)
        dsrc = dsrc_coll.get_strict(role=role)
        return dsrc

    def make_id_generator(self) -> FieldIdGenerator:
        id_generator_factory = self._service_registry.get_field_id_generator_factory()
        return id_generator_factory.get_field_id_generator(ds=self._ds)

    def make_formula_parser(self) -> FormulaParser:
        parser_factory = self._service_registry.get_formula_parser_factory()
        return parser_factory.get_formula_parser()

    def make_filter_compiler(self) -> FilterFormulaCompiler:
        raise NotImplementedError

    def make_query_compiler(self) -> QueryCompiler:
        assert self._column_reg is not None
        return QueryCompiler(
            dataset=self._ds,
            column_reg=self._column_reg,
            formula_compiler=self.formula_compiler,
            filter_compiler=self.make_filter_compiler(),
        )

    def make_query_mutators(self) -> Sequence[QueryMutator]:
        return [
            OptimizingQueryMutator(
                dialect=self.dialect,
                disable_optimizations=self._validation_mode,
            ),
            ExtendedAggregationQueryMutator(
                allow_empty_dimensions_for_forks=self._validation_mode,
                allow_arbitrary_toplevel_lod_dimensions=self._validation_mode,
                new_subquery_mode=True,
            ),
        ]

    def make_multi_query_mutators(self) -> Sequence[MultiQueryMutatorBase]:
        backend_type = self.get_backend_type()
        mqm_factory_factory = self._service_registry.get_multi_query_mutator_factory_factory()
        return mqm_factory_factory.get_multi_query_mutators(
            backend_type=backend_type,
            dataset=self._ds,
            dialect=self.dialect,
        )

    def make_multi_query_translator(self) -> MultiLevelQueryTranslator:
        assert self.inspect_env is not None
        assert self._column_reg is not None
        return MultiLevelQueryTranslator(
            inspect_env=self.inspect_env,
            function_scopes=self._function_scopes,
            verbose_logging=self._verbose_logging,
            source_db_columns=self._column_reg,
            avatar_alias_mapper=self._avatar_alias_mapper,
            collect_stats=not self._validation_mode,
            dialect=self.dialect,
            compeng_dialect=get_compeng_dialect() if is_compeng_enabled() else None,
        )

    def resolve_role(self) -> DataSourceRole:
        return self._capabilities.resolve_source_role()

    def get_backend_type(self) -> SourceBackendType:
        return self._capabilities.get_backend_type(role=self.resolve_role())

    def _reload_sources(self) -> None:
        source_id = self._ds.get_single_data_source_id()
        self._has_sources = source_id is not None
        # resolve database characteristics
        # never go to database from here --> only_cache=True
        if source_id is not None:
            role = self.resolve_role()
            try:
                backend_type = self._capabilities.get_backend_type(role=role)
                dialect_name = resolve_dialect_name(backend_type=backend_type)
                dsrc = self._get_data_source_strict(source_id=source_id, role=role)
                db_info = dsrc.get_cached_db_info()
                db_version = db_info.version
                self.dialect = from_name_and_version(
                    dialect_name=dialect_name,
                    dialect_version=db_version,
                )
            except ReferencedUSEntryNotFound:
                self.dialect = D.DUMMY

        if self._query_spec is not None:
            self.load_exbuilders()

    def _reload_formalized_specs(self, block_spec: Optional[BlockSpec] = None) -> None:
        assert block_spec is not None, "block_spec must not be None in this implementation"
        self._query_spec = self._formalizer.make_query_spec(block_spec=block_spec)
        if self._formula_compiler is None:
            self.load_exbuilders()
        if self._formula_compiler is not None:
            self._formula_compiler.update_environments(
                group_by_ids=[spec.field_id for spec in self.query_spec.group_by_specs],
                order_by_specs=self.query_spec.order_by_specs,
            )

    def allow_nested_window_functions(self) -> bool:
        return True

    def load_exbuilders(self) -> None:
        self.inspect_env = InspectionEnvironment()
        self._column_reg = ColumnRegistry(
            db_columns=self._generate_raw_column_list(),
            avatar_source_map={avatar.id: avatar.source_id for avatar in self._ds_accessor.get_avatar_list()},
        )

        self._formula_compiler = FormulaCompiler(
            columns=self._column_reg,
            inspect_env=self.inspect_env,
            formula_parser=self.make_formula_parser(),
            all_fields=list(self._ds.result_schema),
            group_by_ids=[spec.field_id for spec in self.query_spec.group_by_specs],
            filter_ids=[spec.field_id for spec in self.query_spec.filter_specs],
            field_wrappers={
                spec.field_id: spec.wrapper
                for spec in self.query_spec.select_specs
                if spec.wrapper.type in INTERNALLY_APPLIED_WRAPPERS
            },
            order_by_specs=self.query_spec.order_by_specs,
            mock_among_dimensions=self._validation_mode,
            allow_nested_window_functions=self.allow_nested_window_functions(),
            parameter_value_specs=self.query_spec.parameter_value_specs,
            validate_aggregations=self.query_spec.meta.query_type != QueryType.totals,
        )

    def _generate_raw_column_list(self) -> list[SchemaColumn]:
        """Generate info about columns of the data table"""

        role = self.resolve_role()
        return list(
            chain.from_iterable(
                (self._get_data_source_coll_strict(source_id=source_id).get_cached_raw_schema(role=role) or ())
                for source_id in self._ds_accessor.get_data_source_id_list()
            )
        )

    def get_field_by_id(self, field_id: FieldId) -> BIField:
        try:
            return self._ds.result_schema.by_guid(field_id)
        except KeyError:
            raise FieldNotFound(f"Field {field_id} not found in dataset") from None

    def process_compiled_query(self, compiled_query: CompiledQuery) -> CompiledMultiQueryBase:
        try:
            # Apply whole-query mutations
            for mutator in self.make_query_mutators():
                compiled_query = mutator.mutate_query(compiled_query=compiled_query)

            # Transform flat query into multi-query object
            compiled_multi_query: CompiledMultiQueryBase = CompiledMultiQuery(queries=[compiled_query])

            # Apply multi-query mutations (extended aggregations, lookups and window functions)
            multi_query_mutators = self.make_multi_query_mutators()
            for multi_mutator in multi_query_mutators:
                compiled_multi_query = multi_mutator.mutate_multi_query(compiled_multi_query)

        except formula_exc.FormulaError as err:
            raise dl_query_processing.exc.FormulaHandlingError(*err.errors) from err

        return compiled_multi_query

    def compile_and_translate_query(self, query_spec: QuerySpec) -> TranslatedMultiQueryBase:
        try:
            # Compile query from spec
            query_compiler = self.make_query_compiler()
            compiled_query = query_compiler.make_compiled_query(query_spec=query_spec)

            # Plan, slice and separate query
            compiled_multi_query = self.process_compiled_query(compiled_query=compiled_query)

            # Translate query.
            query_translator = self.make_multi_query_translator()
            translated_multi_query = query_translator.translate_multi_query(compiled_multi_query=compiled_multi_query)
        except formula_exc.FormulaError as err:
            raise dl_query_processing.exc.FormulaHandlingError(*err.errors) from err

        return translated_multi_query
