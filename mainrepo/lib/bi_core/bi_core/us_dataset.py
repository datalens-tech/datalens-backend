from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Collection,
    Generator,
    Optional,
)

import attr

from bi_constants.enums import (
    AggregationFunction,
    BIType,
    CreateDSFrom,
    DataSourceCreatedVia,
    DataSourceRole,
    FieldType,
    ManagedBy,
)
from bi_core import multisource
from bi_core.base_models import (
    ObligatoryFilter,
    connection_ref_from_id,
)
from bi_core.component_errors import ComponentErrorRegistry
from bi_core.components.dependencies.primitives import FieldInterDependencyInfo
from bi_core.components.ids import (
    DEFAULT_FIELD_ID_GENERATOR_TYPE,
    FIELD_ID_GENERATOR_MAP,
    resolve_id_collisions,
)
from bi_core.data_source_spec.base import DataSourceSpec
from bi_core.data_source_spec.collection import (
    DataSourceCollectionSpec,
    DataSourceCollectionSpecBase,
)
from bi_core.data_source_spec.sql import StandardSQLDataSourceSpec
from bi_core.db import SchemaColumn
from bi_core.fields import (
    DirectCalculationSpec,
    ResultSchema,
)
from bi_core.rls import RLS
from bi_core.us_entry import (
    BaseAttrsDataModel,
    USEntry,
)

if TYPE_CHECKING:
    from bi_core.components.ids import FieldIdGenerator


LOGGER = logging.getLogger(__name__)


class Dataset(USEntry):
    dir_name = ""  # type: ignore  # TODO: fix
    scope = "dataset"  # type: ignore  # TODO: fix

    @attr.s
    class DataModel(BaseAttrsDataModel):
        name: str = attr.ib()
        revision_id: Optional[str] = attr.ib(default=None)
        result_schema: ResultSchema = attr.ib(factory=ResultSchema)
        source_collections: list[DataSourceCollectionSpecBase] = attr.ib(factory=list)
        source_avatars: list[multisource.SourceAvatar] = attr.ib(factory=list)
        avatar_relations: list[multisource.AvatarRelation] = attr.ib(factory=list)
        rls: RLS = attr.ib(factory=RLS)

        component_errors: ComponentErrorRegistry = attr.ib(factory=ComponentErrorRegistry)
        obligatory_filters: list[ObligatoryFilter] = attr.ib(factory=list)

        @attr.s
        class ResultSchemaAux:
            inter_dependencies: FieldInterDependencyInfo = attr.ib(factory=FieldInterDependencyInfo)

        result_schema_aux: ResultSchemaAux = attr.ib(factory=ResultSchemaAux)

    @property
    def rls(self) -> RLS:
        return self.data.rls

    @property
    def error_registry(self) -> ComponentErrorRegistry:
        return self.data.component_errors

    def get_single_data_source_id(self, ignore_source_ids: Optional[Collection[str]] = None) -> str:  # type: ignore  # TODO: fix
        # FIXME: remove in the future
        ignore_source_ids = ignore_source_ids or ()
        for dsrc_coll_spec in self.data.source_collections or ():
            managed_by = dsrc_coll_spec.managed_by or ManagedBy.user
            if dsrc_coll_spec.id in ignore_source_ids or managed_by != ManagedBy.user:
                continue
            return dsrc_coll_spec.id

    def get_own_materialized_tables(self, source_id: Optional[str] = None) -> Generator[str, None, None]:
        for dsrc_coll_spec in self.data.source_collections or ():
            if not dsrc_coll_spec or source_id and dsrc_coll_spec.id != source_id:
                continue
            if not isinstance(dsrc_coll_spec, DataSourceCollectionSpec):
                continue
            for role in (DataSourceRole.materialization, DataSourceRole.sample):
                dsrc_spec = dsrc_coll_spec.get_for_role(role)
                if dsrc_spec is None:
                    continue
                assert isinstance(dsrc_spec, StandardSQLDataSourceSpec)
                if dsrc_spec.table_name is not None:
                    yield dsrc_spec.table_name

    def find_data_source_configuration(  # type: ignore  # TODO: fix
        self,
        connection_id: Optional[str],
        created_from: Optional[CreateDSFrom] = None,
        title: Optional[str] = None,
        parameters: Optional[dict] = None,
    ) -> Optional[str]:
        """
        Check whether dataset already has a source with the given configuration.
        Return ``None`` if it doesn't exist, otherwise return the corresponding ``source_id``.
        """
        # TODO refactor using lazy datasets instead of configs

        if connection_id is None:
            return None

        if parameters is None:
            parameters = {}

        def spec_matches_parameters(existing_spec: DataSourceSpec) -> bool:
            # FIXME: Refactor
            for key, value in parameters.items():  # type: ignore  # TODO: fix
                if getattr(existing_spec, key, None) != value:
                    return False
            return True

        connection_ref = connection_ref_from_id(connection_id=connection_id)

        for dsrc_coll_spec in self.data.source_collections:
            if (
                isinstance(dsrc_coll_spec, DataSourceCollectionSpec)
                and dsrc_coll_spec.origin is not None
                and dsrc_coll_spec.origin.connection_ref == connection_ref
                and dsrc_coll_spec.origin.source_type == created_from
                and spec_matches_parameters(dsrc_coll_spec.origin)
                and (True if title is None else dsrc_coll_spec.title == title)
            ):
                # reference matches params
                return dsrc_coll_spec.id

    def create_result_schema_field(
        self,
        column: SchemaColumn,
        field_id_generator: Optional[FieldIdGenerator] = None,
        autofix_title: bool = True,
        avatar_id: Optional[str] = None,
    ) -> dict:
        title = column.title
        if autofix_title:
            existing_titles = {f.title for f in self.result_schema.fields}
            title = resolve_id_collisions(item=title, existing_items=existing_titles, formatter="{} ({})")

        if field_id_generator is None:
            FieldIdGeneratorClass = FIELD_ID_GENERATOR_MAP[DEFAULT_FIELD_ID_GENERATOR_TYPE]
            field_id_generator = FieldIdGeneratorClass(dataset=self)

        guid = field_id_generator.make_field_id(title=title)

        hidden = False
        if column.user_type == BIType.unsupported:
            # Auto-hide because it's unselectable.
            hidden = True

        column_data = {
            "title": title,
            "guid": guid,
            "hidden": hidden,
            "description": column.description,
            "aggregation": AggregationFunction.none.name,
            "cast": column.user_type.name,
            "type": FieldType.DIMENSION.name,
            "data_type": column.user_type.name,
            "initial_data_type": column.user_type.name,
            "valid": True,
            "has_auto_aggregation": column.has_auto_aggregation,
            "lock_aggregation": column.lock_aggregation,
            "managed_by": ManagedBy.user.name,
            "calc_spec": DirectCalculationSpec(
                avatar_id=avatar_id,
                source=column.name,
            ),
        }
        return column_data

    @property
    def result_schema(self) -> ResultSchema:
        return self.data.result_schema

    def _dump_rls(self):  # type: ignore  # TODO: fix
        """Remove rls entries for non-existing in result_schema fields"""
        # FIXME: this should be in the scope of result_schema management
        field_guids = [f.guid for f in self.result_schema.fields]
        self.rls.items = [rlse for rlse in self.rls.items if rlse.field_guid in field_guids]

    @property
    def created_via(self) -> DataSourceCreatedVia:
        created_via_name = self.meta.get("created_via") or DataSourceCreatedVia.user.name
        return DataSourceCreatedVia[created_via_name]

    @property
    def revision_id(self) -> Optional[str]:
        return self.data.revision_id

    def rename_field_id_usages(self, old_id: str, new_id: str) -> None:
        self.error_registry.rename_pack(old_id=old_id, new_id=new_id)
