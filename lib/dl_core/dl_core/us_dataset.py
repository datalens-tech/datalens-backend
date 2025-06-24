from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Collection,
    Generator,
)

import attr

from dl_constants.enums import (
    AggregationFunction,
    DataSourceCreatedVia,
    DataSourceRole,
    DataSourceType,
    FieldType,
    ManagedBy,
    NotificationLevel,
    UserDataType,
)
from dl_core import multisource
from dl_core.base_models import (
    ObligatoryFilter,
    connection_ref_from_id,
)
from dl_core.component_errors import ComponentErrorRegistry
from dl_core.components.dependencies.primitives import FieldInterDependencyInfo
from dl_core.components.ids import (
    DEFAULT_FIELD_ID_GENERATOR_TYPE,
    FIELD_ID_GENERATOR_MAP,
    resolve_id_collisions,
)
from dl_core.data_source_spec.base import DataSourceSpec
from dl_core.data_source_spec.collection import DataSourceCollectionSpec
from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec
from dl_core.db import SchemaColumn
from dl_core.fields import (
    DirectCalculationSpec,
    ResultSchema,
)
from dl_core.i18n.localizer import Translatable
from dl_core.us_entry import (
    BaseAttrsDataModel,
    USEntry,
)
from dl_i18n.localizer_base import Localizer
from dl_rls.rls import RLS


if TYPE_CHECKING:
    from dl_core.components.ids import FieldIdGenerator


LOGGER = logging.getLogger(__name__)


class Dataset(USEntry):
    dir_name = ""
    scope = "dataset"

    @attr.s
    class DataModel(BaseAttrsDataModel):
        name: str = attr.ib()
        revision_id: str | None = attr.ib(default=None)
        load_preview_by_default: bool | None = attr.ib(default=True)
        template_enabled: bool = attr.ib(default=False)
        data_export_forbidden: bool | None = attr.ib(default=False)
        schema_version: str = attr.ib(default="1")
        result_schema: ResultSchema = attr.ib(factory=ResultSchema)
        source_collections: list[DataSourceCollectionSpec] = attr.ib(factory=list)
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

    def get_single_data_source_id(
        self,
        ignore_source_ids: Collection[str] | None = None,
    ) -> str | None:
        # FIXME: remove in the future
        ignore_source_ids = ignore_source_ids or ()
        for dsrc_coll_spec in self.data.source_collections or ():
            managed_by = dsrc_coll_spec.managed_by or ManagedBy.user
            if dsrc_coll_spec.id in ignore_source_ids or managed_by != ManagedBy.user:
                continue
            return dsrc_coll_spec.id

        return None

    def get_own_materialized_tables(self, source_id: str | None = None) -> Generator[str, None, None]:
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

    def find_data_source_configuration(
        self,
        connection_id: str | None,
        created_from: DataSourceType | None = None,
        title: str | None = None,
        parameters: dict | None = None,
    ) -> str | None:
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
            for key, value in parameters.items():
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

        return None

    def create_result_schema_field(
        self,
        column: SchemaColumn,
        field_id_generator: FieldIdGenerator | None = None,
        autofix_title: bool = True,
        avatar_id: str | None = None,
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
        if column.user_type == UserDataType.unsupported:
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

    def _dump_rls(self) -> None:
        """Remove rls entries for non-existing in result_schema fields"""
        # FIXME: this should be in the scope of result_schema management
        field_guids = [f.guid for f in self.result_schema.fields]
        self.rls.items = [rlse for rlse in self.rls.items if rlse.field_guid in field_guids]

    @property
    def created_via(self) -> DataSourceCreatedVia:
        created_via_name = self.meta.get("created_via") or DataSourceCreatedVia.user.name
        return DataSourceCreatedVia[created_via_name]

    @property
    def revision_id(self) -> str | None:
        return self.data.revision_id

    @property
    def load_preview_by_default(self) -> bool | None:
        return self.data.load_preview_by_default

    @property
    def template_enabled(self) -> bool:
        return self.data.template_enabled

    @property
    def data_export_forbidden(self) -> bool | None:
        return self.data.data_export_forbidden

    def rename_field_id_usages(self, old_id: str, new_id: str) -> None:
        self.error_registry.rename_pack(old_id=old_id, new_id=new_id)

    @property
    def schema_version(self) -> str:
        return self.data.schema_version

    def get_import_warnings_list(self, localizer: Localizer) -> list[dict]:
        warnings_list = []
        CODE_PREFIX = "NOTIF.WB_IMPORT.DS."

        if self.rls.items:
            warnings_list.append(
                dict(
                    message=localizer.translate(Translatable("notif_rls")),
                    level=NotificationLevel.info,
                    code=CODE_PREFIX + "RLS",
                )
            )
        return warnings_list

    def get_export_warnings_list(self, localizer: Localizer) -> list[dict]:
        warnings_list = []
        CODE_PREFIX = "NOTIF.WB_EXPORT.DS."

        if self.rls.items:
            warnings_list.append(
                dict(
                    message=localizer.translate(Translatable("notif_rls")),
                    level=NotificationLevel.info,
                    code=CODE_PREFIX + "RLS",
                )
            )
        return warnings_list
