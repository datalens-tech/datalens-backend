from __future__ import annotations

from copy import deepcopy
import logging
from typing import (
    Any,
    Optional,
)

from dl_api_commons.flask.required_resources import RequiredResourceCommon
from dl_api_lib.api_common.dataset_loader import (
    DatasetApiLoader,
    DatasetUpdateInfo,
)
from dl_api_lib.app.control_api.resources.base import BIResource
from dl_api_lib.dataset.dialect import resolve_dialect_name
from dl_api_lib.dataset.utils import (
    allow_rls_for_dataset,
    get_dataset_conn_types,
)
from dl_api_lib.enums import (
    BI_TYPE_AGGREGATIONS,
    CASTS_BY_TYPE,
)
from dl_api_lib.exc import DatasetExportError
from dl_api_lib.query.registry import (
    get_compeng_dialect,
    is_compeng_executable,
)
from dl_api_lib.service_registry.service_registry import ApiServiceRegistry
from dl_constants.enums import (
    AggregationFunction,
    BinaryJoinOperator,
    ConnectionType,
    ManagedBy,
    RLSSubjectType,
    UserDataType,
)
from dl_constants.exc import (
    DEFAULT_ERR_CODE_API_PREFIX,
    GLOBAL_ERR_PREFIX,
)
from dl_core.backend_types import get_backend_type
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.data_source.base import DbInfo
from dl_core.dataset_capabilities import DatasetCapabilities
from dl_core.exc import (
    ReferencedUSEntryNotFound,
    UnexpectedUSEntryType,
    USObjectNotFoundException,
)
from dl_core.services_registry.top_level import ServicesRegistry
from dl_core.us_dataset import (
    Dataset,
    DataSourceRole,
)
from dl_core.us_manager.local_cache import USEntryBuffer
from dl_formula.core.dialect import (
    DialectCombo,
    StandardDialect,
    from_name_and_version,
)
from dl_rls.serializer import FieldRLSSerializer


LOGGER = logging.getLogger(__name__)

DEFAULT_DATABASE_DIALECT = StandardDialect.DUMMY
DEFAULT_MAX_AVATARS = 32
DEFAULT_MAX_CONNECTIONS = 20
UNAVAILABLE = object()


class DatasetResource(BIResource):
    """
    Base class for handlers that need to serialize and deserialize datasets
    """

    @classmethod
    def create_dataset_api_loader(cls) -> DatasetApiLoader:
        return DatasetApiLoader(
            service_registry=cls.get_service_registry(),
        )

    @classmethod
    def get_dataset(
        cls,
        dataset_id: Optional[str],
        body: dict,
        load_dependencies: bool = True,
        params: Optional[dict] = None,
    ) -> tuple[Dataset, DatasetUpdateInfo]:
        us_manager = (
            cls.get_service_us_manager()
            if RequiredResourceCommon.US_HEADERS_TOKEN in cls.REQUIRED_RESOURCES
            else cls.get_us_manager()
        )
        if dataset_id:
            try:
                dataset = us_manager.get_by_id(dataset_id, expected_type=Dataset, params=params)
            except UnexpectedUSEntryType as e:
                raise USObjectNotFoundException("Dataset with id {} does not exist".format(dataset_id)) from e
        else:
            dataset = Dataset.create_from_dict(
                Dataset.DataModel(name=""),  # TODO: Remove name - it's not used, but is required
                us_manager=us_manager,
            )

        if load_dependencies:
            us_manager.load_dependencies(dataset)

        loader = cls.create_dataset_api_loader()
        update_info = loader.update_dataset_from_body(
            dataset=dataset,
            us_manager=us_manager,
            dataset_data=body.get("dataset"),
            allow_settings_change=True,  # TODO: BI-6307 disable in the future
        )
        return dataset, update_info

    @classmethod
    def dump_dataset_data(
        cls,
        dataset: Dataset,
        service_registry: ServicesRegistry,
        us_entry_buffer: USEntryBuffer,
        conn_id_mapping: Optional[dict] = None,
    ) -> dict:
        ds_accessor = DatasetComponentAccessor(dataset=dataset)
        dsrc_coll_factory = service_registry.get_data_source_collection_factory(us_entry_buffer=us_entry_buffer)

        data: dict[str, Any] = {}
        # generate info about sources
        sources = []
        preview_enabled_result = True
        dataset_parameter_values = ds_accessor.get_parameter_values()
        dataset_template_enabled = ds_accessor.get_template_enabled()

        for source_id in ds_accessor.get_data_source_id_list():
            dsrc_coll_spec = ds_accessor.get_data_source_coll_spec_strict(source_id=source_id)
            dsrc_coll = dsrc_coll_factory.get_data_source_collection(
                spec=dsrc_coll_spec,
                dataset_parameter_values=dataset_parameter_values,
                dataset_template_enabled=dataset_template_enabled,
            )

            origin_dsrc = dsrc_coll.get_strict(role=DataSourceRole.origin)
            connection_id = dsrc_coll.get_connection_id(DataSourceRole.origin)
            if conn_id_mapping is not None:
                if connection_id not in conn_id_mapping:
                    LOGGER.info(
                        'Can not find "%s" in conn id mapping for source with id %s',
                        connection_id,
                        source_id,
                    )
                    raise DatasetExportError("Can not export dataset without a connection")  # TODO BI-6296
                else:
                    connection_id = conn_id_mapping[connection_id]

            sources.append(
                {
                    "id": source_id,
                    "title": dsrc_coll.title or origin_dsrc.default_title,
                    "connection_id": connection_id,
                    "managed_by": dsrc_coll.managed_by,
                    "valid": dsrc_coll.valid,
                    "source_type": origin_dsrc.spec.source_type,
                    "raw_schema": origin_dsrc.saved_raw_schema,
                    "index_info_set": origin_dsrc.saved_index_info_set,
                    "parameters": origin_dsrc.get_parameters(),
                    "parameter_hash": dsrc_coll.get_param_hash(),
                }
            )
            if not origin_dsrc.preview_enabled:
                preview_enabled_result = origin_dsrc.preview_enabled
        data["sources"] = sources

        data["preview_enabled"] = preview_enabled_result

        # generate info about source avatars
        data["source_avatars"] = ds_accessor.get_avatar_list()

        # generate info about source relations
        data["avatar_relations"] = ds_accessor.get_avatar_relation_list()

        # fields (result_schema)
        result_schema = []
        for field in dataset.result_schema:
            field_data = field._asdict()
            field_data["autoaggregated"] = field.autoaggregated
            field_data["aggregation_locked"] = field.aggregation_locked
            result_schema.append(field_data)
        data["result_schema"] = result_schema
        data["result_schema_aux"] = dataset.data.result_schema_aux

        # rls
        rls = {}
        rls_v2 = {}
        if allow_rls_for_dataset(dataset):
            for field in dataset.result_schema:
                field_rls = [e for e in dataset.rls.items if e.field_guid == field.guid]
                if field_rls:
                    rls[field.guid] = FieldRLSSerializer.to_text_config(field_rls)
                field_rls2 = [e for e in field_rls if e.subject.subject_type != RLSSubjectType.notfound]
                if field_rls2:
                    rls_v2[field.guid] = field_rls2
        data["rls"] = rls
        data["rls2"] = rls_v2

        data["obligatory_filters"] = ds_accessor.get_obligatory_filter_list()

        data["component_errors"] = deepcopy(dataset.error_registry)
        for item in data["component_errors"].items:
            for error in item.errors:
                if error.code[:2] != [GLOBAL_ERR_PREFIX, DEFAULT_ERR_CODE_API_PREFIX]:
                    error.code = [GLOBAL_ERR_PREFIX, DEFAULT_ERR_CODE_API_PREFIX] + error.code

        data["created_via"] = dataset.created_via

        data["revision_id"] = dataset.revision_id

        data["load_preview_by_default"] = dataset.load_preview_by_default
        data["template_enabled"] = dataset.template_enabled
        data["data_export_forbidden"] = dataset.data_export_forbidden

        # annotation
        data["annotation"] = dataset.annotation

        return {"dataset": data}

    @classmethod
    def dump_option_data(
        cls,
        dataset: Dataset,
        service_registry: ApiServiceRegistry,
        us_entry_buffer: USEntryBuffer,
    ) -> dict:
        ds_accessor = DatasetComponentAccessor(dataset=dataset)
        dsrc_coll_factory = service_registry.get_data_source_collection_factory(us_entry_buffer=us_entry_buffer)
        sfm = service_registry.get_supported_functions_manager()
        capabilities = DatasetCapabilities(dataset=dataset, dsrc_coll_factory=dsrc_coll_factory)

        opt_data: dict[str, Any] = {}
        opt_data["preview"] = dict(
            enabled=capabilities.supports_preview(),
        )
        opt_data["join"] = dict(
            types=capabilities.get_supported_join_types(),
            operators=[BinaryJoinOperator.eq],
        )
        role = capabilities.resolve_source_role()  # the "main" role, not for preview

        # Determine dialect
        dialect: Optional[DialectCombo] = DEFAULT_DATABASE_DIALECT
        one_source_id = dataset.get_single_data_source_id()
        db_info: Optional[DbInfo]

        dataset_parameter_values = ds_accessor.get_parameter_values()
        dataset_template_enabled = ds_accessor.get_template_enabled()

        if one_source_id is not None:
            try:
                dsrc_coll_spec = ds_accessor.get_data_source_coll_spec_strict(source_id=one_source_id)
                dsrc_coll = dsrc_coll_factory.get_data_source_collection(
                    spec=dsrc_coll_spec,
                    dataset_parameter_values=dataset_parameter_values,
                    dataset_template_enabled=dataset_template_enabled,
                )
                dsrc = dsrc_coll.get_strict(role=role)
                db_info = dsrc.get_cached_db_info()
            except ReferencedUSEntryNotFound:
                db_info = None

            if db_info is not None:
                backend_type = capabilities.get_backend_type(role=role)
                dialect_name = resolve_dialect_name(backend_type=backend_type)
                dialect = from_name_and_version(
                    dialect_name=dialect_name,
                    dialect_version=db_info.version,
                )
                if not dialect:
                    LOGGER.error(
                        "Failed to get dialect from SA dialect %r and DB version %r", dialect_name.name, db_info.version
                    )
                    dialect = DEFAULT_DATABASE_DIALECT

        assert dialect is not None
        funcs_dialect = dialect
        conn_types = set(get_dataset_conn_types(dataset=dataset, us_entry_buffer=us_entry_buffer))
        is_compeng_executable_set = {is_compeng_executable(get_backend_type(conn_type)) for conn_type in conn_types}
        if len(is_compeng_executable_set) == 1 and next(iter(is_compeng_executable_set)):
            funcs_dialect = get_compeng_dialect()

        opt_data["data_types"] = dict(
            items=[
                dict(
                    type=user_type,
                    aggregations=sfm.get_supported_aggregations(dialect=funcs_dialect, user_type=user_type),
                    casts=CASTS_BY_TYPE.get(user_type, []),
                    filter_operations=sfm.get_supported_filters(dialect=funcs_dialect, user_type=user_type),
                )
                for user_type in UserDataType
            ],
        )

        opt_data["sources"] = dict(items=[])
        connection_ids = set()
        connection_types: set[Optional[ConnectionType]] = set()

        for source_id in ds_accessor.get_data_source_id_list():
            dsrc_coll_spec = ds_accessor.get_data_source_coll_spec_strict(source_id=source_id)
            dsrc_coll = dsrc_coll_factory.get_data_source_collection(
                spec=dsrc_coll_spec,
                dataset_parameter_values=dataset_parameter_values,
                dataset_template_enabled=dataset_template_enabled,
            )
            if dsrc_coll.managed_by != ManagedBy.user:
                continue
            dsrc = dsrc_coll.get_strict(role=DataSourceRole.origin)
            opt_data["sources"]["items"].append(
                dict(
                    id=source_id,
                    schema_update_enabled=dsrc.supports_schema_update,
                )
            )
            connection_id = dsrc_coll.get_connection_id(role=DataSourceRole.origin)
            if connection_id is not None:
                connection_ids.add(connection_id)
                connection_types.add(dsrc.conn_type)

        opt_data["sources"]["compatible_types"] = [
            dict(source_type=source_type) for source_type in capabilities.get_compatible_source_types()
        ]

        only_one_connection = len(connection_ids) <= 1

        opt_data["connections"] = dict(items=[])
        for conn_id in sorted(connection_ids):
            replacement_types = []
            repl_conn_types = capabilities.get_compatible_connection_types(ignore_connection_ids=[conn_id])
            for conn_type in repl_conn_types:
                # it seems we can do this check once
                if not only_one_connection:
                    continue
                item = dict(conn_type=conn_type)
                if item not in replacement_types:
                    replacement_types.append(item)

            opt_data["connections"]["items"].append(
                dict(
                    id=conn_id,
                    replacement_types=replacement_types,
                )
            )

        compatible_conn_types: list[dict] = []
        for conn_type in capabilities.get_compatible_connection_types():
            if connection_ids:  # There already are connections in the dataset
                continue
            compatible_conn_types.append(dict(conn_type=conn_type))
        opt_data["connections"]["compatible_types"] = compatible_conn_types

        opt_data["connections"]["max"] = DEFAULT_MAX_CONNECTIONS if compatible_conn_types else 1

        opt_data["schema_update_enabled"] = any(
            [dsrc_data["schema_update_enabled"] for dsrc_data in opt_data["sources"]["items"]]
        )

        opt_data["source_avatars"] = dict(
            # if JOINs are supported, then new tables can be added
            max=DEFAULT_MAX_AVATARS if capabilities.get_supported_join_types() else 1,
            items=[],
        )
        for avatar in ds_accessor.get_avatar_list():
            dsrc_coll_spec = ds_accessor.get_data_source_coll_spec_strict(source_id=avatar.source_id)
            dsrc_coll = dsrc_coll_factory.get_data_source_collection(
                spec=dsrc_coll_spec,
                dataset_parameter_values=dataset_parameter_values,
                dataset_template_enabled=dataset_template_enabled,
            )
            dsrc = dsrc_coll.get_strict(role=DataSourceRole.origin)
            opt_data["source_avatars"]["items"].append(
                dict(
                    id=avatar.id,
                    schema_update_enabled=dsrc.supports_schema_update,
                )
            )

        opt_data["supports_offset"] = capabilities.supports_offset(role)

        opt_data["supported_functions"] = sfm.get_supported_function_names(dialect=dialect)

        opt_data["fields"] = dict(
            items=[],
        )
        for field in dataset.result_schema:
            if field.has_auto_aggregation or field.lock_aggregation:
                # Aggregation is disabled
                aggregations = [AggregationFunction.none]
            else:
                aggregations = BI_TYPE_AGGREGATIONS.get(field.cast, [])

            opt_data["fields"]["items"].append(
                dict(
                    guid=field.guid,
                    casts=CASTS_BY_TYPE.get(field.initial_data_type, ()),
                    aggregations=aggregations,
                )
            )

        return {"options": opt_data}

    def make_dataset_response_data(
        self, dataset: Dataset, us_entry_buffer: USEntryBuffer, conn_id_mapping: Optional[dict] = None
    ) -> dict:
        service_registry = self.get_service_registry()
        ds_dict = self.dump_dataset_data(
            dataset=dataset,
            us_entry_buffer=us_entry_buffer,
            service_registry=service_registry,
            conn_id_mapping=conn_id_mapping,
        )
        ds_dict.update(
            self.dump_option_data(
                dataset=dataset,
                us_entry_buffer=us_entry_buffer,
                service_registry=service_registry,
            )
        )
        ds_dict["id"] = dataset.uuid
        if dataset.permissions is not None:
            ds_dict["permissions"] = dataset.permissions

        return ds_dict
