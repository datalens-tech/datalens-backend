from __future__ import annotations

import abc
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Generic,
    NamedTuple,
    Optional,
    Type,
    TypeVar,
    Union,
)

import attr
import sqlalchemy as sa
from sqlalchemy.engine.default import DefaultDialect

from dl_api_commons.reporting.models import NotificationReportingRecord
from dl_api_connector.api_schema.extras import OperationsMode
from dl_cache_engine.primitives import (
    DataKeyPart,
    LocalKeyRepresentation,
)
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_constants.enums import (
    ConnectionType,
    DashSQLQueryType,
    DataSourceRole,
    DataSourceType,
    MigrationStatus,
    NotificationLevel,
    RawSQLLevel,
    UserDataType,
    is_raw_sql_level_dashsql_allowed,
    is_raw_sql_level_subselect_allowed,
    is_raw_sql_level_template_allowed,
)
from dl_core import connection_models
from dl_core.base_models import (
    ConnCacheableDataModelMixin,
    ConnectionDataModelBase,
    ConnectionRef,
    ConnRawSqlLevelDataModelMixin,
    DefaultConnectionRef,
    EntryLocation,
    WorkbookEntryLocation,
)
from dl_core.connection_executors.adapters.common_base import get_dialect_for_conn_type
from dl_core.connection_models import (
    DBIdent,
    SchemaIdent,
)
from dl_core.i18n.localizer import Translatable
from dl_core.us_entry import (
    BaseAttrsDataModel,
    USEntry,
)
from dl_core.utils import (
    parse_comma_separated_hosts,
    secrepr,
)
from dl_i18n.localizer_base import Localizer
from dl_type_transformer.type_transformer import (
    TypeTransformer,
    get_type_transformer,
)
from dl_utils.aio import await_sync
from dl_utils.utils import DataKey


if TYPE_CHECKING:
    from dl_core.connection_executors import SyncConnExecutorBase
    from dl_core.connection_models.common_models import TableIdent
    from dl_core.services_registry import ServicesRegistry
    from dl_core.us_manager.us_manager import USManagerBase
    from dl_core.us_manager.us_manager_sync import SyncUSManager


LOGGER = logging.getLogger(__name__)


class DataSourceTemplate(NamedTuple):
    # for visualization in UI
    title: str
    group: list[str]
    # main properties
    source_type: DataSourceType
    connection_id: str
    # type-specific
    parameters: dict
    tab_title: Optional[str] = None
    form: Optional[list[dict[str, Any]]] = None
    disabled: bool = False

    def get_param_hash(self) -> str:
        from dl_core import data_source  # noqa

        return data_source.get_parameters_hash(
            source_type=self.source_type,
            connection_id=self.connection_id,
            **self.parameters,
        )


def make_subselect_datasource_template(
    connection_id: str,
    source_type: DataSourceType,
    localizer: Localizer,
    disabled: bool,
    template_enabled: bool = False,
    title: str = "SQL",
    field_doc_key: str = "ANY_SUBSELECT/subsql",
) -> DataSourceTemplate:
    return DataSourceTemplate(
        title=title,
        tab_title=localizer.translate(Translatable("source_templates-tab_title-sql")),
        source_type=source_type,
        form=[
            {
                "name": "subsql",
                "input_type": "sql",
                "default": "select 1 as a",
                "required": True,  # Note: `required` means `disallow an empty value` (e.g. an empty string).
                "title": localizer.translate(Translatable("source_templates-label-subquery")),
                "field_doc_key": field_doc_key,
                "template_enabled": template_enabled,
            },
        ],
        disabled=disabled,
        group=[],
        connection_id=connection_id,
        parameters={},
    )


@attr.s(frozen=True)
class RequiredParameterInfo:
    name: str = attr.ib(kw_only=True)
    data_type: UserDataType = attr.ib(kw_only=True)


@attr.s(frozen=True)
class QueryTypeInfo:
    query_type: DashSQLQueryType = attr.ib(kw_only=True)
    query_type_label: str = attr.ib(kw_only=True)  # How the value should be displayed in the UI
    required_parameters: tuple[RequiredParameterInfo, ...] = attr.ib(kw_only=True, default=())
    allow_selector: bool = attr.ib(kw_only=True, default=False)

    @query_type_label.default
    def _default_query_type_label(self) -> str:
        # FIXME: This is a temporary hack until we start using real texts here
        label = self.query_type.name
        label = label.replace("_", " ")
        label = " ".join([word.capitalize() for word in label.split()])
        return label


@attr.s(frozen=True)
class ConnectionOptions:
    allow_dashsql_usage: bool = attr.ib(kw_only=True)
    allow_dataset_usage: bool = attr.ib(kw_only=True)
    allow_typed_query_usage: bool = attr.ib(kw_only=True)
    allow_typed_query_raw_usage: bool = attr.ib(kw_only=True)
    query_types: list[QueryTypeInfo] = attr.ib(kw_only=True)


_CB_TV = TypeVar("_CB_TV", bound="ConnectionBase")


class ConnectionBase(USEntry, metaclass=abc.ABCMeta):
    dir_name: ClassVar[str] = ""  # type: ignore  # TODO: fix
    scope: ClassVar[str] = "connection"  # type: ignore  # TODO: fix

    conn_type: ConnectionType
    source_type: ClassVar[Optional[DataSourceType]] = None
    allowed_source_types: ClassVar[Optional[frozenset[DataSourceType]]] = None
    allow_cache: ClassVar[bool] = False
    allow_export: ClassVar[bool] = False
    is_always_internal_source: ClassVar[bool] = False
    is_always_user_source: ClassVar[bool] = False

    _preview_conn = None

    def __init__(
        self,
        uuid: Optional[str] = None,
        data: Optional[dict] = None,
        entry_key: Optional[EntryLocation] = None,
        type_: Optional[str] = None,
        meta: Optional[dict] = None,
        is_locked: Optional[bool] = None,
        is_favorite: Optional[bool] = None,
        permissions_mode: Optional[str] = None,
        initial_permissions: Optional[str] = None,
        permissions: Optional[dict[str, bool]] = None,
        links: Optional[dict] = None,
        hidden: bool = False,
        data_strict: bool = True,
        migration_status: MigrationStatus = MigrationStatus.non_migrated,
        entry_op_mode: Optional[OperationsMode] = None,
        *,
        us_manager: USManagerBase,
    ):
        super().__init__(
            uuid=uuid,
            data=data,
            entry_key=entry_key,
            type_=type_,
            meta=meta,
            is_locked=is_locked,
            is_favorite=is_favorite,
            permissions_mode=permissions_mode,
            initial_permissions=initial_permissions,
            permissions=permissions,
            links=links,
            hidden=hidden,
            data_strict=data_strict,
            migration_status=migration_status,
            entry_op_mode=entry_op_mode,
            us_manager=us_manager,
        )

        try:
            assert self.type_ is not None
            self.conn_type = ConnectionType(self.type_)
        except ValueError:
            self.conn_type = ConnectionType.unknown

    @property
    def allow_public_usage(self) -> bool:
        return False

    @property
    def pass_db_query_to_user(self) -> bool:
        return self.is_always_user_source or not self.is_always_internal_source

    @attr.s(kw_only=True)
    class DataModel(ConnectionDataModelBase):
        table_name: Optional[str] = attr.ib(default=None)
        sample_table_name: Optional[str] = attr.ib(default=None)
        name: Optional[str] = attr.ib(default=None)
        data_export_forbidden: bool = attr.ib(default=False)
        schema_version: str = attr.ib(default="1")

    @property
    def data_export_forbidden(self) -> bool:
        return self.data.data_export_forbidden if hasattr(self.data, "data_export_forbidden") else False

    @property
    def schema_version(self) -> str:
        return self.data.schema_version

    @classmethod
    def get_provided_source_types(cls) -> frozenset[DataSourceType]:
        if cls.allowed_source_types is not None:
            return cls.allowed_source_types
        if cls.source_type is not None:
            return frozenset((cls.source_type,))
        return frozenset()

    @property
    def conn_ref(self) -> Optional[ConnectionRef]:
        if self.uuid is not None:
            return DefaultConnectionRef(conn_id=self.uuid)

        return None

    @property
    def type_transformer(self) -> TypeTransformer:
        return get_type_transformer(self.conn_type)

    @property
    def is_subselect_allowed(self) -> bool:
        return False

    @property
    def is_dashsql_allowed(self) -> bool:
        return False

    @property
    def is_dataset_allowed(self) -> bool:
        return True

    @property
    def is_typed_query_allowed(self) -> bool:
        return False

    @property
    def is_datasource_template_allowed(self) -> bool:
        return False

    @property
    def is_typed_query_raw_allowed(self) -> bool:
        return False

    def as_dict(self, short=False):  # type: ignore  # TODO: fix
        resp = super().as_dict(short=short)
        if short:
            resp.update(
                {
                    "type": self.conn_type.value,
                }
            )
        else:
            resp.update(
                {
                    "db_type": self.conn_type.value,
                    "meta": self.meta,
                    "created_at": self.created_at,
                    "updated_at": self.updated_at,
                }
            )

        if not isinstance(self.entry_key, WorkbookEntryLocation):
            resp.pop("name", None)
        resp.pop("is_favorite", None)
        resp.pop("table_name", None)
        resp.pop("sample_table_name", None)
        return resp

    @classmethod
    def create_from_dict(
        cls: Type[_CB_TV],
        data_dict: Union[dict, BaseAttrsDataModel],
        ds_key: Union[EntryLocation, str, None] = None,
        type_: Optional[str] = None,
        meta: Optional[dict] = None,
        **kwargs: Any,
    ) -> _CB_TV:
        if meta is None:
            meta = {}
        return super().create_from_dict(
            data_dict,
            ds_key,
            type_,
            meta,
            **kwargs,
        )

    @property
    def table_name(self):  # type: ignore  # TODO: fix
        return self.data.table_name

    @property
    def sample_table_name(self):  # type: ignore  # TODO: fix
        return self.data.sample_table_name

    _dsrc_error = "Only materializable connections can have a data source"  # TODO remove?

    def update_data_source(
        self,
        id: str,
        role: Optional[DataSourceRole] = None,
        raw_schema: Optional[list] = None,
        remove_raw_schema: bool = False,
        **parameters: dict[str, Any],
    ) -> None:
        raise NotImplementedError(self._dsrc_error)

    def get_single_data_source_id(self) -> str:
        raise NotImplementedError(self._dsrc_error)

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        """
        For user-input parametrized sources such as subselects.

        Must *not* query the user database for this.
        """
        # A safe default:
        return []

    def get_data_source_local_templates(self) -> Optional[list[DataSourceTemplate]]:
        """
        A list of available data sources that are stored in the metadata or
        code, i.e. don't require querying the actual database.

        Generally, nonempty local_templates should mean that full templates
        don't need to be requested.
        """
        return None

    def get_data_source_templates(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[DataSourceTemplate]:
        raise NotImplementedError

    async def validate_new_data(
        self,
        services_registry: ServicesRegistry,
        changes: Optional[dict] = None,
        original_version: Optional[ConnectionBase] = None,
    ) -> None:
        """Override point for subclasses"""

    def validate_new_data_sync(
        self,
        services_registry: ServicesRegistry,
        changes: Optional[dict] = None,
        original_version: Optional[ConnectionBase] = None,
    ) -> None:
        """Convenience wrapper"""
        return await_sync(
            self.validate_new_data(
                services_registry=services_registry,
                changes=changes,
                original_version=original_version,
            )
        )

    def check_for_notifications(self) -> list[Optional[NotificationReportingRecord]]:
        return []

    def get_import_warnings_list(self, localizer: Localizer) -> list[dict]:
        CODE_PREFIX = "NOTIF.WB_IMPORT.CONN."

        return [
            dict(
                message=localizer.translate(Translatable("notif_check-creds")),
                level=NotificationLevel.info,
                code=CODE_PREFIX + "CHECK_CREDENTIALS",
            )
        ]

    def get_export_warnings_list(self, localizer: Localizer) -> list[dict]:
        CODE_PREFIX = "NOTIF.WB_EXPORT.CONN."
        return [
            dict(
                message=localizer.translate(Translatable("notif_check-creds")),
                level=NotificationLevel.info,
                code=CODE_PREFIX + "CHECK_CREDENTIALS",
            )
        ]

    def get_cache_key_part(self) -> LocalKeyRepresentation:
        local_key_rep = LocalKeyRepresentation()
        local_key_rep = local_key_rep.multi_extend(
            DataKeyPart(
                part_type="connection_id",
                part_content=self.uuid,
            ),
            DataKeyPart(
                part_type="connection_revision_id",
                part_content=self.revision_id,
            ),
        )
        return local_key_rep

    def get_supported_query_type_infos(self) -> frozenset[QueryTypeInfo]:
        return frozenset({QueryTypeInfo(query_type=DashSQLQueryType.generic_query)})

    def get_options(self) -> ConnectionOptions:
        query_type_info_list = sorted(self.get_supported_query_type_infos(), key=lambda qti: qti.query_type.name)
        return ConnectionOptions(
            allow_dashsql_usage=self.is_dashsql_allowed,
            allow_dataset_usage=self.is_dataset_allowed,
            allow_typed_query_usage=self.is_typed_query_allowed,
            allow_typed_query_raw_usage=self.is_typed_query_raw_allowed,
            query_types=query_type_info_list,
        )

    @abc.abstractmethod
    def get_conn_dto(self) -> connection_models.ConnDTO:
        pass

    def get_conn_options(self) -> connection_models.ConnectOptions:
        return connection_models.ConnectOptions(
            pass_db_messages_to_user=self.is_always_user_source and not self.is_always_internal_source,
            pass_db_query_to_user=self.is_always_user_source and not self.is_always_internal_source,
        )

    def get_dialect(self) -> DefaultDialect:
        return get_dialect_for_conn_type(self.conn_type)

    def quote(self, value) -> sa.sql.quoted_name:  # type: ignore  # TODO: fix  # subclass of str
        return self.get_dialect().identifier_preparer.quote(value)

    @property
    @abc.abstractmethod
    def cache_ttl_sec_override(self) -> Optional[int]:
        pass

    @property
    def use_locked_cache(self) -> bool:
        """Transition configurable override; should eventually become all-`True`"""
        import os

        disabled_types = (os.environ.get("DL_NONLOCKED_CACHE_CONN_TYPES") or "").split(",")
        result = self.conn_type.name not in disabled_types
        LOGGER.debug(f"use_locked_cache = {self.conn_type.name!r} not in {disabled_types!r} = {result!r}")
        return result

    def test(self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase]) -> None:
        conn_executor = conn_executor_factory(self)
        conn_executor.test()

    def get_schema_names(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
        db_name: Optional[str] = None,
    ) -> list[str]:
        conn_executor = conn_executor_factory(self)
        return conn_executor.get_schema_names(DBIdent(db_name))

    def get_tables(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
        db_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> list[TableIdent]:
        conn_executor = conn_executor_factory(self)
        return conn_executor.get_tables(SchemaIdent(db_name=db_name, schema_name=schema_name))


class RawSqlLevelConnectionMixin(ConnectionBase):
    allow_dashsql: ClassVar[bool] = False

    @property
    def _raw_sql_level(self) -> RawSQLLevel:
        assert isinstance(self.data, ConnRawSqlLevelDataModelMixin)
        return self.data.raw_sql_level

    @property
    def is_subselect_allowed(self) -> bool:
        return is_raw_sql_level_subselect_allowed(self._raw_sql_level)

    @property
    def _is_raw_sql_level_template_allowed(self) -> bool:
        return is_raw_sql_level_template_allowed(self._raw_sql_level)

    @property
    def is_dashsql_allowed(self) -> bool:
        if not self.allow_dashsql:
            return False

        return is_raw_sql_level_dashsql_allowed(self._raw_sql_level)


class ConnectionSQL(RawSqlLevelConnectionMixin, ConnectionBase):
    has_schema: ClassVar[bool] = False
    default_schema_name: ClassVar[Optional[str]] = None

    @attr.s(kw_only=True)
    class DataModel(
        ConnCacheableDataModelMixin,
        ConnRawSqlLevelDataModelMixin,
        ConnectionBase.DataModel,
    ):
        host: Optional[str] = attr.ib(default=None)
        port: Optional[int] = attr.ib(default=None)
        db_name: Optional[str] = attr.ib(default=None)
        username: Optional[str] = attr.ib(default=None)
        password: Optional[str] = attr.ib(repr=secrepr, default=None)

        @classmethod
        def get_secret_keys(cls) -> set[DataKey]:
            return {DataKey(parts=("password",))}

    @property
    def cache_ttl_sec_override(self) -> Optional[int]:
        return self.data.cache_ttl_sec

    @property
    def db_name(self) -> Optional[str]:
        return self.data.db_name

    @property
    def is_datasource_template_allowed(self) -> bool:
        return self._is_raw_sql_level_template_allowed

    def get_parameter_combinations(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[dict]:
        if not self.db_name:
            return []

        # has db_name, so just list tables
        if not self.has_schema:
            return [
                dict(table_name=tid.table_name) for tid in self.get_tables(conn_executor_factory=conn_executor_factory)
            ]

        assert self.has_schema
        schemas = self.get_schema_names(conn_executor_factory=conn_executor_factory)
        return [
            dict(schema_name=schema_name, table_name=tid.table_name)
            for schema_name in schemas
            for tid in self.get_tables(conn_executor_factory=conn_executor_factory, schema_name=schema_name)
        ]

    def get_data_source_template_title(self, parameters: dict) -> str:
        return parameters["table_name"]

    def get_data_source_template_group(self, parameters: dict) -> list[str]:
        return [val for val in (parameters.get("db_name"), parameters.get("schema_name")) if val is not None]

    def get_data_source_templates(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[DataSourceTemplate]:
        """For listed sources such as db tables"""
        return [
            DataSourceTemplate(
                title=self.get_data_source_template_title(parameters),
                group=self.get_data_source_template_group(parameters),
                source_type=self.source_type,  # type: ignore  # TODO: fix
                connection_id=self.uuid,  # type: ignore  # TODO: fix
                parameters=parameters,
            )
            for parameters in self.get_parameter_combinations(conn_executor_factory=conn_executor_factory)
        ]

    def validate(self) -> None:
        pass


class ClassicConnectionSQL(ConnectionSQL):
    @property
    def password(self) -> Optional[str]:
        return self.data.password

    def parse_multihosts(self) -> tuple[str, ...]:
        return parse_comma_separated_hosts(self.data.host)


CONNECTOR_SETTINGS_TV = TypeVar("CONNECTOR_SETTINGS_TV", bound=ConnectorSettingsBase)


def _get_connector_settings(
    usm: USManagerBase,
    conn_type: ConnectionType,
    settings_type: Type[CONNECTOR_SETTINGS_TV],
) -> CONNECTOR_SETTINGS_TV:
    sr = usm.get_services_registry()
    connector_settings = sr.get_connectors_settings(conn_type)

    if connector_settings is None:
        try:
            connector_settings = settings_type()
        except Exception as exc:
            raise ValueError(f"Failed to instantiate settings of type {settings_type}") from exc

    assert isinstance(connector_settings, settings_type)
    return connector_settings


class ConnectionSettingsMixin(ConnectionBase, Generic[CONNECTOR_SETTINGS_TV], metaclass=abc.ABCMeta):
    """Connector type specific data is loaded from dl_configs.connectors_settings"""

    settings_type: Type[CONNECTOR_SETTINGS_TV]

    @property
    def _connector_settings(self) -> CONNECTOR_SETTINGS_TV:
        return _get_connector_settings(self.us_manager, self.conn_type, self.settings_type)

    @classmethod
    def _get_connector_settings(cls, usm: SyncUSManager) -> CONNECTOR_SETTINGS_TV:
        return _get_connector_settings(usm, cls.conn_type, cls.settings_type)


class HiddenDatabaseNameMixin(ConnectionSQL):
    """Mixin that hides db name in UI"""

    def get_parameter_combinations(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[dict]:
        return [
            dict(combination, db_name=None)  # don't expose db name
            for combination in super().get_parameter_combinations(conn_executor_factory=conn_executor_factory)
            if combination["db_name"] == self.db_name
        ]


class UnknownConnection(ConnectionBase):
    """
    Special connection class for entries with unknown type and connection scope
    """

    conn_type = ConnectionType.unknown

    @attr.s(kw_only=True)
    class DataModel(ConnectionBase.DataModel):
        pass

    def get_conn_dto(self) -> connection_models.ConnDTO:
        raise NotImplementedError

    @property
    def cache_ttl_sec_override(self) -> Optional[int]:
        return 0
