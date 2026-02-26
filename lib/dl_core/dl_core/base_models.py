from __future__ import annotations

import abc
import logging
from typing import (
    Any,
    Optional,
)

import attr

from dl_constants.enums import (
    AggregationFunction,
    CacheInvalidationMode,
    CalcMode,
    FieldType,
    ManagedBy,
    NotificationLevel,
    RawSQLLevel,
    UserDataType,
    WhereClauseOperation,
)
from dl_utils.utils import DataKey


LOGGER = logging.getLogger(__name__)


# Connection references


@attr.s(frozen=True, slots=True)
class ConnectionRef:
    pass


@attr.s(frozen=True, slots=True)
class DefaultConnectionRef(ConnectionRef):
    conn_id: str = attr.ib(kw_only=True)


@attr.s(frozen=True, slots=True)
class InternalMaterializationConnectionRef(ConnectionRef):
    pass


def connection_ref_from_id(connection_id: Optional[str]) -> ConnectionRef:
    if connection_id is None:
        # TODO REMOVE: some sample source code still relies on mat con ref
        return InternalMaterializationConnectionRef()
    else:
        return DefaultConnectionRef(conn_id=connection_id)


@attr.s()
class DefaultWhereClause:
    operation: WhereClauseOperation = attr.ib()
    values: list = attr.ib(default=[])


@attr.s()
class ObligatoryFilter:
    id: str = attr.ib()
    field_guid: str = attr.ib()
    default_filters: list[DefaultWhereClause] = attr.ib()
    managed_by: ManagedBy = attr.ib(default=ManagedBy.user)
    valid: bool = attr.ib(default=True)


@attr.s()
class CacheInvalidationError:
    """Error from cache invalidation validation"""

    title: str = attr.ib()
    message: str = attr.ib()
    level: NotificationLevel = attr.ib()
    locator: str = attr.ib()


@attr.s()
class CacheInvalidationLastResultError:
    """Error from last cache invalidation execution"""

    code: str = attr.ib()
    message: str | None = attr.ib(default=None)
    details: dict = attr.ib(factory=dict)
    debug: dict = attr.ib(factory=dict)


@attr.s()
class CacheInvalidationField:
    """Field for cache invalidation formula mode.

    Similar to BIField but specific for cache invalidation.
    """

    # The ID of the field
    guid: str = attr.ib()

    # A human-readable name of the field
    title: str = attr.ib(default="INVALIDATION CACHE SERVICE FIELD")

    # Describes how the field was created
    managed_by: ManagedBy = attr.ib(default=ManagedBy.user)

    # Flag indicates whether this field is always shown in UI or not
    hidden: bool = attr.ib(default=False)

    # A plain-text human-readable description of the field
    description: str = attr.ib(default="")

    # Flag indicates whether the field contains any errors
    valid: bool | None = attr.ib(default=None)

    # The earliest identifiable data type of the field
    initial_data_type: UserDataType | None = attr.ib(default=None)

    # Redefines the data type in initial_data_type, is set by the user
    cast: UserDataType | None = attr.ib(default=None)

    # An explicitly set aggregation
    aggregation: AggregationFunction = attr.ib(default=AggregationFunction.none)

    # The data type automatically determined after the aggregation is applied
    data_type: UserDataType | None = attr.ib(default=None)

    # Flag indicates that the field is automatically aggregated
    has_auto_aggregation: bool | None = attr.ib(default=None)

    # Flag that enables or disables the explicit aggregation in UI
    lock_aggregation: bool | None = attr.ib(default=None)

    # Type of field in terms of measure/dimension
    type: FieldType | None = attr.ib(default=None)

    # A string field to store settings from the frontend
    ui_settings: str = attr.ib(default="")

    # Calculation spec fields (flattened)
    calc_mode: CalcMode = attr.ib(default=CalcMode.formula)
    formula: str = attr.ib(default="")
    guid_formula: str = attr.ib(default="")
    source: str = attr.ib(default="")
    avatar_id: str | None = attr.ib(default=None)

    # Virtual field flag
    virtual: bool = attr.ib(default=False)


@attr.s()
class CacheInvalidationSource:
    """Cache invalidation source configuration"""

    mode: CacheInvalidationMode = attr.ib(default=CacheInvalidationMode.off)

    # For mode: formula
    filters: list[ObligatoryFilter] = attr.ib(factory=list)
    field: CacheInvalidationField | None = attr.ib(default=None)

    # For mode: sql
    sql: str | None = attr.ib(default=None)

    # Read-only error fields
    cache_invalidation_error: CacheInvalidationError | None = attr.ib(default=None)
    last_result_timestamp: str | None = attr.ib(default=None)
    last_result_error: CacheInvalidationLastResultError | None = attr.ib(default=None)


@attr.s()
class SourceFilterSpec:
    name: str = attr.ib()
    operation: WhereClauseOperation = attr.ib()
    values: list = attr.ib(default=[])


class EntryLocation(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def to_short_string(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def to_us_req_api_params(self) -> dict[str, Any]:
        raise NotImplementedError()

    @abc.abstractmethod
    def to_us_resp_api_params(self, key_from_us_resp: Optional[str]) -> dict[str, str]:
        raise NotImplementedError()


@attr.s(frozen=True)
class PathEntryLocation(EntryLocation):
    path: str = attr.ib()

    def to_short_string(self) -> str:
        return self.path

    def to_us_req_api_params(self) -> dict[str, Any]:
        return {"key": self.path}

    def to_us_resp_api_params(self, key_from_us_resp: Optional[str]) -> dict[str, str]:
        return {"key": self.path}


@attr.s(frozen=True)
class WorkbookEntryLocation(EntryLocation):
    workbook_id: str = attr.ib()
    entry_name: str = attr.ib()

    def to_short_string(self) -> str:
        return f"WB({self.workbook_id}/{self.entry_name})"

    def to_us_req_api_params(self) -> dict[str, Any]:
        return {
            "workbookId": self.workbook_id,
            "name": self.entry_name,
        }

    def to_us_resp_api_params(self, key_from_us_resp: Optional[str]) -> dict[str, str]:
        restored_key: str
        if key_from_us_resp is None:
            restored_key = f"dummy_workbook_path_repr/{self.entry_name}"
        else:
            restored_key = key_from_us_resp

        return {"key": restored_key, "workbookId": self.workbook_id}


@attr.s(frozen=True)
class CollectionEntryLocation(EntryLocation):
    collection_id: str = attr.ib()
    entry_name: str = attr.ib()

    def to_short_string(self) -> str:
        return f"COLL({self.collection_id}/{self.entry_name})"

    def to_us_req_api_params(self) -> dict[str, Any]:
        return {
            "collectionId": self.collection_id,
            "name": self.entry_name,
        }

    def to_us_resp_api_params(self, key_from_us_resp: Optional[str]) -> dict[str, str]:
        restored_key: str
        if key_from_us_resp is None:
            restored_key = f"dummy_collection_path_repr/{self.entry_name}"
        else:
            restored_key = key_from_us_resp

        return {"key": restored_key, "collectionId": self.collection_id}


@attr.s
class BaseAttrsDataModel:
    """Marker class for attrs-based data"""

    @classmethod
    def get_secret_keys(cls) -> set[DataKey]:
        """
        Returns a set of keys representing secret fields. These fields are serialized with encrytion and deserialized
        with decryption inside the `unversionedData` field.
        """

        return set()

    @classmethod
    def get_unversioned_keys(cls) -> set[DataKey]:
        """
        Returns a set of keys representing unversioned data fields. These fields are directly serialized and
        deserialized inside the `unversionedData` field.
        """

        return set()


@attr.s(kw_only=True)
class ConnectionDataModelBase(BaseAttrsDataModel):
    data_export_forbidden: bool = attr.ib(default=False)


@attr.s
class ConnCacheableDataModelMixin:
    cache_ttl_sec: int | None = attr.ib(kw_only=True, default=None)  # Override for default cache TTL

    cache_invalidation_throttling_interval_sec: int | None = attr.ib(kw_only=True, default=None)


@attr.s
class ConnRawSqlLevelDataModelMixin:
    raw_sql_level: RawSQLLevel = attr.ib(kw_only=True, default=RawSQLLevel.off)
