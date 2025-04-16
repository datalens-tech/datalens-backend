from __future__ import annotations

import abc
import logging
from typing import (
    Any,
    Optional,
)

import attr

from dl_constants.enums import (
    ManagedBy,
    RawSQLLevel,
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


@attr.s
class BaseAttrsDataModel:
    """Marker class for attrs-based data"""

    @classmethod
    def get_secret_keys(cls) -> set[DataKey]:
        return set()


@attr.s
class ConnectionDataModelBase(BaseAttrsDataModel):
    pass


@attr.s
class ConnCacheableDataModelMixin:
    cache_ttl_sec: Optional[int] = attr.ib(kw_only=True, default=None)  # Override for default cache TTL


@attr.s
class ConnRawSqlLevelDataModelMixin:
    raw_sql_level: RawSQLLevel = attr.ib(kw_only=True, default=RawSQLLevel.off)
