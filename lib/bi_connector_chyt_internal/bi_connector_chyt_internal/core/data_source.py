from __future__ import annotations

import attr

from bi_constants.enums import ConnectionType, CreateDSFrom

from bi_connector_chyt.core.data_source import (
    BaseCHYTTableDataSource,
    BaseCHYTTableListDataSource,
    BaseCHYTTableRangeDataSource,
    BaseCHYTTableSubselectDataSource,
    CHYTTokenAuthDataSourceMixin,
)


class CHYTInternalTokenAuthDataSourceMixin(CHYTTokenAuthDataSourceMixin):
    conn_type = ConnectionType.ch_over_yt

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            CreateDSFrom.CHYT_TABLE,
            CreateDSFrom.CHYT_TABLE_LIST,
            CreateDSFrom.CHYT_TABLE_RANGE,
            CreateDSFrom.CHYT_SUBSELECT,
        }


class CHYTInternalTableDataSource(CHYTInternalTokenAuthDataSourceMixin, BaseCHYTTableDataSource):
    pass


class CHYTInternalTableListDataSource(CHYTInternalTokenAuthDataSourceMixin, BaseCHYTTableListDataSource):
    pass


class CHYTInternalTableRangeDataSource(CHYTInternalTokenAuthDataSourceMixin, BaseCHYTTableRangeDataSource):
    pass


class CHYTInternalTableSubselectDataSource(CHYTInternalTokenAuthDataSourceMixin, BaseCHYTTableSubselectDataSource):
    pass


class CHYTUserAuthMixin:
    _cache_enabled: bool = attr.ib(default=False)

    conn_type = ConnectionType.ch_over_yt_user_auth

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            CreateDSFrom.CHYT_USER_AUTH_TABLE,
            CreateDSFrom.CHYT_USER_AUTH_TABLE_LIST,
            CreateDSFrom.CHYT_USER_AUTH_TABLE_RANGE,
            CreateDSFrom.CHYT_USER_AUTH_SUBSELECT,
        }


class CHYTUserAuthTableDataSource(CHYTUserAuthMixin, BaseCHYTTableDataSource):
    pass


class CHYTUserAuthTableListDataSource(CHYTUserAuthMixin, BaseCHYTTableListDataSource):
    pass


class CHYTUserAuthTableRangeDataSource(CHYTUserAuthMixin, BaseCHYTTableRangeDataSource):
    pass


class CHYTUserAuthTableSubselectDataSource(CHYTUserAuthMixin, BaseCHYTTableSubselectDataSource):
    pass
