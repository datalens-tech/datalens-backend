from __future__ import annotations

import attr

from bi_constants.enums import CreateDSFrom

from bi_connector_chyt.core.data_source import (
    BaseCHYTTableDataSource,
    BaseCHYTTableListDataSource,
    BaseCHYTTableRangeDataSource,
    BaseCHYTTableSubselectDataSource,
    CHYTTokenAuthDataSourceMixin,
)

from bi_connector_chyt_internal.core.constants import (
    CONNECTION_TYPE_CH_OVER_YT,
    CONNECTION_TYPE_CH_OVER_YT_USER_AUTH,
    SOURCE_TYPE_CHYT_TABLE,
    SOURCE_TYPE_CHYT_TABLE_LIST,
    SOURCE_TYPE_CHYT_TABLE_RANGE,
    SOURCE_TYPE_CHYT_SUBSELECT,
    SOURCE_TYPE_CHYT_USER_AUTH_TABLE,
    SOURCE_TYPE_CHYT_USER_AUTH_TABLE_LIST,
    SOURCE_TYPE_CHYT_USER_AUTH_TABLE_RANGE,
    SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT,
)



class CHYTInternalTokenAuthDataSourceMixin(CHYTTokenAuthDataSourceMixin):
    conn_type = CONNECTION_TYPE_CH_OVER_YT

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            SOURCE_TYPE_CHYT_TABLE,
            SOURCE_TYPE_CHYT_TABLE_LIST,
            SOURCE_TYPE_CHYT_TABLE_RANGE,
            SOURCE_TYPE_CHYT_SUBSELECT,
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

    conn_type = CONNECTION_TYPE_CH_OVER_YT_USER_AUTH

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            SOURCE_TYPE_CHYT_USER_AUTH_TABLE,
            SOURCE_TYPE_CHYT_USER_AUTH_TABLE_LIST,
            SOURCE_TYPE_CHYT_USER_AUTH_TABLE_RANGE,
            SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT,
        }


class CHYTUserAuthTableDataSource(CHYTUserAuthMixin, BaseCHYTTableDataSource):
    pass


class CHYTUserAuthTableListDataSource(CHYTUserAuthMixin, BaseCHYTTableListDataSource):
    pass


class CHYTUserAuthTableRangeDataSource(CHYTUserAuthMixin, BaseCHYTTableRangeDataSource):
    pass


class CHYTUserAuthTableSubselectDataSource(CHYTUserAuthMixin, BaseCHYTTableSubselectDataSource):
    pass
