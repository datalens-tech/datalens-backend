from __future__ import annotations

import abc
import logging
from typing import (
    TYPE_CHECKING,
    Collection,
)

import attr

from dl_cache_engine.primitives import (
    BIQueryCacheOptions,
    CacheTTLConfig,
    CacheTTLInfo,
    DataKeyPart,
    LocalKeyRepresentation,
)
from dl_core.us_connection_base import ConnectionBase
from dl_model_tools.serialization import hashable_dumps


if TYPE_CHECKING:
    from dl_constants.types import TJSONExt
    from dl_core.data_processing.prepared_components.primitives import PreparedFromInfo
    from dl_core.data_source.base import DataSource


LOGGER = logging.getLogger(__name__)


@attr.s
class CacheOptionsBuilderBase(abc.ABC):
    default_ttl_config: CacheTTLConfig = attr.ib(factory=CacheTTLConfig)

    def get_actual_ttl_config(
        self,
        connection: ConnectionBase,
    ) -> CacheTTLConfig:
        ctc = self.default_ttl_config

        override = connection.cache_ttl_sec_override
        if override is not None:
            ctc = ctc.clone(ttl_sec_direct=override)

        return ctc

    @staticmethod
    def config_to_ttl_info(ttl_config: CacheTTLConfig) -> CacheTTLInfo:
        return CacheTTLInfo(
            ttl_sec=ttl_config.ttl_sec_direct,
            refresh_ttl_on_read=False,
        )

    def get_cache_ttl_info(self, data_source_list: Collection[DataSource]) -> CacheTTLInfo:
        # TODO FIX: Assert that there is no connection divergence or migrate to joint data source info
        assert data_source_list, "Cannot generate cache options for empty source list"
        actual_connection = next(iter({dsrc.connection for dsrc in data_source_list}))

        ttl_config = self.get_actual_ttl_config(
            connection=actual_connection,
        )
        return self.config_to_ttl_info(ttl_config=ttl_config)


@attr.s
class DatasetOptionsBuilder(CacheOptionsBuilderBase):
    @abc.abstractmethod
    def get_cache_enabled(self, joint_dsrc_info: PreparedFromInfo) -> bool:
        raise NotImplementedError

    def get_cache_options(
        self,
        joint_dsrc_info: PreparedFromInfo,
        data_key: LocalKeyRepresentation,
    ) -> BIQueryCacheOptions:
        ttl_info = self.get_cache_ttl_info(data_source_list=joint_dsrc_info.data_source_list)  # type: ignore  # 2024-01-24 # TODO: Argument "data_source_list" to "get_cache_ttl_info" of "CacheOptionsBuilderBase" has incompatible type "tuple[DataSource, ...] | None"; expected "Collection[DataSource]"  [arg-type]
        cache_enabled = self.get_cache_enabled(joint_dsrc_info=joint_dsrc_info)
        return BIQueryCacheOptions(
            cache_enabled=cache_enabled,
            key=data_key if cache_enabled else None,
            ttl_sec=ttl_info.ttl_sec,
            refresh_ttl_on_read=ttl_info.refresh_ttl_on_read,
        )


@attr.s
class CompengOptionsBuilder(DatasetOptionsBuilder):  # TODO: Move to compeng package
    cache_enabled: bool = attr.ib(kw_only=True, default=True)

    def get_cache_enabled(self, joint_dsrc_info: PreparedFromInfo) -> bool:
        return self.cache_enabled


@attr.s
class SelectorCacheOptionsBuilder(DatasetOptionsBuilder):  # TODO: Rename to SourceDbCacheOptionsBuilder
    def get_cache_enabled(self, joint_dsrc_info: PreparedFromInfo) -> bool:
        assert joint_dsrc_info.data_source_list is not None
        cache_enabled = all(dsrc.cache_enabled for dsrc in joint_dsrc_info.data_source_list)
        return cache_enabled


@attr.s
class DashSQLCacheOptionsBuilder(CacheOptionsBuilderBase):
    _is_bleeding_edge_user: bool = attr.ib(default=False)

    def get_cache_enabled(self, conn: ConnectionBase) -> bool:
        return conn.allow_cache

    def get_cache_options(
        self,
        conn: ConnectionBase,
        query_text: str,
        params: TJSONExt,
        db_params: dict[str, str],
        connector_specific_params: TJSONExt,
        data_key: LocalKeyRepresentation = LocalKeyRepresentation(),  # noqa: B008
    ) -> BIQueryCacheOptions:
        cache_enabled = self.get_cache_enabled(conn=conn)
        ttl_config = self.get_actual_ttl_config(connection=conn)
        ttl_info = self.config_to_ttl_info(ttl_config)

        local_key_rep: LocalKeyRepresentation = data_key.multi_extend(*conn.get_cache_key_part().key_parts)
        local_key_rep = local_key_rep.multi_extend(
            DataKeyPart(part_type="query", part_content=query_text),
            DataKeyPart(part_type="params", part_content=hashable_dumps(params)),
            DataKeyPart(part_type="db_params", part_content=tuple(db_params.items())),
            DataKeyPart(part_type="connector_specific_params", part_content=hashable_dumps(connector_specific_params)),
            DataKeyPart(part_type="is_bleeding_edge_user", part_content=self._is_bleeding_edge_user),
        )
        if not cache_enabled:
            local_key_rep = None  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "None", variable has type "LocalKeyRepresentation")  [assignment]

        return BIQueryCacheOptions(
            cache_enabled=cache_enabled,
            key=local_key_rep,
            ttl_sec=ttl_info.ttl_sec,
            refresh_ttl_on_read=ttl_info.refresh_ttl_on_read,
        )
