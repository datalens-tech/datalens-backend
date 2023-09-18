from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Collection,
    Optional,
)

import attr
from sqlalchemy.exc import DatabaseError

from dl_constants.enums import DataSourceRole
from dl_core.data_processing.cache.exc import CachePreparationFailed
from dl_core.data_processing.cache.primitives import (
    BIQueryCacheOptions,
    CacheTTLConfig,
    CacheTTLInfo,
    DataKeyPart,
    LocalKeyRepresentation,
)
from dl_core.serialization import hashable_dumps
from dl_core.us_connection_base import (
    ConnectionBase,
    ExecutorBasedMixin,
)

if TYPE_CHECKING:
    from sqlalchemy.engine.default import DefaultDialect
    from sqlalchemy.sql import Select

    from dl_constants.enums import BIType
    from dl_constants.types import TJSONExt
    from dl_core.data_processing.prepared_components.primitives import PreparedMultiFromInfo
    from dl_core.data_processing.stream_base import DataStreamBase
    from dl_core.data_source.base import DataSource
    from dl_core.us_dataset import Dataset
    from dl_core.us_manager.local_cache import USEntryBuffer


LOGGER = logging.getLogger(__name__)


@attr.s
class CacheOptionsBuilderBase:
    default_ttl_config: CacheTTLConfig = attr.ib(factory=CacheTTLConfig)
    _is_bleeding_edge_user: bool = attr.ib(default=False)

    def get_actual_ttl_config(
        self,
        connection: ConnectionBase,
        dataset: Optional[Dataset] = None,
    ) -> CacheTTLConfig:
        ctc = self.default_ttl_config

        if isinstance(connection, ExecutorBasedMixin):
            override = connection.cache_ttl_sec_override
            if override is not None:
                ctc = ctc.clone(ttl_sec_direct=override)

        return ctc

    @staticmethod
    def get_query_str_for_cache(query: Select, dialect: DefaultDialect) -> str:
        try:
            compiled_query = query.compile(dialect=dialect)
        except DatabaseError as err:
            raise CachePreparationFailed from err

        if isinstance(compiled_query.params, dict):
            ordered_params = sorted(
                compiled_query.params.items(),
                key=lambda item: item[0],
            )
        else:
            ordered_params = compiled_query.params

        return ";".join(
            (
                str(compiled_query),
                str(ordered_params),
            )
        )

    @staticmethod
    def config_to_ttl_info(
        ttl_config: CacheTTLConfig,
        is_materialized: bool,
        data_source_list: Optional[Collection[DataSource]] = None,
    ) -> CacheTTLInfo:
        ttl_info = CacheTTLInfo(
            ttl_sec=ttl_config.ttl_sec_direct,
            refresh_ttl_on_read=False,
        )

        if not is_materialized:
            return ttl_info
        if data_source_list is None:
            return ttl_info

        data_dump_id_list = [dsrc.data_dump_id for dsrc in data_source_list]
        if not all(data_dump_id_list):
            return ttl_info

        # Materialized
        return ttl_info.clone(
            ttl_sec=ttl_config.ttl_sec_materialized,
            refresh_ttl_on_read=True,
        )

    def get_cache_ttl_info(
        self,
        is_materialized: bool,
        data_source_list: Collection[DataSource],
        # For future use
        dataset: Optional[Dataset] = None,  # noqa
    ) -> CacheTTLInfo:
        # TODO FIX: Assert that there is no connection divergence or migrate to joint data source info
        assert data_source_list, "Cannot generate cache options for empty source list"
        actual_connection = next(iter({dsrc.connection for dsrc in data_source_list}))

        ttl_config = self.get_actual_ttl_config(
            connection=actual_connection,
            dataset=dataset,
        )
        return self.config_to_ttl_info(
            ttl_config=ttl_config,
            is_materialized=is_materialized,
            data_source_list=data_source_list,
        )


@attr.s
class CacheOptionsBuilderDataProcessor(CacheOptionsBuilderBase):
    def get_cache_options_for_stream(
        self,
        stream: DataStreamBase,
        dataset: Optional[Dataset] = None,
    ) -> BIQueryCacheOptions:
        ttl_info = self.get_cache_ttl_info(
            is_materialized=stream.meta.is_materialized,
            data_source_list=stream.meta.data_source_list,
            dataset=dataset,
        )
        key = stream.data_key
        cache_enabled = key is not None
        return BIQueryCacheOptions(
            cache_enabled=cache_enabled,
            key=key,
            ttl_sec=ttl_info.ttl_sec,
            refresh_ttl_on_read=ttl_info.refresh_ttl_on_read,
        )


@attr.s
class SelectorCacheOptionsBuilder(CacheOptionsBuilderBase):
    _is_bleeding_edge_user: bool = attr.ib(default=False)
    _us_entry_buffer: USEntryBuffer = attr.ib(kw_only=True)

    def get_cache_enabled(self, joint_dsrc_info: PreparedMultiFromInfo) -> bool:
        assert joint_dsrc_info.data_source_list is not None
        cache_enabled = all(dsrc.cache_enabled for dsrc in joint_dsrc_info.data_source_list)
        return cache_enabled

    def get_cache_options(
        self,
        role: DataSourceRole,
        joint_dsrc_info: PreparedMultiFromInfo,
        query: Select,
        user_types: list[BIType],
        dataset: Dataset,
    ) -> BIQueryCacheOptions:
        """Returns cache key, TTL for new entries, refresh TTL flag"""

        merged_data_dump_id = "N/A"
        assert joint_dsrc_info.data_source_list is not None
        if role != DataSourceRole.origin:
            data_dump_id_list = [dsrc.data_dump_id for dsrc in joint_dsrc_info.data_source_list]
            if all(data_dump_id_list):
                merged_data_dump_id = "+".join(data_dump_id_list)  # type: ignore

        compiled_query = self.get_query_str_for_cache(
            query=query,
            dialect=joint_dsrc_info.query_compiler.dialect,
        )
        local_key_rep: Optional[LocalKeyRepresentation] = self.make_data_select_cache_key(
            joint_dsrc_info=joint_dsrc_info,
            compiled_query=compiled_query,
            user_types=user_types,
            data_dump_id=merged_data_dump_id,
            is_bleeding_edge_user=self._is_bleeding_edge_user,
        )
        ttl_info = self.get_cache_ttl_info(
            is_materialized=role != DataSourceRole.origin,
            data_source_list=joint_dsrc_info.data_source_list,
            dataset=dataset,
        )
        cache_enabled = self.get_cache_enabled(joint_dsrc_info=joint_dsrc_info)
        if not cache_enabled:
            local_key_rep = None

        return BIQueryCacheOptions(
            cache_enabled=cache_enabled,
            key=local_key_rep,
            ttl_sec=ttl_info.ttl_sec,
            refresh_ttl_on_read=ttl_info.refresh_ttl_on_read,
        )

    def make_data_select_cache_key(
        self,
        joint_dsrc_info: PreparedMultiFromInfo,
        compiled_query: str,
        user_types: list[BIType],
        data_dump_id: str,
        is_bleeding_edge_user: bool,
    ) -> LocalKeyRepresentation:
        assert joint_dsrc_info.target_connection_ref is not None
        target_connection = self._us_entry_buffer.get_entry(joint_dsrc_info.target_connection_ref)
        assert isinstance(target_connection, ConnectionBase)
        connection_id = target_connection.uuid
        assert connection_id is not None

        local_key_rep = target_connection.get_cache_key_part()
        if joint_dsrc_info.db_name is not None:
            # FIXME: Replace with key parts for every participating dsrc
            # For now db_name will be duplicated in some source types
            # (one from connection, one from source)
            local_key_rep = local_key_rep.extend(part_type="db_name", part_content=joint_dsrc_info.db_name)
        local_key_rep = local_key_rep.extend(part_type="query", part_content=str(compiled_query))
        local_key_rep = local_key_rep.extend(part_type="user_types", part_content=tuple(user_types or ()))
        local_key_rep = local_key_rep.extend(part_type="data_dump_id", part_content=data_dump_id or "N/A")
        local_key_rep = local_key_rep.extend(
            part_type="is_bleeding_edge_user",
            part_content=is_bleeding_edge_user,
        )

        return local_key_rep


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
    ) -> BIQueryCacheOptions:
        cache_enabled = self.get_cache_enabled(conn=conn)
        ttl_config = self.get_actual_ttl_config(connection=conn, dataset=None)
        ttl_info = self.config_to_ttl_info(ttl_config, is_materialized=False, data_source_list=None)

        local_key_rep: Optional[LocalKeyRepresentation] = conn.get_cache_key_part()
        local_key_rep = local_key_rep.multi_extend(
            DataKeyPart(part_type="query", part_content=query_text),
            DataKeyPart(part_type="params", part_content=hashable_dumps(params)),
            DataKeyPart(part_type="db_params", part_content=tuple(db_params.items())),
            DataKeyPart(part_type="connector_specific_params", part_content=hashable_dumps(connector_specific_params)),
            DataKeyPart(part_type="is_bleeding_edge_user", part_content=self._is_bleeding_edge_user),
        )
        if not cache_enabled:
            local_key_rep = None

        return BIQueryCacheOptions(
            cache_enabled=cache_enabled,
            key=local_key_rep,
            ttl_sec=ttl_info.ttl_sec,
            refresh_ttl_on_read=ttl_info.refresh_ttl_on_read,
        )
