from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Collection,
    Optional,
)

import attr
from sqlalchemy.exc import DatabaseError

from dl_cache_engine.exc import CachePreparationFailed
from dl_cache_engine.primitives import (
    BIQueryCacheOptions,
    CacheTTLConfig,
    CacheTTLInfo,
    DataKeyPart,
    LocalKeyRepresentation,
)
from dl_core.query.bi_query import QueryAndResultInfo
from dl_core.us_connection_base import ConnectionBase
from dl_model_tools.serialization import hashable_dumps


if TYPE_CHECKING:
    from sqlalchemy.engine.default import DefaultDialect
    from sqlalchemy.sql import Select

    from dl_constants.enums import UserDataType
    from dl_constants.types import TJSONExt
    from dl_core.data_processing.prepared_components.primitives import PreparedFromInfo
    from dl_core.data_source.base import DataSource
    from dl_core.us_manager.local_cache import USEntryBuffer


LOGGER = logging.getLogger(__name__)


@attr.s
class CacheOptionsBuilderBase:
    default_ttl_config: CacheTTLConfig = attr.ib(factory=CacheTTLConfig)
    _is_bleeding_edge_user: bool = attr.ib(default=False)

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

    def get_data_key(
        self,
        *,
        query_res_info: QueryAndResultInfo,
        from_info: Optional[PreparedFromInfo] = None,
        base_key: LocalKeyRepresentation = LocalKeyRepresentation(),  # noqa: B008
    ) -> Optional[LocalKeyRepresentation]:
        return base_key


@attr.s
class DatasetOptionsBuilder(CacheOptionsBuilderBase):
    cache_enabled: bool = attr.ib(kw_only=True, default=True)

    def get_cache_options(
        self,
        joint_dsrc_info: PreparedFromInfo,
        data_key: LocalKeyRepresentation,
    ) -> BIQueryCacheOptions:
        raise NotImplementedError


@attr.s
class CompengOptionsBuilder(DatasetOptionsBuilder):  # TODO: Move to compeng package
    cache_enabled: bool = attr.ib(kw_only=True, default=True)

    def get_cache_options(
        self,
        joint_dsrc_info: PreparedFromInfo,
        data_key: LocalKeyRepresentation,
    ) -> BIQueryCacheOptions:
        ttl_info = self.get_cache_ttl_info(data_source_list=joint_dsrc_info.data_source_list)  # type: ignore  # 2024-01-24 # TODO: Argument "data_source_list" to "get_cache_ttl_info" of "CacheOptionsBuilderBase" has incompatible type "tuple[DataSource, ...] | None"; expected "Collection[DataSource]"  [arg-type]
        return BIQueryCacheOptions(
            cache_enabled=self.cache_enabled,
            key=data_key,
            ttl_sec=ttl_info.ttl_sec,
            refresh_ttl_on_read=ttl_info.refresh_ttl_on_read,
        )

    def get_data_key(
        self,
        *,
        query_res_info: QueryAndResultInfo,
        from_info: Optional[PreparedFromInfo] = None,
        base_key: LocalKeyRepresentation = LocalKeyRepresentation(),  # noqa: B008
    ) -> Optional[LocalKeyRepresentation]:
        # TODO: Remove after switching to new cache keys
        compiled_query = self.get_query_str_for_cache(
            query=query_res_info.query,
            dialect=from_info.query_compiler.dialect,  # type: ignore  # 2024-01-24 # TODO: Item "None" of "PreparedFromInfo | None" has no attribute "query_compiler"  [union-attr]
        )
        return base_key.extend(part_type="query", part_content=compiled_query)


@attr.s
class SelectorCacheOptionsBuilder(DatasetOptionsBuilder):
    _is_bleeding_edge_user: bool = attr.ib(default=False)
    _us_entry_buffer: USEntryBuffer = attr.ib(kw_only=True)

    def get_cache_enabled(self, joint_dsrc_info: PreparedFromInfo) -> bool:
        assert joint_dsrc_info.data_source_list is not None
        cache_enabled = all(dsrc.cache_enabled for dsrc in joint_dsrc_info.data_source_list)
        return cache_enabled

    def get_cache_options(
        self,
        joint_dsrc_info: PreparedFromInfo,
        data_key: LocalKeyRepresentation,
    ) -> BIQueryCacheOptions:
        """Returns cache key, TTL for new entries, refresh TTL flag"""

        ttl_info = self.get_cache_ttl_info(data_source_list=joint_dsrc_info.data_source_list)  # type: ignore  # 2024-01-24 # TODO: Argument "data_source_list" to "get_cache_ttl_info" of "CacheOptionsBuilderBase" has incompatible type "tuple[DataSource, ...] | None"; expected "Collection[DataSource]"  [arg-type]
        cache_enabled = self.get_cache_enabled(joint_dsrc_info=joint_dsrc_info)
        return BIQueryCacheOptions(
            cache_enabled=cache_enabled,
            key=data_key if cache_enabled else None,
            ttl_sec=ttl_info.ttl_sec,
            refresh_ttl_on_read=ttl_info.refresh_ttl_on_read,
        )

    def make_data_select_cache_key(
        self,
        from_info: PreparedFromInfo,
        compiled_query: str,
        user_types: list[UserDataType],
        is_bleeding_edge_user: bool,
        base_key: LocalKeyRepresentation = LocalKeyRepresentation(),  # noqa: B008
    ) -> LocalKeyRepresentation:
        # TODO: Remove after switching to new cache keys,
        #  but put the db_name + target_connection.get_cache_key_part() parts somewhere
        assert from_info.target_connection_ref is not None
        target_connection = self._us_entry_buffer.get_entry(from_info.target_connection_ref)
        assert isinstance(target_connection, ConnectionBase)
        connection_id = target_connection.uuid
        assert connection_id is not None

        local_key_rep = base_key
        local_key_rep = local_key_rep.extend(part_type="query", part_content=str(compiled_query))
        local_key_rep = local_key_rep.extend(part_type="user_types", part_content=tuple(user_types or ()))
        local_key_rep = local_key_rep.extend(
            part_type="is_bleeding_edge_user",
            part_content=is_bleeding_edge_user,
        )

        return local_key_rep

    def get_data_key(
        self,
        *,
        query_res_info: QueryAndResultInfo,
        from_info: Optional[PreparedFromInfo] = None,
        base_key: LocalKeyRepresentation = LocalKeyRepresentation(),  # noqa: B008
    ) -> Optional[LocalKeyRepresentation]:
        # TODO: Remove after switching to new cache keys
        compiled_query = self.get_query_str_for_cache(
            query=query_res_info.query,
            dialect=from_info.query_compiler.dialect,  # type: ignore  # 2024-01-24 # TODO: Item "None" of "PreparedFromInfo | None" has no attribute "query_compiler"  [union-attr]
        )
        data_key: Optional[LocalKeyRepresentation] = self.make_data_select_cache_key(
            base_key=base_key,
            from_info=from_info,  # type: ignore  # 2024-01-24 # TODO: Argument "from_info" to "make_data_select_cache_key" of "SelectorCacheOptionsBuilder" has incompatible type "PreparedFromInfo | None"; expected "PreparedFromInfo"  [arg-type]
            compiled_query=compiled_query,
            user_types=query_res_info.user_types,
            is_bleeding_edge_user=self._is_bleeding_edge_user,
        )
        return data_key


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
