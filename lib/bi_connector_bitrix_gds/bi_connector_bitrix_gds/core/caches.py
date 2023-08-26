from typing import Optional, Any, AsyncGenerator
import gzip
import logging

import redis.asyncio
import attr
from contextlib import asynccontextmanager
from redis_cache_lock.types import TClientACM

from bi_core.aio.web_app_services.redis import RedisConnParams
from bi_core.data_processing.cache.primitives import LocalKeyRepresentation
from bi_core.serialization import common_dumps, common_loads
from bi_constants.types import TJSONExt
from bi_app_tools.profiling_base import GenericProfiler


LOGGER = logging.getLogger(__name__)


def build_local_key_rep(portal: str, table: str, body: dict) -> LocalKeyRepresentation:
    local_key_rep = LocalKeyRepresentation()
    local_key_rep = local_key_rep.extend(part_type='portal', part_content=portal)
    local_key_rep = local_key_rep.extend(part_type='table', part_content=table)
    local_key_rep = local_key_rep.extend(part_type='body', part_content=frozenset(body.items()))

    return local_key_rep


def make_simple_cli_acm(conn_params: RedisConnParams) -> TClientACM:
    @asynccontextmanager
    async def cli_acm(**_: Any) -> AsyncGenerator[redis.asyncio.Redis, None]:
        rcli: redis.asyncio.Redis = redis.asyncio.Redis(**attr.asdict(conn_params))
        try:
            yield rcli
        finally:
            await rcli.connection_pool.disconnect()

    return cli_acm


def get_redis_cli_acm_from_params(redis_conn_params: Optional[RedisConnParams]) -> Optional[TClientACM]:
    if redis_conn_params is None:
        return None
    return make_simple_cli_acm(conn_params=redis_conn_params)


def bitrix_cache_serializer(data: TJSONExt) -> bytes:
    with GenericProfiler("qcache-serialize"):
        serialized_result_data = common_dumps(data)
    with GenericProfiler("qcache-compress"):
        result_data = gzip.compress(serialized_result_data)
    return result_data


def bitrix_cache_deserializer(data_repr: bytes) -> TJSONExt:
    with GenericProfiler("qcache-decompress"):
        encoded_data = gzip.decompress(data_repr)
    with GenericProfiler("qcache-deserialize"):
        data = common_loads(encoded_data)
    return data
