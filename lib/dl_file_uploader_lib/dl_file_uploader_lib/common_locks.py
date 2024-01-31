from __future__ import annotations

import logging

import redis
from redis.asyncio.lock import Lock as RedisLock


LOGGER = logging.getLogger(__name__)


def get_update_connection_source_lock(src_id: str) -> tuple[str, str]:
    """Returns redis lock key and token"""

    return f"UpdateConnectionSource/{src_id}", src_id


async def release_source_update_locks(redis_cli: redis.asyncio.Redis, *source_ids: str) -> None:
    for source_id in source_ids:
        source_update_lock_key, source_update_lock_token = get_update_connection_source_lock(src_id=source_id)
        source_lock = RedisLock(redis=redis_cli, name=source_update_lock_key)
        source_lock.local.token = source_update_lock_token
        try:
            await source_lock.release()
        except redis.exceptions.LockError:
            LOGGER.info(f"Lock {source_update_lock_key} is already released")
        else:
            LOGGER.info(f"Released lock {source_update_lock_key}")
