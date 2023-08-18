from __future__ import annotations

import asyncio


class CacheError(Exception):
    """Common base exception"""


class NetworkCallTimeoutError(asyncio.TimeoutError, CacheError):
    """Timed out while waiting on a network request"""


class CacheRedisError(CacheError):
    """Redis communication failure"""


class CacheLockLost(CacheError):
    """Previously owned lock is not owned anymore, abort"""


# class CacheTimeoutError(CacheError):
#     """ Some waiting failed within the time limit """
