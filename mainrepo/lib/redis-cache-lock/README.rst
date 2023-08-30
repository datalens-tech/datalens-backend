================
redis_cache_lock
================

Cache with synchronization over redis.

Similar to `aioredis lock
<https://github.com/aio-libs/aioredis-py/blob/master/aioredis/lock.py>`_,
but optimized for synchronizing cache, to reduce work done.

Highly similar to `redis-memolock
<https://github.com/kristoff-it/redis-memolock>`_.


Usage
-----

See `example.py <doc/example.py>`_.
