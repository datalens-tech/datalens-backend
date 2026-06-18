from __future__ import annotations


class CacheError(Exception):
    pass


class CachedEntryPackageVersionMismatchError(CacheError):
    pass


class CacheKeyValidationError(CacheError):
    pass


class CachePreparationFailedError(CacheError):
    pass
