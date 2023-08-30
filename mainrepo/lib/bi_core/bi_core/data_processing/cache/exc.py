from __future__ import annotations


class CacheException(Exception):
    pass


class CachedEntryPackageVersionMismatch(CacheException):
    pass


class CacheKeyValidationError(CacheException):
    pass


class CachePreparationFailed(CacheException):
    pass
