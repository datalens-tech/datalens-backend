from __future__ import annotations

import attr

from dl_constants.enums import (
    CacheInvalidationMode,
    NotificationLevel,
)
from dl_core.base_models import ObligatoryFilter
from dl_core.fields import BIField


@attr.s()
class CacheInvalidationError:
    """Error from cache invalidation validation"""

    title: str = attr.ib()
    message: str = attr.ib()
    level: NotificationLevel = attr.ib()
    locator: str = attr.ib()


@attr.s()
class CacheInvalidationLastResultError:
    """Error from last cache invalidation execution"""

    code: str = attr.ib()
    message: str | None = attr.ib(default=None)
    details: dict = attr.ib(factory=dict)
    debug: dict = attr.ib(factory=dict)


class CacheInvalidationField(BIField):
    """Field for cache invalidation formula mode."""

    ...


@attr.s()
class CacheInvalidationSource:
    """Cache invalidation source configuration"""

    mode: CacheInvalidationMode = attr.ib(default=CacheInvalidationMode.off)

    # For mode: formula
    filters: list[ObligatoryFilter] = attr.ib(factory=list)
    field: CacheInvalidationField | None = attr.ib(default=None)

    # For mode: sql
    sql: str | None = attr.ib(default=None)

    # Read-only error field
    cache_invalidation_error: CacheInvalidationError | None = attr.ib(default=None)
