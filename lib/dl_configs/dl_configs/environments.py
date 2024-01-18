"""
Information about the outside context for all environments.

All the classes (instead of dicts) for static checking.
"""
from __future__ import annotations

from typing import Any


def is_setting_applicable(cfg: object, key: str):
    # TODO: remove it
    # it's a crutch-function
    # we will remove this after migrating values to dedicated keys
    return bool(getattr(cfg, key, False))


class LegacyDefaults:
    def __getattr__(self, item: Any) -> Any:
        # only for type checker
        return getattr(self, item)


class BaseInstallationsMap:
    pass


class LegacyEnvAliasesMap:
    pass
