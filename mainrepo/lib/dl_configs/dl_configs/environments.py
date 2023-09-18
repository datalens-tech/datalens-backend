"""
Information about the outside context for all environments.

All the classes (instead of dicts) for static checking.
"""

from __future__ import annotations


def is_setting_applicable(cfg: object, key: str):
    # TODO: remove it
    # it's a crutch-function
    # we will remove this after migrating values to dedicated keys
    return bool(getattr(cfg, key, False))


class LegacyDefaults:
    pass


class BaseInstallationsMap:
    pass


class LegacyEnvAliasesMap:
    pass
