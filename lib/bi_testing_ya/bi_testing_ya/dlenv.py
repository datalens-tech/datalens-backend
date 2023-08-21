from __future__ import annotations

import enum


class DLEnv(enum.Enum):
    cloud_prod = enum.auto()
    cloud_preprod = enum.auto()
    dynamic = enum.auto()
    internal_prod = enum.auto()
    internal_preprod = enum.auto()
    dc_testing = enum.auto()
    dc_prod = enum.auto()
