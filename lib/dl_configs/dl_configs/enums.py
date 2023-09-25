import enum


class FeatureEnablingMode(enum.Enum):
    enabled = "enabled"
    bleeding_edge = "bleeding_edge"
    disabled = "disabled"


class RequiredService(enum.Enum):
    POSTGRES = enum.auto()
    RQE_EXT_ASYNC = enum.auto()
    RQE_EXT_SYNC = enum.auto()
    RQE_INT_SYNC = enum.auto()


RQE_SERVICES: set[RequiredService] = {
    RequiredService.RQE_INT_SYNC,
    RequiredService.RQE_EXT_SYNC,
    RequiredService.RQE_EXT_ASYNC,
}


class RedisMode(enum.Enum):
    sentinel = enum.auto()
    single_host = enum.auto()
