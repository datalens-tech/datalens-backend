import enum


class EnvType(enum.Enum):
    development = enum.auto()
    int_production = enum.auto()
    int_testing = enum.auto()
    yc_testing = enum.auto()
    yc_production = enum.auto()
    dc_any = enum.auto()
    israel = enum.auto()
    nemax = enum.auto()


class FeatureEnablingMode(enum.Enum):
    enabled = 'enabled'
    bleeding_edge = 'bleeding_edge'
    disabled = 'disabled'


class AppType(enum.Enum):
    CLOUD = enum.auto()
    CLOUD_PUBLIC = enum.auto()
    CLOUD_EMBED = enum.auto()
    INTRANET = enum.auto()
    TESTS = enum.auto()
    DATA_CLOUD = enum.auto()
    NEBIUS = enum.auto()
    DATA_CLOUD_EMBED = enum.auto()


class RequiredService(enum.Enum):
    POSTGRES = enum.auto()
    RQE_EXT_ASYNC = enum.auto()
    RQE_EXT_SYNC = enum.auto()
    RQE_INT_SYNC = enum.auto()


RQE_SERVICES: set[RequiredService] = {
    RequiredService.RQE_INT_SYNC,
    RequiredService.RQE_EXT_SYNC,
    RequiredService.RQE_EXT_ASYNC
}


class RedisMode(enum.Enum):
    sentinel = enum.auto()
    single_host = enum.auto()
