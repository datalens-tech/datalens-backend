import enum


class ResourceState(enum.Enum):
    free = enum.auto()
    acquired = enum.auto()
    create_required = enum.auto()
    create_in_progress = enum.auto()
    # Resource can not be used anymore but not still deleted
    # This state is used for batch resource invalidation
    exhausted = enum.auto()
    # Resource was deleted. Resources in this state will be cleaned up periodically
    recycle_required = enum.auto()
    recycle_in_progress = enum.auto()
    deleted = enum.auto()


class ResourceKind(enum.Enum):
    single_docker = enum.auto()


class ResourceType(enum.Enum):
    standalone_postgres = enum.auto()
    standalone_clickhouse = enum.auto()
    standalone_mssql = enum.auto()
    standalone_oracle = enum.auto()


class SessionState(enum.Enum):
    active = enum.auto()
    deleted = enum.auto()
