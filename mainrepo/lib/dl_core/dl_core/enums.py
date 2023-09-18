from enum import (
    Enum,
    unique,
)


@unique
class QueryExecutorMode(Enum):
    text = "text"
    sqla_dump = "sqla_dump"


@unique
class TableDefinitionType(Enum):
    table_ident = "table_ident"
    sa_text = "sa_text"


@unique
class RQEEventType(Enum):
    raw_cursor_info = "raw_cursor_info"
    raw_chunk = "raw_chunk"
    error_dump = "error_dump"
    finished = "finished"


@unique
class USApiType(Enum):
    v1 = "v1"
    private = "private"
    public = "public"
    embeds = "embeds"


@unique
class RoleReason(Enum):
    # Role was selected
    selected = "selected"
    # Role was nort requested
    not_needed = "not_needed"
    # Role is not supported for source
    not_supported = "not_supported"
    # Role is not yet configured
    not_configured = "not_configured"
    # Missing source for default role
    missing_source = "missing_source"
    # Schema mismatch between default role and origin
    schema_mismatch = "schema_mismatch"
    # This role is not allowed for feature-managed sources
    forbidden_for_features = "forbidden_for_features"
