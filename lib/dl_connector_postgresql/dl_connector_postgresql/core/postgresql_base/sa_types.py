from __future__ import annotations

from sqlalchemy.dialects import postgresql as pg_types

from dl_core.db.sa_types_base import (
    make_native_type,
    simple_instantiator,
    timezone_instantiator,
    typed_instantiator,
)

from dl_connector_postgresql.core.postgresql_base.type_transformer import (
    PG_TYPES_FLOAT,
    PG_TYPES_INT,
    PG_TYPES_STRING,
)


SQLALCHEMY_POSTGRES_BASE_TYPES = (
    *PG_TYPES_INT,
    *PG_TYPES_FLOAT,
    *PG_TYPES_STRING,
    pg_types.BOOLEAN,
    pg_types.DATE,
    pg_types.UUID,
)
SQLALCHEMY_POSTGRES_TIMEZONE_TYPES = (pg_types.TIMESTAMP,)
SQLALCHEMY_POSTGRES_ARRAY_INNER_TYPES = (
    *PG_TYPES_INT,
    *PG_TYPES_FLOAT,
    *PG_TYPES_STRING,
)
SQLALCHEMY_POSTGRES_TYPES = {
    **{
        make_native_type(typecls): simple_instantiator(typecls)
        for typecls in SQLALCHEMY_POSTGRES_BASE_TYPES
    },
    **{
        make_native_type(typecls): timezone_instantiator(typecls)
        for typecls in SQLALCHEMY_POSTGRES_TIMEZONE_TYPES
    },
    **{
        make_native_type(pg_types.ARRAY(inner_typecls)): typed_instantiator(
            pg_types.ARRAY, inner_typecls
        )
        for inner_typecls in SQLALCHEMY_POSTGRES_ARRAY_INNER_TYPES
    },
}
