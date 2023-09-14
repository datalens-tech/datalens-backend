import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg_types

from bi_constants.enums import BIType
from bi_core.db.conversion_base import (
    TypeTransformer,
    UTCDatetimeTypeCaster,
    UTCTimezoneDatetimeTypeCaster,
    make_native_type,
)
from bi_sqlalchemy_postgres.base import CITEXT

from bi_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES

PG_TYPES_INT = frozenset((pg_types.SMALLINT, pg_types.INTEGER, pg_types.BIGINT))
PG_TYPES_FLOAT = frozenset((pg_types.REAL, pg_types.DOUBLE_PRECISION, pg_types.NUMERIC))
PG_TYPES_STRING = frozenset((pg_types.CHAR, pg_types.VARCHAR, pg_types.TEXT, CITEXT))


class PostgreSQLTypeTransformer(TypeTransformer):
    conn_type = CONNECTION_TYPE_POSTGRES
    casters = {
        **TypeTransformer.casters,  # type: ignore  # TODO: fix
        # Preliminary asyncpg-related hack: before inserting, make all datetimes UTC-naive.
        # A correct fix would require different BI-types for naive/aware datetimes.
        BIType.datetime: UTCDatetimeTypeCaster(),
        BIType.genericdatetime: UTCTimezoneDatetimeTypeCaster(),
    }
    native_to_user_map = {
        **{make_native_type(CONNECTION_TYPE_POSTGRES, t): BIType.integer for t in PG_TYPES_INT},  # type: ignore  # TODO: fix
        **{make_native_type(CONNECTION_TYPE_POSTGRES, t): BIType.float for t in PG_TYPES_FLOAT},
        make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.BOOLEAN): BIType.boolean,
        **{make_native_type(CONNECTION_TYPE_POSTGRES, t): BIType.string for t in PG_TYPES_STRING},
        make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.DATE): BIType.date,
        make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.TIMESTAMP): BIType.genericdatetime,
        make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.UUID): BIType.uuid,
        make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.ENUM): BIType.string,
        **{
            make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.ARRAY(typecls)): BIType.array_int
            for typecls in PG_TYPES_INT
        },
        **{
            make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.ARRAY(typecls)): BIType.array_float
            for typecls in PG_TYPES_FLOAT
        },
        **{
            make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.ARRAY(typecls)): BIType.array_str
            for typecls in PG_TYPES_STRING
        },
        make_native_type(CONNECTION_TYPE_POSTGRES, sa.sql.sqltypes.NullType): BIType.unsupported,
    }
    user_to_native_map = {
        BIType.integer: make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.BIGINT),
        BIType.float: make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.DOUBLE_PRECISION),
        BIType.boolean: make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.BOOLEAN),
        BIType.string: make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.TEXT),
        BIType.date: make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.DATE),
        BIType.datetime: make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.TIMESTAMP),
        BIType.genericdatetime: make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.TIMESTAMP),
        BIType.geopoint: make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.TEXT),
        BIType.geopolygon: make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.TEXT),
        BIType.uuid: make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.UUID),
        BIType.markup: make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.TEXT),
        BIType.array_int: make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.ARRAY(pg_types.BIGINT)),
        BIType.array_float: make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.ARRAY(pg_types.DOUBLE_PRECISION)),
        BIType.array_str: make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.ARRAY(pg_types.TEXT)),
        BIType.tree_str: make_native_type(CONNECTION_TYPE_POSTGRES, pg_types.ARRAY(pg_types.TEXT)),
        BIType.unsupported: make_native_type(CONNECTION_TYPE_POSTGRES, sa.sql.sqltypes.NullType),
    }
