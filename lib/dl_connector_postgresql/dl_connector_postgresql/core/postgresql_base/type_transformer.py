import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg_types

from dl_constants.enums import UserDataType
from dl_core.db.conversion_base import (
    TypeTransformer,
    UTCDatetimeTypeCaster,
    UTCTimezoneDatetimeTypeCaster,
    make_native_type,
)
from dl_sqlalchemy_postgres.base import CITEXT


PG_TYPES_INT = frozenset((pg_types.SMALLINT, pg_types.INTEGER, pg_types.BIGINT))
PG_TYPES_FLOAT = frozenset((pg_types.REAL, pg_types.DOUBLE_PRECISION, pg_types.NUMERIC))
PG_TYPES_STRING = frozenset((pg_types.CHAR, pg_types.VARCHAR, pg_types.TEXT, CITEXT))


class PostgreSQLTypeTransformer(TypeTransformer):
    casters = {
        **TypeTransformer.casters,  # type: ignore  # TODO: fix
        # Preliminary asyncpg-related hack: before inserting, make all datetimes UTC-naive.
        # A correct fix would require different BI-types for naive/aware datetimes.
        UserDataType.datetime: UTCDatetimeTypeCaster(),
        UserDataType.genericdatetime: UTCTimezoneDatetimeTypeCaster(),
    }
    native_to_user_map = {
        **{make_native_type(t): UserDataType.integer for t in PG_TYPES_INT},  # type: ignore  # TODO: fix
        **{make_native_type(t): UserDataType.float for t in PG_TYPES_FLOAT},
        make_native_type(pg_types.BOOLEAN): UserDataType.boolean,
        **{make_native_type(t): UserDataType.string for t in PG_TYPES_STRING},
        make_native_type(pg_types.DATE): UserDataType.date,
        make_native_type(pg_types.TIMESTAMP): UserDataType.genericdatetime,
        make_native_type(pg_types.UUID): UserDataType.uuid,
        make_native_type(pg_types.ENUM): UserDataType.string,
        **{make_native_type(pg_types.ARRAY(typecls)): UserDataType.array_int for typecls in PG_TYPES_INT},
        **{make_native_type(pg_types.ARRAY(typecls)): UserDataType.array_float for typecls in PG_TYPES_FLOAT},
        **{make_native_type(pg_types.ARRAY(typecls)): UserDataType.array_str for typecls in PG_TYPES_STRING},
        make_native_type(sa.sql.sqltypes.NullType): UserDataType.unsupported,
    }
    user_to_native_map = {
        UserDataType.integer: make_native_type(pg_types.BIGINT),
        UserDataType.float: make_native_type(pg_types.DOUBLE_PRECISION),
        UserDataType.boolean: make_native_type(pg_types.BOOLEAN),
        UserDataType.string: make_native_type(pg_types.TEXT),
        UserDataType.date: make_native_type(pg_types.DATE),
        UserDataType.datetime: make_native_type(pg_types.TIMESTAMP),
        UserDataType.genericdatetime: make_native_type(pg_types.TIMESTAMP),
        UserDataType.geopoint: make_native_type(pg_types.TEXT),
        UserDataType.geopolygon: make_native_type(pg_types.TEXT),
        UserDataType.uuid: make_native_type(pg_types.UUID),
        UserDataType.markup: make_native_type(pg_types.TEXT),
        UserDataType.array_int: make_native_type(pg_types.ARRAY(pg_types.BIGINT)),
        UserDataType.array_float: make_native_type(pg_types.ARRAY(pg_types.DOUBLE_PRECISION)),
        UserDataType.array_str: make_native_type(pg_types.ARRAY(pg_types.TEXT)),
        UserDataType.tree_str: make_native_type(pg_types.ARRAY(pg_types.TEXT)),
        UserDataType.unsupported: make_native_type(sa.sql.sqltypes.NullType),
    }
