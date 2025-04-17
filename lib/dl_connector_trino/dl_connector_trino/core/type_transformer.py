import sqlalchemy as sa
import trino.sqlalchemy.datatype as tsa

from dl_constants.enums import UserDataType
from dl_type_transformer.type_transformer import (
    TypeTransformer,
    make_native_type,
)


TRINO_TYPES_INT = frozenset(
    (
        sa.SMALLINT,
        sa.INTEGER,
        sa.BIGINT,
    )
)
TRINO_TYPES_FLOAT = frozenset(
    (
        sa.REAL,
        tsa.DOUBLE,
        sa.DECIMAL,
    )
)
TRINO_TYPES_STRING = frozenset(
    (
        sa.VARCHAR,
        sa.CHAR,
        sa.VARBINARY,
        tsa.JSON,
    )
)


class TrinoTypeTransformer(TypeTransformer):
    # https://trino.io/docs/current/language/types.html

    native_to_user_map = {
        make_native_type(sa.BOOLEAN): UserDataType.boolean,
        **{make_native_type(t): UserDataType.integer for t in TRINO_TYPES_INT},
        **{make_native_type(t): UserDataType.float for t in TRINO_TYPES_FLOAT},
        **{make_native_type(t): UserDataType.string for t in TRINO_TYPES_STRING},
        # make_native_type(sa.Uuid) : UserDataType.uuid, # Uuid is not supported by sqlalchemy v1.4.46
        make_native_type(sa.DATE): UserDataType.date,
        make_native_type(tsa.TIMESTAMP): UserDataType.genericdatetime,
        **{make_native_type(sa.ARRAY(t)): UserDataType.array_int for t in TRINO_TYPES_INT},
        **{make_native_type(sa.ARRAY(t)): UserDataType.array_float for t in TRINO_TYPES_FLOAT},
        **{make_native_type(sa.ARRAY(t)): UserDataType.array_str for t in TRINO_TYPES_STRING},
        make_native_type(sa.sql.sqltypes.NullType): UserDataType.unsupported,
    }
    user_to_native_map = {
        UserDataType.string: make_native_type(sa.VARCHAR),
        UserDataType.integer: make_native_type(sa.BIGINT),
        UserDataType.float: make_native_type(tsa.DOUBLE),
        UserDataType.boolean: make_native_type(sa.BOOLEAN),
        UserDataType.date: make_native_type(sa.DATE),
        UserDataType.datetime: make_native_type(tsa.TIMESTAMP),
        UserDataType.datetimetz: make_native_type(tsa.TIMESTAMP(timezone=True)),
        UserDataType.genericdatetime: make_native_type(tsa.TIMESTAMP),
        UserDataType.geopoint: make_native_type(sa.VARCHAR),
        UserDataType.geopolygon: make_native_type(sa.VARCHAR),
        UserDataType.uuid: make_native_type(sa.VARCHAR),
        UserDataType.markup: make_native_type(sa.VARCHAR),
        UserDataType.unsupported: make_native_type(sa.sql.sqltypes.NullType),
        UserDataType.array_int: make_native_type(sa.ARRAY(sa.BIGINT)),
        UserDataType.array_float: make_native_type(sa.ARRAY(tsa.DOUBLE)),
        UserDataType.array_str: make_native_type(sa.ARRAY(sa.VARCHAR)),
        UserDataType.tree_str: make_native_type(sa.ARRAY(sa.VARCHAR)),
    }
