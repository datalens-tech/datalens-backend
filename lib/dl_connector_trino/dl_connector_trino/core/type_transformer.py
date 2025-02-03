from sqlalchemy import types as trino_sa

from dl_constants.enums import UserDataType
from dl_type_transformer.type_transformer import (
    TypeTransformer,
    make_native_type,
)


# from sqlalchemy.sql import sqltypes as trino_sa


TRINO_TYPES_INT = frozenset(
    (
        trino_sa.BIGINT,
        trino_sa.SMALLINT,
        trino_sa.INTEGER,
        # trino_sa.TINYINT,
    )
)
TRINO_TYPES_FLOAT = frozenset(
    (
        trino_sa.FLOAT,
        trino_sa.DECIMAL,
        # trino_sa.DEC,
        # trino_sa.DOUBLE,
        trino_sa.REAL,
    )
)
TRINO_TYPES_STRING = frozenset(
    (
        # trino_sa.STRING,
        trino_sa.VARCHAR,
        trino_sa.CHAR,
        # trino_sa.CHARACTER,
        trino_sa.TEXT,
    )
)


class TrinoTypeTransformer(TypeTransformer):
    # https://trino.io/docs/current/language/types.html

    native_to_user_map = {
        make_native_type(trino_sa.DATE): UserDataType.date,
        make_native_type(trino_sa.TIMESTAMP): UserDataType.genericdatetime,
        **{make_native_type(t): UserDataType.integer for t in TRINO_TYPES_INT},
        **{make_native_type(t): UserDataType.float for t in TRINO_TYPES_FLOAT},
        **{make_native_type(t): UserDataType.string for t in TRINO_TYPES_STRING},
        make_native_type(trino_sa.BOOLEAN): UserDataType.boolean,
    }
    user_to_native_map = {
        UserDataType.date: make_native_type(trino_sa.DATE),
        UserDataType.genericdatetime: make_native_type(trino_sa.TIMESTAMP),
        UserDataType.integer: make_native_type(trino_sa.INTEGER),
        UserDataType.string: make_native_type(trino_sa.VARCHAR),
        UserDataType.boolean: make_native_type(trino_sa.BOOLEAN),
    }
    casters = {
        **TypeTransformer.casters,
    }
