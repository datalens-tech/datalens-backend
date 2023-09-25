from __future__ import annotations

from sqlalchemy import util
from sqlalchemy.dialects.postgresql import (
    INTERVAL,
    OID,
    REGCLASS,
    UUID,
)

# we dont use SA asyncpg beta objects
# but it's ok to use type descriptions
from sqlalchemy.dialects.postgresql.asyncpg import (
    AsyncpgBigInteger,
    AsyncpgBoolean,
    AsyncpgDate,
    AsyncpgDateTime,
    AsyncPgEnum,
    AsyncpgInteger,
    AsyncPgInterval,
    AsyncpgJSON,
    AsyncpgJSONB,
    AsyncpgJSONIndexType,
    AsyncpgJSONIntIndexType,
    AsyncpgJSONPathType,
    AsyncpgJSONStrIndexType,
    AsyncpgNumeric,
    AsyncpgOID,
    AsyncpgREGCLASS,
    AsyncpgTime,
    AsyncpgUUID,
)
from sqlalchemy.dialects.postgresql.asyncpg import json as asynpg_json
from sqlalchemy.sql import sqltypes

from dl_sqlalchemy_postgres.base import (
    CITEXT,
    BIPGDialect,
)


# for backward compatibility with SA psycopg
class AsyncpgFloat(AsyncpgNumeric):
    def get_dbapi_type(self, dbapi):
        return dbapi.NUMBER


# types mapping for an asyncpg driver
# without any requests to a database
# we have to replace it by native SA asyncpg tools after their release
# see https://docs.sqlalchemy.org/en/14/orm/extensions/asyncio.html
class DBAPIMock:
    def __init__(self, *args, **kwargs):
        pass

    STRING = util.symbol("STRING")
    TIMESTAMP = util.symbol("TIMESTAMP")
    TIMESTAMP_W_TZ = util.symbol("TIMESTAMP_W_TZ")
    TIME = util.symbol("TIME")
    DATE = util.symbol("DATE")
    INTERVAL = util.symbol("INTERVAL")
    NUMBER = util.symbol("NUMBER")
    FLOAT = util.symbol("FLOAT")
    BOOLEAN = util.symbol("BOOLEAN")
    INTEGER = util.symbol("INTEGER")
    BIGINTEGER = util.symbol("BIGINTEGER")
    BYTES = util.symbol("BYTES")
    DECIMAL = util.symbol("DECIMAL")
    JSON = util.symbol("JSON")
    JSONB = util.symbol("JSONB")
    ENUM = util.symbol("ENUM")
    UUID = util.symbol("UUID")
    BYTEA = util.symbol("BYTEA")
    CITEXT = util.symbol("CITEXT")
    DATETIME = TIMESTAMP
    BINARY = BYTEA

    pg_types = {
        STRING: "varchar",
        TIMESTAMP: "timestamp",
        TIMESTAMP_W_TZ: "timestamp with time zone",
        DATE: "date",
        TIME: "time",
        INTERVAL: "interval",
        NUMBER: "numeric",
        FLOAT: "float",
        BOOLEAN: "bool",
        INTEGER: "integer",
        BIGINTEGER: "bigint",
        BYTES: "bytes",
        DECIMAL: "decimal",
        JSON: "json",
        JSONB: "jsonb",
        ENUM: "enum",
        UUID: "uuid",
        BYTEA: "bytea",
        CITEXT: "citext",
    }


# special dialect for asyncpg adapter
# native SA asyncpg dialect is beta now
# we can use it after release
# see https://docs.sqlalchemy.org/en/14/orm/extensions/asyncio.html
class AsyncBIPGDialect(BIPGDialect):
    # we have to use _get_set_input_sizes_lookup from compiled query
    use_setinputsizes = True
    implicit_returning = True
    supports_native_enum = True
    supports_smallserial = True  # 9.2+
    supports_sane_multi_rowcount = True  # psycopg 2.0.9+
    _has_native_hstore = True  # type: ignore  # TODO: fix
    _backslash_escapes = False

    colspecs = util.update_copy(
        BIPGDialect.colspecs,
        {
            sqltypes.Time: AsyncpgTime,
            sqltypes.Date: AsyncpgDate,
            sqltypes.DateTime: AsyncpgDateTime,
            sqltypes.Interval: AsyncPgInterval,
            INTERVAL: AsyncPgInterval,
            UUID: AsyncpgUUID,
            sqltypes.Boolean: AsyncpgBoolean,
            sqltypes.Integer: AsyncpgInteger,
            sqltypes.BigInteger: AsyncpgBigInteger,
            sqltypes.Numeric: AsyncpgNumeric,
            sqltypes.JSON: AsyncpgJSON,
            asynpg_json.JSONB: AsyncpgJSONB,
            sqltypes.JSON.JSONPathType: AsyncpgJSONPathType,
            sqltypes.JSON.JSONIndexType: AsyncpgJSONIndexType,
            sqltypes.JSON.JSONIntIndexType: AsyncpgJSONIntIndexType,
            sqltypes.JSON.JSONStrIndexType: AsyncpgJSONStrIndexType,
            sqltypes.Enum: AsyncPgEnum,
            OID: AsyncpgOID,
            REGCLASS: AsyncpgREGCLASS,
            # special override for floats
            sqltypes.Float: AsyncpgFloat,
            CITEXT: CITEXT,
        },
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # special hack for getting types in compile query
        self.dbapi = DBAPIMock()
