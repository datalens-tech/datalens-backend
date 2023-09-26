import sqlalchemy_bigquery._types as bq_types

from dl_connector_bigquery.core.constants import CONNECTION_TYPE_BIGQUERY
from dl_constants.enums import UserDataType
from dl_core.db.conversion_base import (
    TypeTransformer,
    make_native_type,
)


class BigQueryTypeTransformer(TypeTransformer):
    conn_type = CONNECTION_TYPE_BIGQUERY
    native_to_user_map = {
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.DATE): UserDataType.date,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.DATETIME): UserDataType.genericdatetime,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.STRING): UserDataType.string,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.BOOLEAN): UserDataType.boolean,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.INTEGER): UserDataType.integer,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.FLOAT): UserDataType.float,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.NUMERIC): UserDataType.float,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.ARRAY(bq_types.INTEGER)): UserDataType.array_int,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.ARRAY(bq_types.STRING)): UserDataType.array_str,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.ARRAY(bq_types.FLOAT)): UserDataType.array_float,
    }
    user_to_native_map = {
        UserDataType.date: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.DATE),
        UserDataType.genericdatetime: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.DATETIME),
        UserDataType.string: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.STRING),
        UserDataType.boolean: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.BOOLEAN),
        UserDataType.integer: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.INTEGER),
        UserDataType.float: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.FLOAT),
        UserDataType.array_int: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.ARRAY(bq_types.INTEGER)),
        UserDataType.array_str: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.ARRAY(bq_types.STRING)),
        UserDataType.array_float: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.ARRAY(bq_types.FLOAT)),
    }
