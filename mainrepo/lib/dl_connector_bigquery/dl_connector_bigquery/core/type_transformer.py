import sqlalchemy_bigquery._types as bq_types

from dl_constants.enums import BIType
from dl_core.db.conversion_base import (
    TypeTransformer,
    make_native_type,
)

from dl_connector_bigquery.core.constants import CONNECTION_TYPE_BIGQUERY


class BigQueryTypeTransformer(TypeTransformer):
    conn_type = CONNECTION_TYPE_BIGQUERY
    native_to_user_map = {
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.DATE): BIType.date,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.DATETIME): BIType.genericdatetime,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.STRING): BIType.string,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.BOOLEAN): BIType.boolean,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.INTEGER): BIType.integer,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.FLOAT): BIType.float,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.NUMERIC): BIType.float,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.ARRAY(bq_types.INTEGER)): BIType.array_int,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.ARRAY(bq_types.STRING)): BIType.array_str,
        make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.ARRAY(bq_types.FLOAT)): BIType.array_float,
    }
    user_to_native_map = {
        BIType.date: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.DATE),
        BIType.genericdatetime: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.DATETIME),
        BIType.string: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.STRING),
        BIType.boolean: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.BOOLEAN),
        BIType.integer: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.INTEGER),
        BIType.float: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.FLOAT),
        BIType.array_int: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.ARRAY(bq_types.INTEGER)),
        BIType.array_str: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.ARRAY(bq_types.STRING)),
        BIType.array_float: make_native_type(CONNECTION_TYPE_BIGQUERY, bq_types.ARRAY(bq_types.FLOAT)),
    }
