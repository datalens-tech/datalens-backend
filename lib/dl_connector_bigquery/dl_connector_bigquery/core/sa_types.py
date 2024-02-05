import sqlalchemy_bigquery._types as bq_types

from dl_core.db.sa_types_base import (
    make_native_type,
    simple_instantiator,
)

from dl_connector_bigquery.core.constants import BACKEND_TYPE_BIGQUERY


SQLALCHEMY_BIGQUERY_TYPES = {
    (BACKEND_TYPE_BIGQUERY, make_native_type(bq_types.DATE)): simple_instantiator(bq_types.DATE),
    (BACKEND_TYPE_BIGQUERY, make_native_type(bq_types.DATETIME)): simple_instantiator(bq_types.DATE),
    (BACKEND_TYPE_BIGQUERY, make_native_type(bq_types.STRING)): simple_instantiator(bq_types.STRING),
    (BACKEND_TYPE_BIGQUERY, make_native_type(bq_types.BOOLEAN)): simple_instantiator(bq_types.BOOLEAN),
    (BACKEND_TYPE_BIGQUERY, make_native_type(bq_types.INTEGER)): simple_instantiator(bq_types.INTEGER),
    (BACKEND_TYPE_BIGQUERY, make_native_type(bq_types.FLOAT)): simple_instantiator(bq_types.FLOAT),
    (BACKEND_TYPE_BIGQUERY, make_native_type(bq_types.NUMERIC)): simple_instantiator(bq_types.NUMERIC),
    (
        BACKEND_TYPE_BIGQUERY,
        make_native_type(bq_types.ARRAY(bq_types.INTEGER)),
    ): simple_instantiator(bq_types.ARRAY(bq_types.INTEGER)),
    (
        BACKEND_TYPE_BIGQUERY,
        make_native_type(bq_types.ARRAY(bq_types.STRING)),
    ): simple_instantiator(bq_types.ARRAY(bq_types.STRING)),
    (
        BACKEND_TYPE_BIGQUERY,
        make_native_type(bq_types.ARRAY(bq_types.FLOAT)),
    ): simple_instantiator(bq_types.ARRAY(bq_types.FLOAT)),
}
