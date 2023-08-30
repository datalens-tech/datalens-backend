import sqlalchemy_bigquery._types as bq_types

from bi_connector_bigquery.core.constants import CONNECTION_TYPE_BIGQUERY as CT_BQ
from bi_core.db.sa_types_base import make_native_type, simple_instantiator


SQLALCHEMY_BIGQUERY_TYPES = {
    make_native_type(CT_BQ, bq_types.DATE): simple_instantiator(bq_types.DATE),
    make_native_type(CT_BQ, bq_types.DATETIME): simple_instantiator(bq_types.DATE),
    make_native_type(CT_BQ, bq_types.STRING): simple_instantiator(bq_types.STRING),
    make_native_type(CT_BQ, bq_types.BOOLEAN): simple_instantiator(bq_types.BOOLEAN),
    make_native_type(CT_BQ, bq_types.INTEGER): simple_instantiator(bq_types.INTEGER),
    make_native_type(CT_BQ, bq_types.FLOAT): simple_instantiator(bq_types.FLOAT),
    make_native_type(CT_BQ, bq_types.NUMERIC): simple_instantiator(bq_types.NUMERIC),
    make_native_type(CT_BQ, bq_types.ARRAY(bq_types.INTEGER)): simple_instantiator(bq_types.ARRAY(bq_types.INTEGER)),
    make_native_type(CT_BQ, bq_types.ARRAY(bq_types.STRING)): simple_instantiator(bq_types.ARRAY(bq_types.STRING)),
    make_native_type(CT_BQ, bq_types.ARRAY(bq_types.FLOAT)): simple_instantiator(bq_types.ARRAY(bq_types.FLOAT)),
}
