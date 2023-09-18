import sqlalchemy.sql.functions as sa_funcs
from sqlalchemy_bigquery.base import BigQueryDialect as SABigQueryDialect

from dl_connector_bigquery.formula.constants import BigQueryDialect as BigQueryDialectNS
from dl_connector_bigquery.formula.definitions.all import DEFINITIONS
from dl_formula.connectors.base.connector import FormulaConnector


class BigQueryFormulaConnector(FormulaConnector):
    dialect_ns_cls = BigQueryDialectNS
    dialects = BigQueryDialectNS.BIGQUERY
    default_dialect = BigQueryDialectNS.BIGQUERY
    op_definitions = DEFINITIONS
    sa_dialect = SABigQueryDialect()

    @classmethod
    def registration_hook(cls) -> None:
        # Unregister BigQuery's custom implementation of `unnest` because it breaks other connectors
        # https://github.com/googleapis/python-bigquery-sqlalchemy/issues/882
        del sa_funcs._registry["_default"]["unnest"]  # noqa
