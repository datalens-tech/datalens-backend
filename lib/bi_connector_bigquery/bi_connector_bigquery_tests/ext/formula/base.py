import re

import pytest
import sqlalchemy.exc as sa_exc

from bi_formula.connectors.base.testing.base import FormulaConnectorTestBase

from bi_connector_bigquery.db_testing.engine_wrapper import BigQueryDbEngineConfig
from bi_connector_bigquery.formula.constants import BigQueryDialect as D


class BigQueryTestBase(FormulaConnectorTestBase):
    dialect = D.BIGQUERY
    eval_attempts = 5
    retry_on_exceptions = (
        (sa_exc.DatabaseError, re.compile('404 Not found: Table')),
    )
    engine_config_cls = BigQueryDbEngineConfig

    supports_arrays = False
    supports_uuid = False
    null_casts_to_false = True

    @pytest.fixture(scope='class')
    def engine_params(self, bq_secrets) -> dict:
        return dict(
            credentials_base64=bq_secrets.get_creds(),
        )

    @pytest.fixture(scope='class')
    def engine_config(self, db_url: str, engine_params: dict, bq_secrets) -> BigQueryDbEngineConfig:
        default_dataset_name = bq_secrets.get_dataset_name()
        return BigQueryDbEngineConfig(
            url=db_url, engine_params=engine_params,
            default_dataset_name=default_dataset_name,
        )

    @pytest.fixture(scope='class')
    def db_url(self, bq_secrets) -> str:
        return f'bigquery://{bq_secrets.get_project_id()}'
