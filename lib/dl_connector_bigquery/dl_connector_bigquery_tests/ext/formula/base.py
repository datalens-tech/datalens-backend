import re

import pytest
import sqlalchemy.exc as sa_exc

from dl_formula_testing.testcases.base import FormulaConnectorTestBase

from dl_connector_bigquery.db_testing.engine_wrapper import BigQueryDbEngineConfig
from dl_connector_bigquery.formula.constants import BigQueryDialect as D
from dl_connector_bigquery_tests.ext.settings import Settings


class BigQueryTestBase(FormulaConnectorTestBase):
    dialect = D.BIGQUERY
    eval_attempts = 5
    retry_on_exceptions = ((sa_exc.DatabaseError, re.compile("404 Not found: Table")),)
    engine_config_cls = BigQueryDbEngineConfig

    supports_arrays = False
    supports_uuid = False
    null_casts_to_false = True

    @pytest.fixture(scope="class")
    def engine_params(self, settings: Settings) -> dict:
        return dict(
            credentials_base64=settings.BIGQUERY_CREDS,
        )

    @pytest.fixture(scope="class")
    def engine_config(self, db_url: str, engine_params: dict, settings: Settings) -> BigQueryDbEngineConfig:
        default_dataset_name = settings.BIGQUERY_CONFIG["dataset_name"]
        return BigQueryDbEngineConfig(
            url=db_url,
            engine_params=engine_params,
            default_dataset_name=default_dataset_name,
        )

    @pytest.fixture(scope="class")
    def db_url(self, settings: Settings) -> str:
        return f"bigquery://{settings.BIGQUERY_CONFIG['project_id']}"
