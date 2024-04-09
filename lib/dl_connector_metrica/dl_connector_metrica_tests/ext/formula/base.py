import re

import pytest
import sqlalchemy.exc as sa_exc

from dl_formula_testing.testcases.base import FormulaConnectorTestBase

from dl_connector_metrica.db_testing.engine_wrapper import BigQueryDbEngineConfig
from dl_connector_metrica.formula.constants import MetricaDialect as D


class MetricaTestBase(FormulaConnectorTestBase):
    dialect = D.METRIKAAPI

    @pytest.fixture(scope="class")
    def engine_params(self, bq_secrets) -> dict:
        return dict(
            credentials_base64=bq_secrets.get_creds(),
        )

    @pytest.fixture(scope="class")
    def engine_config(self, db_url: str, engine_params: dict, bq_secrets) -> BigQueryDbEngineConfig:
        default_dataset_name = bq_secrets.get_dataset_name()
        return BigQueryDbEngineConfig(
            url=db_url,
            engine_params=engine_params,
            default_dataset_name=default_dataset_name,
        )

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return ""
