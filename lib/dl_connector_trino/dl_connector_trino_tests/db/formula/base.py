from frozendict import frozendict
import pytest

from dl_formula_testing.testcases.base import FormulaConnectorTestBase

from dl_connector_trino.formula.constants import TrinoDialect as D
import dl_connector_trino_tests.db.config as test_config


class TrinoFormulaTestBase(FormulaConnectorTestBase):
    dialect = D.TRINO
    supports_arrays = True
    supports_uuid = False  # TODO: @khamitovdr - ckeck if UUID is supported

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_CORE_URL

    @pytest.fixture(scope="class")
    def table_schema_name(self) -> str:
        return "default"

    @pytest.fixture(scope="class")
    def engine_params(self) -> dict:
        engine_params = {
            "connect_args": frozendict(
                {
                    "timezone": "UTC",
                }
            ),
        }
        return engine_params
