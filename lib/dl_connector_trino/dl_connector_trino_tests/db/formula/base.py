import pytest

from dl_formula_testing.testcases.base import FormulaConnectorTestBase

from dl_connector_trino.formula.constants import TrinoDialect as D
import dl_connector_trino_tests.db.config as test_config


class TrinoFormulaTestBase(FormulaConnectorTestBase):
    dialect = D.TRINO
    supports_arrays = False  # TODO: @khamitovdr - fix n.func.GET_ITEM and turn this to True
    supports_uuid = False  # TODO: @khamitovdr - ckeck if UUID is supported

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_CORE_URL_MEMORY_CATALOG
