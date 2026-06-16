import pytest

from dl_formula_testing.database import FormulaDbConfig
from dl_formula_testing.testcases.base import FormulaConnectorTestBase
from dl_testing.regulated_test import RegulatedTestCase

from dl_connector_greenplum_tests.db import config as test_config
from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D


class GreenplumTestBase(FormulaConnectorTestBase, RegulatedTestCase):
    """Base class for Greenplum formula tests.

    Greenplum uses the PostgreSQL formula plugin, so we use the POSTGRESQL_9_4 dialect.
    Tests are only run against GP7.
    """

    dialect = D.POSTGRESQL_9_4
    supports_arrays = True
    supports_uuid = True

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_URLS[test_config.GreenplumVersion.GP7]

    @pytest.fixture(scope="function")
    def enabled_pgcrypto_extension(self, db_config: FormulaDbConfig) -> None:
        db = self.db_dispenser.get_database(db_config)
        try:
            db.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
        except Exception:
            pytest.skip("pgcrypto extension not available")
        yield
        try:
            db.execute("DROP EXTENSION IF EXISTS pgcrypto;")
        except Exception:
            pass
