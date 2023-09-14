import datetime

import pytest
import pytz

from bi_formula_testing.testcases.base import FormulaConnectorTestBase

from bi_connector_clickhouse.db_testing.engine_wrapper import ClickhouseDbEngineConfig
from bi_connector_clickhouse.formula.constants import ClickHouseDialect as D
from bi_connector_clickhouse_tests.db.config import DB_URLS


class ClickHouseTestBase(FormulaConnectorTestBase):
    engine_config_cls = ClickhouseDbEngineConfig
    supports_arrays = True
    supports_uuid = True

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return DB_URLS[self.dialect]


class ClickHouse_21_8TestBase(ClickHouseTestBase):
    dialect = D.CLICKHOUSE_21_8

    @pytest.fixture(scope="class")
    def tzinfo(self) -> datetime.tzinfo:
        # The database is configured to use a custom timezone
        return pytz.timezone("America/New_York")


class ClickHouse_22_10TestBase(ClickHouseTestBase):
    dialect = D.CLICKHOUSE_22_10
