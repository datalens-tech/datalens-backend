import pytest

from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec

from bi_connector_bundle_partners.kontur_market.core.constants import SOURCE_TYPE_KONTUR_MARKET_CH_TABLE
from bi_connector_bundle_partners.kontur_market.core.data_source import KonturMarketCHDataSource
from bi_connector_bundle_partners.kontur_market.core.us_connection import KonturMarketCHConnection
from bi_connector_bundle_partners_tests.db.base.core.data_source import PartnersDataSourceTestClass
import bi_connector_bundle_partners_tests.db.config as test_config
from bi_connector_bundle_partners_tests.db.kontur_market.core.base import BaseKonturMarketTestClass


class TestKonturMarketDataSource(
    BaseKonturMarketTestClass,
    PartnersDataSourceTestClass[
        KonturMarketCHConnection,
        StandardSQLDataSourceSpec,
        KonturMarketCHDataSource,
    ],
):
    DSRC_CLS = KonturMarketCHDataSource

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self) -> StandardSQLDataSourceSpec:
        dsrc_spec = StandardSQLDataSourceSpec(
            source_type=SOURCE_TYPE_KONTUR_MARKET_CH_TABLE,
            db_name=test_config.DB_NAME,
            table_name=test_config.TABLE_NAME,
        )
        return dsrc_spec
