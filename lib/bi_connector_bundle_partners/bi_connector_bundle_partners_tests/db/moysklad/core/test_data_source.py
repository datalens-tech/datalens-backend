import pytest


from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec

from bi_connector_bundle_partners.moysklad.core.constants import SOURCE_TYPE_MOYSKLAD_CH_TABLE
from bi_connector_bundle_partners.moysklad.core.data_source import MoySkladCHDataSource
from bi_connector_bundle_partners.moysklad.core.us_connection import MoySkladCHConnection

from bi_connector_bundle_partners_tests.db.base.core.data_source import PartnersDataSourceTestClass
from bi_connector_bundle_partners_tests.db.moysklad.core.base import BaseMoySkladTestClass

import bi_connector_bundle_partners_tests.db.config as test_config


class TestMoySkladDataSource(
        BaseMoySkladTestClass,
        PartnersDataSourceTestClass[
            MoySkladCHConnection,
            StandardSQLDataSourceSpec,
            MoySkladCHDataSource,
        ]
):
    DSRC_CLS = MoySkladCHDataSource

    @pytest.fixture(scope='class')
    def initial_data_source_spec(self) -> StandardSQLDataSourceSpec:
        dsrc_spec = StandardSQLDataSourceSpec(
            source_type=SOURCE_TYPE_MOYSKLAD_CH_TABLE,
            db_name=test_config.DB_NAME,
            table_name=test_config.TABLE_NAME,
        )
        return dsrc_spec
