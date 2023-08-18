import pytest


from bi_core.data_source_spec.sql import StandardSQLDataSourceSpec

from bi_connector_bundle_partners.equeo.core.constants import SOURCE_TYPE_EQUEO_CH_TABLE
from bi_connector_bundle_partners.equeo.core.data_source import EqueoCHDataSource
from bi_connector_bundle_partners.equeo.core.us_connection import EqueoCHConnection

from bi_connector_bundle_partners_tests.db.base.core.data_source import PartnersDataSourceTestClass
from bi_connector_bundle_partners_tests.db.equeo.core.base import BaseEqueoTestClass

import bi_connector_bundle_partners_tests.db.config as test_config


class TestEqueoDataSource(
        BaseEqueoTestClass,
        PartnersDataSourceTestClass[
            EqueoCHConnection,
            StandardSQLDataSourceSpec,
            EqueoCHDataSource,
        ]
):
    DSRC_CLS = EqueoCHDataSource

    @pytest.fixture(scope='class')
    def initial_data_source_spec(self) -> StandardSQLDataSourceSpec:
        dsrc_spec = StandardSQLDataSourceSpec(
            source_type=SOURCE_TYPE_EQUEO_CH_TABLE,
            db_name=test_config.DB_NAME,
            table_name=test_config.TABLE_NAME,
        )
        return dsrc_spec
