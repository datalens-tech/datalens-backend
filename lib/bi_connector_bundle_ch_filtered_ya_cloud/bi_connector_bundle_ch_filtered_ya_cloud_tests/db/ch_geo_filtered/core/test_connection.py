from __future__ import annotations

import attr

from bi_core.us_connection_base import DataSourceTemplate

from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.constants import (
    SOURCE_TYPE_CH_GEO_FILTERED_TABLE,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.us_connection import ConnectionClickhouseGeoFiltered

from bi_core_testing.testcases.connection import DefaultConnectionTestClass

from bi_connector_bundle_ch_filtered_ya_cloud_tests.db.ch_geo_filtered.core.base import BaseCHGeoFilteredClass
from bi_connector_bundle_ch_filtered_ya_cloud_tests.db.ch_geo_filtered.config import DB_NAME, TABLE_NAME


class TestCHGeoFilteredConnection(
        BaseCHGeoFilteredClass,
        DefaultConnectionTestClass[ConnectionClickhouseGeoFiltered],
):
    def check_saved_connection(self, conn: ConnectionClickhouseGeoFiltered, params: dict) -> None:
        assert conn.uuid is not None
        conn_data = attr.asdict(conn.data)
        assert params.items() <= conn_data.items()

    def check_data_source_templates(
            self, conn: ConnectionClickhouseGeoFiltered, dsrc_templates: list[DataSourceTemplate]
    ) -> None:
        expected_template = DataSourceTemplate(
            title=TABLE_NAME,
            group=[DB_NAME],
            connection_id=conn.uuid,
            source_type=SOURCE_TYPE_CH_GEO_FILTERED_TABLE,
            parameters=dict(db_name=DB_NAME, table_name=TABLE_NAME),
        )
        assert [expected_template] == dsrc_templates
