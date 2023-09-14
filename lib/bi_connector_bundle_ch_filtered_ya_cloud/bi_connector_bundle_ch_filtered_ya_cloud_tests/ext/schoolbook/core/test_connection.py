from __future__ import annotations

from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.settings import SchoolbookConnectorSettings
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.us_connection import (
    ConnectionClickhouseSchoolbook,
)

from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config import SR_CONNECTION_SETTINGS_PARAMS
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.connection import (
    BaseClickhouseFilteredSubselectByPuidConnectionTestClass,
    ClickhouseFilteredSubselectByPuidConnectionTestWithWrongAuth,
)

from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.schoolbook.core.base import (
    BaseSchoolbookTestClass, SchoolbookTestClassWithWrongAuth
)


class TestSchoolbookConnection(
        BaseSchoolbookTestClass,
        BaseClickhouseFilteredSubselectByPuidConnectionTestClass[ConnectionClickhouseSchoolbook],
):
    sr_connection_settings = SchoolbookConnectorSettings(**SR_CONNECTION_SETTINGS_PARAMS)


class TestSchoolbookConnectionWithWrongAuth(
        SchoolbookTestClassWithWrongAuth,
        ClickhouseFilteredSubselectByPuidConnectionTestWithWrongAuth
):
    pass
