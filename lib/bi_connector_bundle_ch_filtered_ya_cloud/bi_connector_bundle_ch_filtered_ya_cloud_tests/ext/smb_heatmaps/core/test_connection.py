from __future__ import annotations

from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.settings import SMBHeatmapsConnectorSettings
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.us_connection import ConnectionClickhouseSMBHeatmaps
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config import SR_CONNECTION_SETTINGS_PARAMS
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.connection import (
    BaseClickhouseFilteredSubselectByPuidConnectionTestClass,
    ClickhouseFilteredSubselectByPuidConnectionTestWithWrongAuth,
)
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.smb_heatmaps.core.base import (
    BaseSMBHeatmapsTestClass,
    SMBHeatmapsTestClassWithWrongAuth,
)


class TestSMBHeatmapsConnection(
    BaseSMBHeatmapsTestClass,
    BaseClickhouseFilteredSubselectByPuidConnectionTestClass[ConnectionClickhouseSMBHeatmaps],
):
    sr_connection_settings = SMBHeatmapsConnectorSettings(**SR_CONNECTION_SETTINGS_PARAMS)


class TestSMBHeatmapsConnectionWithWrongAuth(
    SMBHeatmapsTestClassWithWrongAuth, ClickhouseFilteredSubselectByPuidConnectionTestWithWrongAuth
):
    pass
