from __future__ import annotations

import pytest

from dl_core.us_manager.us_manager_sync import SyncUSManager
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.constants import (
    CONNECTION_TYPE_SMB_HEATMAPS,
)
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.settings import SMBHeatmapsConnectorSettings
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.us_connection import (
    ConnectionClickhouseSMBHeatmaps,
)
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.testing.connection import (
    make_saved_smb_heatmaps_connection,
)

import bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config as test_config
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.base import (
    BaseClickhouseFilteredSubselectByPuidTestClass, ClickhouseFilteredSubselectByPuidTestClassWithWrongAuth,
)


class BaseSMBHeatmapsTestClass(BaseClickhouseFilteredSubselectByPuidTestClass[ConnectionClickhouseSMBHeatmaps]):
    conn_type = CONNECTION_TYPE_SMB_HEATMAPS
    connection_settings = SMBHeatmapsConnectorSettings(**test_config.SR_CONNECTION_SETTINGS_PARAMS)

    @pytest.fixture(scope='function')
    def saved_connection(
            self, sync_us_manager: SyncUSManager, connection_creation_params: dict
    ) -> ConnectionClickhouseSMBHeatmaps:
        conn = make_saved_smb_heatmaps_connection(
            sync_usm=sync_us_manager,
            **connection_creation_params
        )
        return sync_us_manager.get_by_id(conn.uuid)  # to invoke a lifecycle manager


class SMBHeatmapsTestClassWithWrongAuth(
        BaseSMBHeatmapsTestClass,
        ClickhouseFilteredSubselectByPuidTestClassWithWrongAuth[ConnectionClickhouseSMBHeatmaps],
):
    pass
