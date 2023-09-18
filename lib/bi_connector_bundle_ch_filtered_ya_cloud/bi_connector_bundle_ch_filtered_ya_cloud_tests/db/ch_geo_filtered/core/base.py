from __future__ import annotations

import asyncio
from typing import Generator

import pytest

from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.testcases.connection import BaseConnectionTestClass

from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.constants import CONNECTION_TYPE_CH_GEO_FILTERED
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.testing.connection import (
    make_saved_ch_geo_filtered_connection,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.us_connection import ConnectionClickhouseGeoFiltered
import bi_connector_bundle_ch_filtered_ya_cloud_tests.db.ch_geo_filtered.config as test_config
import bi_connector_bundle_ch_filtered_ya_cloud_tests.db.config as common_test_config


class BaseCHGeoFilteredClass(BaseConnectionTestClass[ConnectionClickhouseGeoFiltered]):
    conn_type = CONNECTION_TYPE_CH_GEO_FILTERED
    core_test_config = common_test_config.CORE_TEST_CONFIG

    @pytest.fixture(autouse=True)
    # FIXME: This fixture is a temporary solution for failing core tests when they are run together with api tests
    def loop(self, event_loop: asyncio.AbstractEventLoop) -> Generator[asyncio.AbstractEventLoop, None, None]:
        asyncio.set_event_loop(event_loop)
        yield event_loop
        # Attempt to cover an old version of pytest-asyncio:
        # https://github.com/pytest-dev/pytest-asyncio/commit/51d986cec83fdbc14fa08015424c79397afc7ad9
        asyncio.set_event_loop_policy(None)

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return common_test_config.DB_CORE_URL

    @pytest.fixture(scope="function")
    def connection_creation_params(self) -> dict:
        return test_config.CONNECTION_PARAMS

    @pytest.fixture(scope="function")
    def saved_connection(
        self,
        sync_us_manager: SyncUSManager,
        connection_creation_params: dict,
    ) -> ConnectionClickhouseGeoFiltered:
        conn = make_saved_ch_geo_filtered_connection(sync_usm=sync_us_manager, **connection_creation_params)
        return conn


class DownloadableCHGeoFilteredClass(BaseCHGeoFilteredClass):
    @pytest.fixture(scope="function")
    def connection_creation_params(self) -> dict:
        return test_config.DOWNLOADABLE_CONNECTION_PARAMS
