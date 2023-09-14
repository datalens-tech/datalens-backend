import uuid
from typing import Generator

import pytest

from bi_constants.enums import ConnectionType

from bi_connector_clickhouse.db_testing.engine_wrapper import ClickhouseDbEngineConfig

from bi_connector_bundle_ch_filtered.base.core.settings import CHFrozenConnectorSettings
from bi_connector_bundle_ch_frozen.ch_frozen_base.core.constants import (
    SOURCE_TYPE_CH_FROZEN_SOURCE, SOURCE_TYPE_CH_FROZEN_SUBSELECT,
)
from bi_connector_bundle_ch_frozen.ch_frozen_base.core.us_connection import ConnectionClickhouseFrozenBase
from bi_connector_bundle_ch_frozen.ch_frozen_bumpy_roads.core.constants import CONNECTION_TYPE_CH_FROZEN_BUMPY_ROADS
from bi_connector_bundle_ch_frozen.ch_frozen_bumpy_roads.core.settings import CHFrozenBumpyRoadsConnectorSettings
from bi_connector_bundle_ch_frozen.ch_frozen_covid.core.constants import CONNECTION_TYPE_CH_FROZEN_COVID
from bi_connector_bundle_ch_frozen.ch_frozen_covid.core.settings import CHFrozenCovidConnectorSettings
from bi_connector_bundle_ch_frozen.ch_frozen_demo.core.constants import CONNECTION_TYPE_CH_FROZEN_DEMO
from bi_connector_bundle_ch_frozen.ch_frozen_demo.core.settings import CHFrozenDemoConnectorSettings
from bi_connector_bundle_ch_frozen.ch_frozen_dtp.core.constants import CONNECTION_TYPE_CH_FROZEN_DTP
from bi_connector_bundle_ch_frozen.ch_frozen_dtp.core.settings import CHFrozenDTPConnectorSettings
from bi_connector_bundle_ch_frozen.ch_frozen_gkh.core.constants import CONNECTION_TYPE_CH_FROZEN_GKH
from bi_connector_bundle_ch_frozen.ch_frozen_gkh.core.settings import CHFrozenGKHConnectorSettings
from bi_connector_bundle_ch_frozen.ch_frozen_horeca.core.constants import CONNECTION_TYPE_CH_FROZEN_HORECA
from bi_connector_bundle_ch_frozen.ch_frozen_horeca.core.settings import CHFrozenHorecaConnectorSettings
from bi_connector_bundle_ch_frozen.ch_frozen_samples.core.constants import CONNECTION_TYPE_CH_FROZEN_SAMPLES
from bi_connector_bundle_ch_frozen.ch_frozen_samples.core.settings import CHFrozenSamplesConnectorSettings
from bi_connector_bundle_ch_frozen.ch_frozen_transparency.core.constants import CONNECTION_TYPE_CH_FROZEN_TRANSPARENCY
from bi_connector_bundle_ch_frozen.ch_frozen_transparency.core.settings import CHFrozenTransparencyConnectorSettings
from bi_connector_bundle_ch_frozen.ch_frozen_weather.core.constants import CONNECTION_TYPE_CH_FROZEN_WEATHER
from bi_connector_bundle_ch_frozen.ch_frozen_weather.core.settings import CHFrozenWeatherConnectorSettings

from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_core_testing.database import DbTable
from bi_core_testing.testcases.service_base import ServiceFixtureTextClass

from bi_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from bi_api_lib_testing.connection_base import ConnectionTestBase
from bi_api_lib_testing.dataset_base import DatasetTestBase
from bi_api_lib_testing.data_api_base import StandardizedDataApiTestBase

from bi_connector_bundle_ch_frozen_tests.db.config import (
    BI_TEST_CONFIG, CONNECTION_PARAMS, CORE_TEST_CONFIG, DB_CORE_URL, SR_CONNECTION_SETTINGS_PARAMS
)


class CHFrozenConnectionTestBase(ConnectionTestBase, ServiceFixtureTextClass):
    core_test_config = CORE_TEST_CONFIG
    bi_compeng_pg_on = False

    @pytest.fixture(scope='class', params=[
        CONNECTION_TYPE_CH_FROZEN_BUMPY_ROADS,
        CONNECTION_TYPE_CH_FROZEN_COVID,
        CONNECTION_TYPE_CH_FROZEN_DEMO,
        CONNECTION_TYPE_CH_FROZEN_DTP,
        CONNECTION_TYPE_CH_FROZEN_GKH,
        CONNECTION_TYPE_CH_FROZEN_HORECA,
        CONNECTION_TYPE_CH_FROZEN_SAMPLES,
        CONNECTION_TYPE_CH_FROZEN_TRANSPARENCY,
        CONNECTION_TYPE_CH_FROZEN_WEATHER,
    ], ids=lambda param: param.value)
    def _conn_type(self, request) -> ConnectionType:
        request.cls.conn_type = request.param
        return request.param

    @pytest.fixture(scope='class')
    def db_url(self, _conn_type: ConnectionType) -> str:
        return DB_CORE_URL

    @pytest.fixture(scope='class')
    def engine_config(self, db_url: str, engine_params: dict) -> ClickhouseDbEngineConfig:
        return ClickhouseDbEngineConfig(url=db_url, engine_params=engine_params)

    @pytest.fixture(scope='class')
    def connectors_settings(self, sample_table: DbTable) -> dict[ConnectionType, CHFrozenConnectorSettings]:
        params = SR_CONNECTION_SETTINGS_PARAMS | dict(
            ALLOWED_TABLES=[sample_table.name],
            DB_NAME=sample_table.db.name,
        )
        return {
            CONNECTION_TYPE_CH_FROZEN_BUMPY_ROADS: CHFrozenBumpyRoadsConnectorSettings(**params),
            CONNECTION_TYPE_CH_FROZEN_COVID: CHFrozenCovidConnectorSettings(**params),
            CONNECTION_TYPE_CH_FROZEN_DEMO: CHFrozenDemoConnectorSettings(**params),
            CONNECTION_TYPE_CH_FROZEN_DTP: CHFrozenDTPConnectorSettings(**params),
            CONNECTION_TYPE_CH_FROZEN_GKH: CHFrozenGKHConnectorSettings(**params),
            CONNECTION_TYPE_CH_FROZEN_HORECA: CHFrozenHorecaConnectorSettings(**params),
            CONNECTION_TYPE_CH_FROZEN_SAMPLES: CHFrozenSamplesConnectorSettings(**params),
            CONNECTION_TYPE_CH_FROZEN_TRANSPARENCY: CHFrozenTransparencyConnectorSettings(**params),
            CONNECTION_TYPE_CH_FROZEN_WEATHER: CHFrozenWeatherConnectorSettings(**params),
        }

    @pytest.fixture(scope='class')
    def bi_test_config(self) -> BiApiTestEnvironmentConfiguration:
        return BI_TEST_CONFIG

    @pytest.fixture(scope='class')
    def connection_params(self) -> dict:
        return CONNECTION_PARAMS

    @pytest.fixture(scope='function')
    def saved_connection_id(
            self, conn_default_sync_us_manager: SyncUSManager, connection_params: dict,
    ) -> Generator[str, None, None]:
        usm = conn_default_sync_us_manager
        conn_name = f'{self.conn_type.name} test conn {uuid.uuid4()}'
        conn = ConnectionClickhouseFrozenBase.create_from_dict(
            ConnectionClickhouseFrozenBase.DataModel(**connection_params),
            ds_key='connections/tests/{}'.format(conn_name),
            type_=self.conn_type.name,
            meta={'title': conn_name, 'state': 'saved'},
            us_manager=usm,
        )

        usm.save(conn)
        yield conn.uuid
        usm.delete(conn)


class CHFrozenDatasetTestBase(CHFrozenConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope='class')
    def dataset_params(self, sample_table: DbTable) -> dict:
        return dict(
            source_type=SOURCE_TYPE_CH_FROZEN_SOURCE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
            ),
        )


class CHFrozenDatasetSubselectTestBase(CHFrozenConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope='class')
    def dataset_params(self, sample_table: DbTable) -> dict:
        return dict(
            source_type=SOURCE_TYPE_CH_FROZEN_SUBSELECT.name,
            parameters=dict(
                subsql=f'select * from {sample_table.db.name}.{sample_table.name} limit 100',
                title='My SQL Source',
            ),
        )


class CHFrozenDataApiTestBase(CHFrozenDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_on = False


class CHFrozenDataApiSubselectTestBase(CHFrozenDatasetSubselectTestBase, StandardizedDataApiTestBase):
    mutation_caches_on = False
