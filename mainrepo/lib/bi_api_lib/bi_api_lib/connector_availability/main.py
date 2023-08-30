from __future__ import annotations

from typing import Optional

from bi_configs.enums import EnvType

from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from bi_api_lib.connector_availability.configs.development import CONFIG as DEVELOPMENT_CONNECTORS
from bi_api_lib.connector_availability.configs.int_testing import CONFIG as INT_TESTING_CONNECTORS
from bi_api_lib.connector_availability.configs.int_production import CONFIG as INT_PRODUCTION_CONNECTORS
from bi_api_lib.connector_availability.configs.ext_testing import CONFIG as EXT_TESTING_CONNECTORS
from bi_api_lib.connector_availability.configs.ext_production import CONFIG as EXT_PRODUCTION_CONNECTORS
from bi_api_lib.connector_availability.configs.doublecloud import CONFIG as DOUBLECLOUD_CONNECTORS
from bi_api_lib.connector_availability.configs.israel import CONFIG as ISRAEL_CONNECTORS
from bi_api_lib.connector_availability.configs.nemax import CONFIG as NEMAX_CONNECTORS


def get_connector_availability_config_for_env(env: EnvType) -> Optional[ConnectorAvailabilityConfig]:
    # FIXME: Connectorize: BI-4613
    return {
        EnvType.development: DEVELOPMENT_CONNECTORS,
        EnvType.int_testing: INT_TESTING_CONNECTORS,
        EnvType.yc_testing: EXT_TESTING_CONNECTORS,
        EnvType.int_production: INT_PRODUCTION_CONNECTORS,
        EnvType.yc_production: EXT_PRODUCTION_CONNECTORS,
        EnvType.dc_any: DOUBLECLOUD_CONNECTORS,
        EnvType.israel: ISRAEL_CONNECTORS,
        EnvType.nemax: NEMAX_CONNECTORS,
    }[env]
