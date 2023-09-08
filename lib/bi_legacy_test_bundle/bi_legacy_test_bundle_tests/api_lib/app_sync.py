from __future__ import annotations

from typing import Optional

import flask

from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_constants.enums import ConnectionType

from bi_api_lib.app_settings import (
    ControlApiAppSettings,
    ControlApiAppTestingsSettings,
)
from bi_api_lib.loader import ApiLibraryConfig, load_bi_api_lib
from bi_api_lib_testing_ya.app import TestingControlApiAppFactoryPrivate


def create_app(
        app_settings: ControlApiAppSettings,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
        testing_app_settings: Optional[ControlApiAppTestingsSettings] = None,
        close_loop_after_request: bool = True,
) -> flask.Flask:
    mng_app_factory = TestingControlApiAppFactoryPrivate(settings=app_settings)
    load_bi_api_lib(ApiLibraryConfig(api_connector_ep_names=app_settings.CONNECTOR_WHITELIST))
    return mng_app_factory.create_app(
        connectors_settings=connectors_settings,
        testing_app_settings=testing_app_settings,
        close_loop_after_request=close_loop_after_request,
    )
