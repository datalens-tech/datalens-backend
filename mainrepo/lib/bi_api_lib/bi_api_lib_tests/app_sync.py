from __future__ import annotations

from typing import Optional

import flask

from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_constants.enums import ConnectionType

from bi_api_lib.app_settings import (
    ControlApiAppSettings,
    ControlPlaneAppTestingsSettings,
)
from bi_api_lib.loader import load_bi_api_lib
from bi_api_lib_testing.app import TestingControlApiAppFactory


def create_app(
        app_settings: ControlApiAppSettings,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
        testing_app_settings: Optional[ControlPlaneAppTestingsSettings] = None,
        close_loop_after_request: bool = True,
) -> flask.Flask:
    mng_app_factory = TestingControlApiAppFactory(settings=app_settings)
    load_bi_api_lib()
    return mng_app_factory.create_app(
        connectors_settings=connectors_settings,
        testing_app_settings=testing_app_settings,
        close_loop_after_request=close_loop_after_request,
    )
