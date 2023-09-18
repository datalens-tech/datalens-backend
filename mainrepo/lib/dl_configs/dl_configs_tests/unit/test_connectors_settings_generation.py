from typing import Optional

import attr
from attr import fields

from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.settings_loaders.connectors_settings import generate_connectors_settings_class
from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_configs.settings_loaders.settings_obj_base import SettingsBase
from dl_constants.enums import ConnectionType

CONNECTION_TYPE_APPALLING = ConnectionType.declare("appalling")
CONNECTION_TYPE_EMPTY_YET_COOL = ConnectionType.declare("empty_yet_cool")
CONNECTION_TYPE_FANTASTIC = ConnectionType.declare("fantastic")
CONNECTION_TYPE_WONDERFUL = ConnectionType.declare("wonderful")


@attr.s(frozen=True)
class WonderfulConnectorSettings(ConnectorSettingsBase):
    WONDERFUL_STRING_SETTING: str = s_attrib("WONDERFUL_STRING_SETTING", missing=None)
    WONDERFUL_LIST_OF_STRINGS_SETTING: list[str] = s_attrib("WONDERFUL_LIST_OF_STRINGS_SETTING", missing=None)


@attr.s(frozen=True)
class FantasticConnectorSettings(ConnectorSettingsBase):
    FANTASTIC_INTEGER_SETTING: int = s_attrib("FANTASTIC_INTEGER_SETTING", missing=None)
    FANTASTIC_FLOAT_SETTING: float = s_attrib("FANTASTIC_FLOAT_SETTING", missing=None)


@attr.s(frozen=True)
class EmptyYetCoolConnectorSettings(ConnectorSettingsBase):
    pass


@attr.s(frozen=True)
class AppallingConnectorSettings(ConnectorSettingsBase):
    APPALING_SET_SETTING: set[int] = s_attrib("APPALING_SET_SETTING", missing=None)


@attr.s(frozen=True)
class ExpectedConnectorsSettings(SettingsBase):
    EMPTY_YET_COOL: Optional[EmptyYetCoolConnectorSettings] = s_attrib("EMPTY_YET_COOL", missing=None)
    FANTASTIC: Optional[FantasticConnectorSettings] = s_attrib("FANTASTIC", missing=None)
    WONDERFUL: Optional[WonderfulConnectorSettings] = s_attrib("WONDERFUL", missing=None)


def test_connectors_settings_class_generator():
    settings_registry = {
        CONNECTION_TYPE_EMPTY_YET_COOL: EmptyYetCoolConnectorSettings,
        CONNECTION_TYPE_FANTASTIC: FantasticConnectorSettings,
        CONNECTION_TYPE_WONDERFUL: WonderfulConnectorSettings,
    }
    generated_class = generate_connectors_settings_class(settings_registry=settings_registry)
    assert issubclass(generated_class, SettingsBase)
    assert fields(generated_class) == fields(ExpectedConnectorsSettings)


def test_connectors_settings_class_generation_with_whitelist():
    settings_registry = {
        CONNECTION_TYPE_APPALLING: AppallingConnectorSettings,
        CONNECTION_TYPE_EMPTY_YET_COOL: EmptyYetCoolConnectorSettings,
        CONNECTION_TYPE_FANTASTIC: FantasticConnectorSettings,
        CONNECTION_TYPE_WONDERFUL: WonderfulConnectorSettings,
    }
    whitelist = {CONNECTION_TYPE_EMPTY_YET_COOL, CONNECTION_TYPE_FANTASTIC, CONNECTION_TYPE_WONDERFUL}
    generated_class = generate_connectors_settings_class(settings_registry=settings_registry, whitelist=whitelist)
    assert issubclass(generated_class, SettingsBase)
    assert fields(generated_class) == fields(ExpectedConnectorsSettings)
