from typing import (
    Optional,
)

import attr
from attr import make_class

from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_configs.settings_loaders.settings_obj_base import SettingsBase
from dl_constants.enums import ConnectionType


def generate_connectors_settings_class(
    settings_registry: dict[ConnectionType, type[ConnectorSettingsBase]],
) -> type[attr.AttrsInstance]:
    attrs: dict[str, attr.Attribute] = {}
    for conn_type, settings_class in settings_registry.items():
        field_name = conn_type.value.upper()
        attrs[field_name] = s_attrib(field_name, type_class=Optional[settings_class], missing=None)

    return make_class("ConnectorSettingsByTypeGenerated", attrs=attrs, bases=(SettingsBase,), frozen=True)
