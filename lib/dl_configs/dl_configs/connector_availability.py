from typing import (
    Any,
    Optional,
)

import attr
from dynamic_enum import (
    AutoEnumValue,
    DynamicEnum,
)

from dl_configs.settings_loaders.settings_obj_base import SettingsBase
from dl_constants.enums import ConnectorAvailability


@attr.s(kw_only=True)
class TranslatableSettings(SettingsBase):
    text: str = attr.ib()
    domain: Optional[str] = attr.ib(default=None)


@attr.s(kw_only=True)
class ConnectorBaseSettings(SettingsBase):
    pass


class ConnectorIconSrcType(DynamicEnum):
    data = AutoEnumValue()
    url = AutoEnumValue()


@attr.s(kw_only=True)
class ConnectorIconSrc:
    icon_type: ConnectorIconSrcType = attr.ib(default=ConnectorIconSrcType.data)
    url_prefix: Optional[str] = attr.ib(default=None)
    data: Optional[str] = attr.ib(default=None)


@attr.s(kw_only=True)
class ConnectorSettings(ConnectorBaseSettings):
    conn_type: str = attr.ib()
    availability: ConnectorAvailability = attr.ib(default=ConnectorAvailability.free)


@attr.s(kw_only=True)
class ConnectorContainerSettings(ConnectorBaseSettings):
    title_translatable: TranslatableSettings = attr.ib()
    alias: str = attr.ib()
    includes: list[ConnectorSettings] = attr.ib()


@attr.s(kw_only=True)
class SectionSettings(SettingsBase):
    title_translatable: TranslatableSettings = attr.ib()
    connectors: list[ConnectorBaseSettings] = attr.ib(validator=attr.validators.min_len(1))


@attr.s(kw_only=True)
class ConnectorAvailabilityConfigSettings(SettingsBase):
    uncategorized: list[ConnectorSettings] = attr.ib(factory=list)
    sections: list[SectionSettings] = attr.ib(factory=list)

    visible_connectors: set[str] = attr.ib(factory=set)
    icon_src: ConnectorIconSrc = attr.ib(default=ConnectorIconSrc())
