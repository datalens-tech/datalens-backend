from typing import Optional

import attr

from bi_configs.settings_loaders.settings_obj_base import SettingsBase
from bi_constants.enums import ConnectorAvailability


@attr.s(kw_only=True)
class TranslatableSettings(SettingsBase):
    text: str = attr.ib()
    domain: Optional[str] = attr.ib(default=None)


@attr.s(kw_only=True)
class ConnectorBaseSettings(SettingsBase):
    pass


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
    connectors: list[ConnectorBaseSettings] = attr.ib(validator=attr.validators.min_len(1))  # type: ignore


@attr.s(kw_only=True)
class ConnectorAvailabilityConfigSettings(SettingsBase):
    uncategorized: list[ConnectorSettings] = attr.ib(factory=list)
    sections: list[SectionSettings] = attr.ib(factory=list)

    visible_connectors: set[str] = attr.ib(factory=set)
