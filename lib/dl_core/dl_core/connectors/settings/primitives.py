from typing import (  # noqa: F401
    Any,
    ClassVar,
    Optional,
    Type,
)

import attr

from dl_core.connectors.settings.base import ConnectorSettings


@attr.s(frozen=True)
class ConnectorSettingsDefinition:
    pydantic_settings_class: ClassVar[type[ConnectorSettings]] = ConnectorSettings
