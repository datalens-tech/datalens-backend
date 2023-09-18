from __future__ import annotations

import json
from typing import Optional

import attr

from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.settings_loaders.meta_definition import s_attrib


@attr.s(frozen=True)
class PartnerKeys:
    """Keys by versions"""

    dl_private: dict[str, str] = attr.ib(repr=False)
    partner_public: dict[str, str] = attr.ib(repr=False)

    @classmethod
    def from_json(cls, json_data: str) -> Optional[PartnerKeys]:
        if not json_data:
            return None
        data = json.loads(json_data)
        return PartnerKeys(
            dl_private=data["dl_private"],
            partner_public=data["partner_public"],
        )


@attr.s(frozen=True)
class PartnerConnectorSettingsBase(ConnectorSettingsBase):
    ALLOW_PUBLIC_USAGE: bool = True

    SECURE: bool = s_attrib("SECURE", missing=True)  # type: ignore
    HOST: str = s_attrib("HOST")  # type: ignore
    PORT: int = s_attrib("PORT", missing=8443)  # type: ignore
    USERNAME: str = s_attrib("USERNAME")  # type: ignore
    PASSWORD: str = s_attrib("PASSWORD", sensitive=True, missing=None)  # type: ignore
    USE_MANAGED_NETWORK: bool = s_attrib("USE_MANAGED_NETWORK")  # type: ignore
    PARTNER_KEYS: PartnerKeys = s_attrib(  # type: ignore
        "PARTNER_KEYS",
        env_var_converter=PartnerKeys.from_json,
        missing=None,
    )
