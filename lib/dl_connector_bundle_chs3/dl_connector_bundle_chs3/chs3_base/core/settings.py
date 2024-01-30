from typing import (
    ClassVar,
    Optional,
)
import uuid

import attr

from dl_configs.connectors_data import ConnectorsDataBase
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.settings_loaders.meta_definition import s_attrib


@attr.s(frozen=True)
class FileS3ConnectorSettings(ConnectorSettingsBase):
    SECURE: bool = s_attrib("SECURE", missing=True)
    HOST: str = s_attrib("HOST")
    PORT: int = s_attrib("PORT", missing=8443)
    USERNAME: str = s_attrib("USERNAME")
    PASSWORD: str = s_attrib("PASSWORD", sensitive=True, missing=None)

    ACCESS_KEY_ID: str = s_attrib("ACCESS_KEY_ID", sensitive=True, missing=None)
    SECRET_ACCESS_KEY: str = s_attrib("SECRET_ACCESS_KEY", sensitive=True, missing=None)
    S3_ENDPOINT: str = s_attrib("S3_ENDPOINT")
    BUCKET: str = s_attrib("BUCKET")

    REPLACE_SECRET_SALT: str = s_attrib(
        "REPLACE_SECRET_SALT", sensitive=True, missing_factory=lambda: str(uuid.uuid4())
    )
    # ^ Note that this is used in a query, which, in turn, is used in a cache key at the moment
    #   This means that the value must be set explicitly to preserve caches between restarts and instances


class ConnectorsDataFileBase(ConnectorsDataBase):
    CONN_FILE_CH_HOST: ClassVar[Optional[str]] = None
    CONN_FILE_CH_PORT: ClassVar[int] = 8443
    CONN_FILE_CH_USERNAME: ClassVar[Optional[str]] = None

    @classmethod
    def connector_name(cls) -> str:
        return "FILE"
