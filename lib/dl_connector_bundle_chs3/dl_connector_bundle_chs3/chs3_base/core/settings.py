import uuid

import attr

from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.settings_loaders.meta_definition import s_attrib


@attr.s(frozen=True)
class FileS3ConnectorSettings(ConnectorSettingsBase):
    SECURE: bool = s_attrib("SECURE", missing=True)  # type: ignore
    HOST: str = s_attrib("HOST")  # type: ignore
    PORT: int = s_attrib("PORT", missing=8443)  # type: ignore
    USERNAME: str = s_attrib("USERNAME")  # type: ignore
    PASSWORD: str = s_attrib("PASSWORD", sensitive=True, missing=None)  # type: ignore

    ACCESS_KEY_ID: str = s_attrib("ACCESS_KEY_ID", sensitive=True, missing=None)  # type: ignore
    SECRET_ACCESS_KEY: str = s_attrib("SECRET_ACCESS_KEY", sensitive=True, missing=None)  # type: ignore
    S3_ENDPOINT: str = s_attrib("S3_ENDPOINT")  # type: ignore
    BUCKET: str = s_attrib("BUCKET")  # type: ignore

    REPLACE_SECRET_SALT: str = s_attrib(  # type: ignore
        "REPLACE_SECRET_SALT", sensitive=True, missing_factory=lambda: str(uuid.uuid4())
    )
    # ^ Note that this is used in a query, which, in turn, is used in a cache key at the moment
    #   This means that the value must be set explicitly to preserve caches between restarts and instances
