import uuid

import pydantic

from dl_core.connectors.settings.base import ConnectorSettings
import dl_settings

from dl_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE


class _RootSettings(dl_settings.BaseRootSettings):
    S3_ENDPOINT_URL: str = NotImplemented
    FILE_UPLOADER_S3_PERSISTENT_BUCKET_NAME: str = NotImplemented


class FileS3ConnectorSettingsBase(ConnectorSettings):
    type: str = CONNECTION_TYPE_FILE.value

    model_config = pydantic.ConfigDict(alias_generator=dl_settings.prefix_alias_generator("CONN_FILE_"))

    SECURE: bool = True
    HOST: str
    PORT: int = 8443
    USERNAME: str
    PASSWORD: str

    ACCESS_KEY_ID: str = pydantic.Field(repr=False)
    SECRET_ACCESS_KEY: str = pydantic.Field(repr=False)

    REPLACE_SECRET_SALT: str = pydantic.Field(repr=False, default_factory=lambda: str(uuid.uuid4()))
    # ^ Note that this is used in a query, which, in turn, is used in a cache key at the moment
    #   This means that the value must be set explicitly to preserve caches between restarts and instances

    root: _RootSettings = pydantic.Field(default_factory=_RootSettings)

    @property
    def S3_ENDPOINT(self) -> str:
        return self.root.S3_ENDPOINT_URL

    @property
    def BUCKET(self) -> str:
        return self.root.FILE_UPLOADER_S3_PERSISTENT_BUCKET_NAME
