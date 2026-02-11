import uuid

import pydantic

from dl_core.connectors.settings.base import ConnectorSettings
import dl_settings

from dl_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE


class _RootSettings(dl_settings.BaseRootSettings):
    S3_ENDPOINT_URL: str | None = None
    FILE_UPLOADER_S3_PERSISTENT_BUCKET_NAME: str | None = None


class FileS3ConnectorSettingsBase(ConnectorSettings):
    type: str = CONNECTION_TYPE_FILE.value

    model_config = pydantic.ConfigDict(alias_generator=dl_settings.prefix_alias_generator("CONN_FILE_CH_"))

    SECURE: bool = True
    HOST: str
    PORT: int = 8443
    USERNAME: str
    PASSWORD: str
    S3_ENDPOINT_URL: str | None = pydantic.Field(default=None, alias="S3_ENDPOINT")
    S3_BUCKET: str | None = pydantic.Field(default=None, alias="BUCKET")

    ACCESS_KEY_ID: str = pydantic.Field(repr=False)
    SECRET_ACCESS_KEY: str = pydantic.Field(repr=False)

    REPLACE_SECRET_SALT: str = pydantic.Field(repr=False, default_factory=lambda: str(uuid.uuid4()))
    # ^ Note that this is used in a query, which, in turn, is used in a cache key at the moment
    #   This means that the value must be set explicitly to preserve caches between restarts and instances

    root: _RootSettings = pydantic.Field(default_factory=_RootSettings)

    @pydantic.model_validator(mode="after")
    def _validate_s3_settings(self) -> "FileS3ConnectorSettingsBase":
        if self.S3_ENDPOINT_URL is None and self.root.S3_ENDPOINT_URL is None:
            raise ValueError("Either S3_ENDPOINT_URL or root.S3_ENDPOINT_URL must be set")
        if self.S3_BUCKET is None and self.root.FILE_UPLOADER_S3_PERSISTENT_BUCKET_NAME is None:
            raise ValueError("Either BUCKET or root.FILE_UPLOADER_S3_PERSISTENT_BUCKET_NAME must be set")
        return self

    @property
    def S3_ENDPOINT(self) -> str:
        result = self.S3_ENDPOINT_URL or self.root.S3_ENDPOINT_URL
        assert result is not None
        return result

    @property
    def BUCKET(self) -> str:
        result = self.S3_BUCKET or self.root.FILE_UPLOADER_S3_PERSISTENT_BUCKET_NAME
        assert result is not None
        return result
