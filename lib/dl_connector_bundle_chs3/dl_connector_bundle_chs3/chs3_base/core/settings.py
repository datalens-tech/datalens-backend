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
    SECURE: bool = s_attrib("SECURE", missing=True)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "bool")  [assignment]
    HOST: str = s_attrib("HOST")  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
    PORT: int = s_attrib("PORT", missing=8443)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "int")  [assignment]
    USERNAME: str = s_attrib("USERNAME")  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
    PASSWORD: str = s_attrib("PASSWORD", sensitive=True, missing=None)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]

    ACCESS_KEY_ID: str = s_attrib("ACCESS_KEY_ID", sensitive=True, missing=None)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
    SECRET_ACCESS_KEY: str = s_attrib("SECRET_ACCESS_KEY", sensitive=True, missing=None)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
    S3_ENDPOINT: str = s_attrib("S3_ENDPOINT")  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
    BUCKET: str = s_attrib("BUCKET")  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]

    REPLACE_SECRET_SALT: str = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
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
