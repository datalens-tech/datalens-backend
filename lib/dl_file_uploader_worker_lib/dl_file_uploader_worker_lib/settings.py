from typing import (
    Annotated,
    Optional,
)

import attr
import pydantic

from dl_api_lib.app_settings import postload_connectors_settings
from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_configs.settings_loaders.settings_obj_base import SettingsBase
from dl_configs.settings_submodels import GoogleAppSettings
from dl_configs.utils import (
    get_root_certificates_path,
    split_by_comma,
)
from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.us_manager.settings import USClientSettings
from dl_file_uploader_lib.settings import (
    DeprecatedFileUploaderBaseSettings,
    FileUploaderBaseSettings,
)
import dl_settings

from dl_connector_bundle_chs3.chs3_gsheets.core.constants import CONNECTION_TYPE_GSHEETS_V2
from dl_connector_bundle_chs3.chs3_yadocs.core.constants import CONNECTION_TYPE_YADOCS
from dl_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE


@attr.s(frozen=True)
class SecureReader(SettingsBase):
    SOCKET: str = s_attrib("SOCKET")  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
    ENDPOINT: Optional[str] = s_attrib("ENDPOINT", missing=None)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str | None")  [assignment]
    CAFILE: Optional[str] = s_attrib("CAFILE", missing=None)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str | None")  [assignment]


@attr.s(frozen=True)
class DeprecatedFileUploaderWorkerSettings(DeprecatedFileUploaderBaseSettings):
    MAX_CONCURRENT_JOBS: int = s_attrib("MAX_CONCURRENT_JOBS", missing=15)  # type: ignore

    SENTRY_DSN: Optional[str] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str | None")  [assignment]
        "DL_SENTRY_DSN",
        fallback_cfg_key="SENTRY_DSN_FILE_UPLOADER_WORKER",
        missing=None,
    )

    US_BASE_URL: str = s_attrib("US_HOST", fallback_cfg_key="US_BASE_URL")  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
    US_MASTER_TOKEN: str = s_attrib("US_MASTER_TOKEN", sensitive=True)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]

    GSHEETS_APP: GoogleAppSettings = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "GoogleAppSettings")  [assignment]
        # TODO: check gsheets connector availability
        "GSHEETS_APP",
    )

    ENABLE_REGULAR_S3_LC_RULES_CLEANUP: bool = s_attrib("ENABLE_REGULAR_S3_LC_RULES_CLEANUP", missing=False)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "bool")  [assignment]
    SECURE_READER: SecureReader = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "SecureReader")  [assignment]
        "SECURE_READER",
        fallback_factory=(
            lambda: SecureReader(  # type: ignore  # 2024-01-30 # TODO: Argument "fallback_factory" to "s_attrib" has incompatible type "Callable[[], SecureReader]"; expected "Callable[[Any, Any], Any] | Callable[[Any], Any] | None"  [arg-type]
                SOCKET="/var/reader.sock",
                ENDPOINT=None,
                CAFILE=None,
            )
        ),
    )

    CA_FILE_PATH: str = s_attrib("CA_FILE_PATH", missing=get_root_certificates_path())  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
    EXTRA_CA_FILE_PATHS: tuple[str, ...] = s_attrib(  # type: ignore # 2024-07-29 # TODO
        "EXTRA_CA_FILE_PATHS",
        env_var_converter=split_by_comma,
        missing_factory=tuple,
    )


def file_uploader_worker_postload_connectors_settings(
    value: dict[str, ConnectorSettings]
) -> dict[str, ConnectorSettings]:
    """
    Validator function to populate missing connector settings with defaults for file uploader worker app.

    In yaml/envs for file-worker application, we only specify settings for the CONNECTION_TYPE_FILE connector,
    and reuse these same settings for CONNECTION_TYPE_GSHEETS_V2 and CONNECTION_TYPE_YADOCS.
    """
    conn_types_to_fill_manually = [CONNECTION_TYPE_GSHEETS_V2.value, CONNECTION_TYPE_YADOCS.value]
    conn_file_settings = ConnectorSettings.classes[CONNECTION_TYPE_FILE.value]
    assert issubclass(conn_file_settings, ConnectorSettings)
    for conn_type in conn_types_to_fill_manually:
        value[conn_type] = conn_file_settings()
    return postload_connectors_settings(value)


class FileUploaderWorkerSettings(FileUploaderBaseSettings):
    US_CLIENT: USClientSettings = pydantic.Field(default_factory=USClientSettings)
    CONNECTORS: Annotated[
        dl_settings.TypedDictWithTypeKeyAnnotation[ConnectorSettings],
        pydantic.AfterValidator(file_uploader_worker_postload_connectors_settings),
    ] = pydantic.Field(default_factory=dict)
