from __future__ import annotations

import enum
import os
from typing import Optional

import attr

from dl_configs.settings_loaders.common import SDict
from dl_configs.settings_loaders.loader_env import EnvSettingsLoader
from dl_configs.settings_loaders.meta_definition import (
    required,
    s_attrib,
)
from dl_configs.settings_loaders.settings_obj_base import SettingsBase
from dl_configs.utils import validate_one_of


class RQEExecuteRequestMode(enum.Enum):
    STREAM = "stream"
    NON_STREAM = "non_stream"


@attr.s(frozen=True)
class RQEBaseURL(SettingsBase):
    scheme: str = s_attrib("SCHEME", env_var_converter=validate_one_of({"http", "https"}), missing="http")  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
    host: str = s_attrib("HOST")  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
    port: int = s_attrib("PORT")  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "int")  [assignment]

    def __str__(self) -> str:
        return f"{self.scheme}://{self.host}:{self.port}"


@attr.s(frozen=True)
class RQEConfig(SettingsBase):
    ext_sync_rqe: RQEBaseURL = s_attrib("EXT_SYNC")  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "RQEBaseURL")  [assignment]
    ext_async_rqe: RQEBaseURL = s_attrib("EXT_ASYNC")  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "RQEBaseURL")  [assignment]
    int_sync_rqe: RQEBaseURL = s_attrib("INT_SYNC")  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "RQEBaseURL")  [assignment]
    int_async_rqe: RQEBaseURL = s_attrib("INT_ASYNC")  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "RQEBaseURL")  [assignment]
    hmac_key: bytes = s_attrib(  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "bytes")  [assignment]
        "SECRET_KEY",
        sensitive=True,
        env_var_converter=lambda s: s.encode("ascii"),
    )
    execute_request_mode: RQEExecuteRequestMode = s_attrib(  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "RQEExecuteRequestMode")  [assignment]
        "EXECUTE_REQUEST_MODE",
        missing=RQEExecuteRequestMode.STREAM,
        env_var_converter=lambda s: RQEExecuteRequestMode[s.upper()],
    )

    @classmethod
    def get_default(cls) -> RQEConfig:
        return RQEConfig(  # type: ignore  # 2024-01-24 # TODO: Unexpected keyword argument "hmac_key" for "RQEConfig"  [call-arg]
            hmac_key=required(bytes),
            int_sync_rqe=RQEBaseURL(scheme="http", host="[::1]", port=9874),  # type: ignore  # 2024-01-24 # TODO: Unexpected keyword argument "scheme" for "RQEBaseURL"  [call-arg]
            int_async_rqe=RQEBaseURL(scheme="http", host="[::1]", port=9875),  # type: ignore  # 2024-01-24 # TODO: Unexpected keyword argument "scheme" for "RQEBaseURL"  [call-arg]
            ext_sync_rqe=RQEBaseURL(scheme="http", host="[::1]", port=9876),  # type: ignore  # 2024-01-24 # TODO: Unexpected keyword argument "scheme" for "RQEBaseURL"  [call-arg]
            ext_async_rqe=RQEBaseURL(scheme="http", host="[::1]", port=9877),  # type: ignore  # 2024-01-24 # TODO: Unexpected keyword argument "scheme" for "RQEBaseURL"  [call-arg]
        )

    def clone(self, **kwargs):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
        return attr.evolve(self, **kwargs)


def rqe_config_from_env(env: Optional[SDict] = None, add_dummy_hmac_if_missing: bool = False) -> RQEConfig:
    """
    :return: RQE config (default with overrides from environment variables)
    """
    hmac_env_key = "RQE_SECRET_KEY"
    actual_env: dict[str, str]

    if env is None:
        actual_env = dict(os.environ)
        # TODO FIX: BI-2497 remove this kludge when configs loading in tests import time will be eliminated (mostly uWSGI apps instantiation)
        if add_dummy_hmac_if_missing and hmac_env_key not in actual_env:
            actual_env[hmac_env_key] = ""
    else:
        actual_env = env  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Mapping[str, str]", variable has type "dict[str, str]")  [assignment]

    loader = EnvSettingsLoader(actual_env)
    return loader.load_settings(
        key_prefix="RQE",
        settings_type=RQEConfig,
        default_value=RQEConfig.get_default(),
    )
