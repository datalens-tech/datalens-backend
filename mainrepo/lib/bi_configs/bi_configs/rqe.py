from __future__ import annotations

import os
from typing import Optional

import attr

from bi_configs.settings_loaders.common import SDict
from bi_configs.settings_loaders.loader_env import EnvSettingsLoader
from bi_configs.settings_loaders.meta_definition import (
    required,
    s_attrib,
)
from bi_configs.settings_loaders.settings_obj_base import SettingsBase
from bi_configs.utils import validate_one_of


@attr.s(frozen=True)
class RQEBaseURL(SettingsBase):
    scheme: str = s_attrib("SCHEME", env_var_converter=validate_one_of({"http", "https"}), missing="http")
    host: str = s_attrib("HOST")
    port: int = s_attrib("PORT")

    def __str__(self) -> str:
        return f"{self.scheme}://{self.host}:{self.port}"


@attr.s(frozen=True)
class RQEConfig(SettingsBase):
    ext_sync_rqe: RQEBaseURL = s_attrib("EXT_SYNC")
    ext_async_rqe: RQEBaseURL = s_attrib("EXT_ASYNC")
    int_sync_rqe: RQEBaseURL = s_attrib("INT_SYNC")
    int_async_rqe: RQEBaseURL = s_attrib("INT_ASYNC")
    hmac_key: bytes = s_attrib(
        "SECRET_KEY",
        sensitive=True,
        env_var_converter=lambda s: s.encode("ascii"),
    )

    @classmethod
    def get_default(cls) -> RQEConfig:
        return RQEConfig(
            hmac_key=required(bytes),
            int_sync_rqe=RQEBaseURL(scheme="http", host="[::1]", port=9874),
            int_async_rqe=RQEBaseURL(scheme="http", host="[::1]", port=9875),
            ext_sync_rqe=RQEBaseURL(scheme="http", host="[::1]", port=9876),
            ext_async_rqe=RQEBaseURL(scheme="http", host="[::1]", port=9877),
        )

    def clone(self, **kwargs):
        return attr.evolve(self, **kwargs)


def rqe_config_from_env(env: Optional[SDict] = None, add_dummy_hmac_if_missing: bool = False) -> RQEConfig:
    """
    :return: RQE config (default with overrides from environment variables)
    """
    hmac_env_key = "RQE_SECRET_KEY"
    actual_env: dict[str, str]

    if env is None:
        actual_env = dict(os.environ)
        # TODO FIX: BI-2497 remove this costyl when configs loading in tests import time will be eliminated (mostly uWSGI apps instantiation)
        if add_dummy_hmac_if_missing and hmac_env_key not in actual_env:
            actual_env[hmac_env_key] = ""
    else:
        actual_env = env

    loader = EnvSettingsLoader(actual_env)
    return loader.load_settings(
        key_prefix="RQE",
        settings_type=RQEConfig,
        default_value=RQEConfig.get_default(),
    )
