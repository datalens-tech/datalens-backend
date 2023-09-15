from typing import Optional

import attr

from bi_configs.enums import RedisMode
from bi_configs.settings_loaders.meta_definition import s_attrib
from bi_configs.settings_loaders.settings_obj_base import SettingsBase
from bi_configs.utils import split_by_comma


def redis_mode_env_var_converter(env_value: str) -> RedisMode:
    return {
        "sentinel": RedisMode.sentinel,
        "single_host": RedisMode.single_host,
    }[env_value.lower()]


@attr.s(frozen=True)
class RedisSettings(SettingsBase):
    MODE: RedisMode = s_attrib("MODE", env_var_converter=redis_mode_env_var_converter)
    CLUSTER_NAME: str = s_attrib("NAME", missing="")
    HOSTS: tuple[str, ...] = s_attrib("HOSTS", env_var_converter=split_by_comma)
    PORT: int = s_attrib("PORT")
    DB: int = s_attrib("DB")
    PASSWORD: str = s_attrib("PASSWORD", sensitive=True, missing=None)
    SSL: Optional[bool] = s_attrib("SSL", missing=None)


@attr.s(frozen=True)
class CorsSettings(SettingsBase):
    ALLOWED_ORIGINS: tuple[str, ...] = s_attrib("ALLOWED_ORIGINS", env_var_converter=split_by_comma)
    ALLOWED_HEADERS: tuple[str, ...] = s_attrib("ALLOWED_HEADERS", env_var_converter=split_by_comma)
    EXPOSE_HEADERS: tuple[str, ...] = s_attrib("EXPOSE_HEADERS", env_var_converter=split_by_comma)


@attr.s(frozen=True)
class CsrfSettings(SettingsBase):
    METHODS: tuple[str, ...] = s_attrib("METHODS", env_var_converter=split_by_comma)
    HEADER_NAME: str = s_attrib("HEADER_NAME")
    TIME_LIMIT: int = s_attrib("TIME_LIMIT")
    SECRET: str = s_attrib("SECRET", sensitive=True, missing=None)


@attr.s(frozen=True)
class S3Settings(SettingsBase):
    ACCESS_KEY_ID: str = s_attrib("ACCESS_KEY_ID", sensitive=True, missing=None)
    SECRET_ACCESS_KEY: str = s_attrib("SECRET_ACCESS_KEY", sensitive=True, missing=None)
    ENDPOINT_URL: str = s_attrib("ENDPOINT_URL")


@attr.s(frozen=True)
class GoogleAppSettings(SettingsBase):
    API_KEY: str = s_attrib("API_KEY", sensitive=True)
    CLIENT_ID: str = s_attrib("CLIENT_ID", sensitive=True)
    CLIENT_SECRET: str = s_attrib("CLIENT_SECRET", sensitive=True)
