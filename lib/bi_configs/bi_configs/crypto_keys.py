from __future__ import annotations

from os import environ
from typing import Dict, Optional, Mapping, Any

import attr
from cryptography import fernet

from bi_configs.settings_loaders.loader_env import EnvSettingsLoader
from bi_configs.settings_loaders.meta_definition import s_attrib
from bi_configs.settings_loaders.settings_obj_base import SettingsBase


@attr.s(frozen=True)
class CryptoKeysConfig(SettingsBase):
    map_id_key: Dict[str, str] = s_attrib("KEY_VAL_ID", sensitive=True)
    actual_key_id: str = s_attrib("ACTUAL_KEY_ID")

    @actual_key_id.default
    def _default_actual_key_id(self):
        if len(self.map_id_key) == 1:
            return next(iter(self.map_id_key.keys()))
        # TODO FIX: https://st.yandex-team.ru/BI-2497 Integrate with env loader exceptions handling
        raise ValueError("Missing actual_key_id (acceptable only in single key case)")

    def __attrs_post_init__(self):
        if self.actual_key_id not in self.map_id_key:
            raise ValueError()

    @classmethod
    def from_json(cls, json_value: Dict[str, Any]) -> CryptoKeysConfig:
        try:
            actual_key_id = json_value["actual_key_id"]
        except KeyError as exc:
            # TODO FIX: https://st.yandex-team.ru/BI-2497 Integrate with env loader exceptions handling
            raise ValueError(f"Crypto keys config missing key {exc!r}")
        try:
            map_id_key = json_value["keys"]
        except KeyError as exc:
            # TODO FIX: https://st.yandex-team.ru/BI-2497 Integrate with env loader exceptions handling
            raise ValueError(f"Crypto keys config missing key {exc!r}")
        return CryptoKeysConfig(
            actual_key_id=actual_key_id,
            map_id_key=map_id_key,
        )


def get_single_key_crypto_keys_config(*, key_id: str, key_value: str) -> CryptoKeysConfig:
    return CryptoKeysConfig(
        map_id_key={key_id: key_value},
        actual_key_id=key_id,
    )


def get_dummy_crypto_keys_config() -> CryptoKeysConfig:
    return CryptoKeysConfig(
        map_id_key={"dummy_key_id_qwerty123": fernet.Fernet.generate_key().decode()},
        actual_key_id="dummy_key_id_qwerty123",
    )


@attr.s()
class _CryptoKeysConfigOnlySettings(SettingsBase):
    """
    We can not load CryptoKeysConfig if it encoded in env as single JSON string.
    Loader can not handle this case due to root settings class should be env dict.
    So we make this wrapper.
    """
    cry: CryptoKeysConfig = s_attrib("DL_CRY", json_converter=CryptoKeysConfig.from_json, sensitive=True)


def get_crypto_keys_config_from_env(config_source: Optional[Mapping[str, str]] = None) -> CryptoKeysConfig:
    if config_source is None:
        config_source = environ

    loader = EnvSettingsLoader(config_source)
    return loader.load_settings(
        settings_type=_CryptoKeysConfigOnlySettings,
    ).cry