# TODO: Rename to env_tools.py ?
"""
Usage example:

    ipython -i -c "\
        import bi_api_lib.maintenance.tooling as mt, bi_core.maintenance.crawlers.dumper as cd, bi_api_lib.maintenance.crawler_runner as cr; \
        mt.update_env('int_testing'); \
        crawler = cd.Dumper(); \
        cr.run_crawler(crawler); \
    "
"""

from __future__ import annotations

import os
from typing import Dict, Any

try:
    from library.python.vault_client.instances import Production as VaultClient  # noqa
except ImportError:
    from vault_client.instances import Production as VaultClient  # type: ignore  # TODO: fix  # noqa

from bi_configs.environments import EnvironmentsSettingsMap
from bi_configs.crypto_keys import get_crypto_keys_from_vault_data


class SecretsCache:

    _vault_client: VaultClient = None  # type: ignore  # TODO: fix

    def __init__(self) -> None:
        # sec_id -> get_version response
        self._cache: Dict[str, Dict[str, Any]] = {}

    def _make_vault_client(self) -> VaultClient:  # type: ignore  # TODO: fix
        username = os.environ.get("YAV_USER")
        return VaultClient(
            rsa_login=username,
        )

    @property
    def vault_client(self) -> VaultClient:  # type: ignore  # TODO: fix
        vc = self._vault_client
        if vc is None:
            vc = self._make_vault_client()
            self._vault_client = vc
        return vc

    def get_sec_data_nc(self, sec_id: str) -> Dict[str, Any]:
        return self.vault_client.get_version(sec_id)  # type: ignore  # TODO: fix

    def get_sec_data(self, sec_id: str) -> Dict[str, Any]:
        sec_data = self._cache.get(sec_id)
        if sec_data is None:
            sec_data = self.get_sec_data_nc(sec_id)
            self._cache[sec_id] = sec_data
        return sec_data

    def get_value(self, sec_id: str, sec_key: str) -> str:
        sec_data = self.get_sec_data(sec_id)
        return sec_data['value'][sec_key]


class SecretsCacheSingleton:
    secrets_cache = None

    @classmethod
    def get(cls) -> SecretsCache:
        secrets_cache = cls.secrets_cache
        if secrets_cache is not None:
            return secrets_cache
        secrets_cache = SecretsCache()
        cls.secrets_cache = secrets_cache
        return secrets_cache


def prepare_env(yenv_type: str) -> dict:
    secrets_cache = SecretsCacheSingleton.get()

    # env_name_norm = getattr(EnvAliasesMap, env_name)  # not relevant for the settings
    yenv_type = yenv_type.replace('-', '_')  # minor convenience / env-var match.

    env_settings = getattr(EnvironmentsSettingsMap, yenv_type)
    straight_keys = (
        'YENV_TYPE', 'YENV_NAME',
    )
    osenv_map = dict(
        # os.environ name -> source settings key
        US_MASTER_TOKEN='US_MASTER_SECRET_YAV',
        CACHES_REDIS_PASSWORD='REDIS_CACHES_PASSWORD_YAV',
        **{key: key for key in straight_keys},
    )
    yav_suffix = '_YAV'

    osenv_data = dict()

    crypto_sec_data = secrets_cache.get_sec_data(env_settings.ENV_VAULT_ID)
    crypto_sec_value = crypto_sec_data['value']
    crypto_sec_map = get_crypto_keys_from_vault_data(crypto_sec_value)
    crypto_sec_map = dict(crypto_sec_map)
    for key, mapped_key in env_settings.DL_CRY_KEYMAP.items():
        value = crypto_sec_map.pop(key, None)
        if value is not None:
            crypto_sec_map[mapped_key] = value
    osenv_data.update({
        f'DL_CRY_KEY_VAL_ID_{key}': value
        for key, value in crypto_sec_map.items()
    })
    osenv_data.update(DL_CRY_ACTUAL_KEY_ID=env_settings.DL_CRY_ACTUAL_KEY_ID)

    for osenv_key, env_key in osenv_map.items():
        env_value = getattr(env_settings, env_key)
        assert env_key.endswith(yav_suffix) == isinstance(env_value, tuple)
        if env_key.endswith(yav_suffix):
            env_value = secrets_cache.get_value(*env_value)
            assert isinstance(env_value, str)
        osenv_data[osenv_key] = env_value

    osenv_data['RQE_SECRET_KEY'] = 'xxx'  # required, but not expected to be important.

    return osenv_data


def update_env(yenv_type: str) -> None:
    os.environ.update(prepare_env(yenv_type))
