import abc
from typing import ClassVar

import attr

from dl_testing.env_params.getter import EnvParamGetter


class SnowFlakeSecretReaderBase(abc.ABC):
    KEY_CONFIG: ClassVar[str] = "SNOWFLAKE_CONFIG"
    KEY_CLIENT_SECRET: ClassVar[str] = "SNOWFLAKE_CLIENT_SECRET"
    KEY_REFRESH_TOKEN_EXPIRED: ClassVar[str] = "SNOWFLAKE_REFRESH_TOKEN_EXPIRED"
    KEY_REFRESH_TOKEN_X: ClassVar[str] = "SNOWFLAKE_REFRESH_TOKEN_X"

    @property
    @abc.abstractmethod
    def project_config(self) -> dict:
        raise NotImplementedError

    def get_account_name(self) -> str:
        return self.project_config["account_name"]

    def get_user_name(self) -> str:
        return self.project_config["user_name"]

    def get_user_role(self) -> str:
        return self.project_config["user_role"]

    def get_login_name(self) -> str:
        return self.project_config["login_name"]

    def get_database(self) -> str:
        return self.project_config["database"]

    def get_schema(self) -> str:
        return self.project_config["schema"]

    def get_table_name(self) -> str:
        return self.project_config["table_name"]

    def get_warehouse(self) -> str:
        return self.project_config["warehouse"]

    def get_client_id(self) -> str:
        return self.project_config["client_id"]

    @abc.abstractmethod
    def get_client_secret(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def get_refresh_token_expired(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def get_refresh_token_x(self) -> str:
        raise NotImplementedError


@attr.s
class SnowFlakeSecretReader(SnowFlakeSecretReaderBase):
    _env_param_getter: EnvParamGetter = attr.ib()
    _project_config: dict = attr.ib(init=False)

    @_project_config.default
    def _make_project_config(self) -> dict:
        return self._env_param_getter.get_json_value_strict(self.KEY_CONFIG)

    @property
    def project_config(self) -> dict:
        return self._project_config

    def get_client_secret(self) -> str:
        return self._env_param_getter.get_str_value_strict(self.KEY_CLIENT_SECRET)

    def get_refresh_token_expired(self) -> str:
        return self._env_param_getter.get_str_value_strict(self.KEY_REFRESH_TOKEN_EXPIRED)

    def get_refresh_token_x(self) -> str:
        return self._env_param_getter.get_str_value_strict(self.KEY_REFRESH_TOKEN_X)
