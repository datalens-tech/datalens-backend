import os
from typing import Optional

import attr
from dotenv import (
    dotenv_values,
    find_dotenv,
)

from dl_testing.env_params.getter import EnvParamGetter


class DirectEnvParamGetter(EnvParamGetter):
    """Returns the key as the value for all keys"""

    def get_str_value(self, key: str) -> str:
        return str(key)

    def initialize(self, config: dict) -> None:
        pass


@attr.s
class OsEnvParamGetter(EnvParamGetter):
    """Gets params from os environment, with an env-file fallback"""

    _env_from_file: dict = attr.ib(init=False, factory=dict)

    def initialize(self, config: dict) -> None:
        env_file = os.environ.get("DL_TESTS_ENV_FILE") or find_dotenv(filename=".env")
        self._env_from_file = dotenv_values(env_file)

    def get_str_value(self, key: str) -> Optional[str]:
        env_value = os.environ.get(key)

        if env_value is None:
            return self._env_from_file.get(key)

        return env_value
