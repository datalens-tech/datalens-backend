import os

from bi_testing.env_params.getter import EnvParamGetter


class DirectEnvParamGetter(EnvParamGetter):
    """Returns the key as the value for all keys"""

    def get_str_value(self, key: str) -> str:
        return str(key)


class OsEnvParamGetter(EnvParamGetter):
    """Gets params from os environment"""

    def get_str_value(self, key: str) -> str:
        return os.environ.get(key)
