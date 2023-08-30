from __future__ import annotations

import attr
import yaml

from bi_testing.env_params.getter import EnvParamGetter
from bi_testing.env_params.loader import EnvParamGetterLoader


@attr.s
class GenericEnvParamGetter(EnvParamGetter):
    _loader: EnvParamGetterLoader = attr.ib(init=False, factory=EnvParamGetterLoader)
    _key_mapping: dict[str, tuple[str, str]] = attr.ib(init=False, factory=dict)  # key -> (getter_name, remapped_key)

    def get_str_value(self, key: str) -> str:
        getter_name, remapped_key = self._key_mapping[key]
        getter = self._loader.get_getter(getter_name)
        value = getter.get_str_value(remapped_key)
        return value

    def initialize(self, config: dict) -> None:
        param_config = config['params']
        for key, key_config in param_config.items():
            self._key_mapping[key] = (key_config['getter'], key_config['key'])

        self._loader.initialize(config.get('getters', []), requirement_getter=self)

    @classmethod
    def from_yaml_file(cls, path: str) -> GenericEnvParamGetter:
        with open(path) as config_file:
            config_data = yaml.safe_load(config_file)

        getter = cls()
        getter.initialize(config_data)
        return getter
