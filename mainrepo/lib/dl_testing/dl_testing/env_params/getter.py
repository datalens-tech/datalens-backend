from __future__ import annotations

import abc
import json

import yaml


class EnvParamGetter(abc.ABC):
    @abc.abstractmethod
    def get_str_value(self, key: str) -> str:
        raise NotImplementedError

    def get_int_value(self, key: str) -> int:
        str_value = self.get_str_value(key)
        return int(str_value)

    def get_json_value(self, key: str) -> dict:
        str_value = self.get_str_value(key)
        return json.loads(str_value)

    def get_yaml_value(self, key: str) -> dict:
        str_value = self.get_str_value(key)
        return yaml.safe_load(str_value)

    def initialize(self, config: dict) -> None:
        pass
