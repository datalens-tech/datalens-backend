from __future__ import annotations

import abc
import json
from typing import (
    NoReturn,
    Optional,
)

import yaml


def _raise_error_no_key(key: str) -> NoReturn:
    raise ValueError(f"Key {key!r} is missing")


class EnvParamGetter(abc.ABC):
    @abc.abstractmethod
    def get_str_value(self, key: str) -> Optional[str]:
        raise NotImplementedError

    def get_str_value_strict(self, key: str) -> str:
        str_value = self.get_str_value(key)
        if str_value is None:
            _raise_error_no_key(key)
        return str_value

    def get_int_value(self, key: str) -> Optional[int]:
        str_value = self.get_str_value(key)
        if str_value is not None:
            return int(str_value)
        return None

    def get_int_value_strict(self, key: str) -> int:
        int_value = self.get_int_value(key)
        if int_value is None:
            _raise_error_no_key(key)
        return int_value

    def get_json_value(self, key: str) -> Optional[dict]:
        str_value = self.get_str_value(key)
        if str_value is not None:
            return json.loads(str_value)
        return None

    def get_json_value_strict(self, key: str) -> dict:
        json_value = self.get_json_value(key)
        if json_value is None:
            _raise_error_no_key(key)
        return json_value

    def get_yaml_value(self, key: str) -> Optional[dict]:
        str_value = self.get_str_value(key)
        if str_value is not None:
            return yaml.safe_load(str_value)
        return None

    def get_yaml_value_strict(self, key: str) -> dict:
        yaml_value = self.get_yaml_value(key)
        if yaml_value is None:
            _raise_error_no_key(key)
        return yaml_value

    @abc.abstractmethod
    def initialize(self, config: dict) -> None:
        pass
