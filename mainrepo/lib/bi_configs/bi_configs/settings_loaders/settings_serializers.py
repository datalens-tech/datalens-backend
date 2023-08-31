from __future__ import annotations

import inspect
import enum
from collections import defaultdict
from typing import Type

import attr
import yaml

from dynamic_enum import DynamicEnum
from bi_configs.connectors_data import ConnectorsDataBase
from bi_configs.environments import (
    CommonInstallation,
)


def object_to_dict(obj: object) -> dict:
    def _transform(obj: object) -> object:
        if isinstance(obj, enum.Enum):
            return obj.value
        if isinstance(obj, tuple):
            return [_transform(v) for v in obj]
        if isinstance(obj, list):
            return [_transform(v) for v in obj]
        return obj

    config = {
        k: _transform(v) if not hasattr(v, '__dict__') or isinstance(v, enum.Enum) else object_to_dict(v.__class__)
        for k, v in inspect.getmembers(obj)
        if not k.startswith('_')
    }
    return config


def defaults_to_dict(default_settings: CommonInstallation) -> dict:
    def _uniq_connector_settings(conn_classes: list[Type[ConnectorsDataBase]]) -> dict[str, Type[ConnectorsDataBase]]:
        conn_cls_by_type = defaultdict(list)
        result: dict[str, Type[ConnectorsDataBase]] = {}
        for cls in conn_classes:
            conn_cls_by_type[cls.connector_name()].append(cls)
        for name, classes in conn_cls_by_type.items():
            if len(classes) == 1:
                result[name] = classes[0]
                continue
            class_for_env = [
                cls
                for cls in classes
                if cls.__name__.endswith('Production') or cls.__name__.endswith('Testing')
            ]
            if len(class_for_env) == 1:
                result[name] = class_for_env[0]
                continue
            elif len(class_for_env) > 1:
                raise Exception('Too many envs')
            class_for_installation = [
                cls
                for cls in classes
                if cls.__name__.endswith('Installation')
            ]
            if len(class_for_installation) == 1:
                result[name] = class_for_installation[0]
                continue
            elif len(class_for_installation) > 1:
                raise Exception('Too many installations')
            raise Exception('Can\'t choose the right class')
        return result

    config = object_to_dict(default_settings)

    if hasattr(default_settings, 'CONNECTOR_AVAILABILITY'):
        config['CONNECTOR_AVAILABILITY'] = attr.asdict(
            default_settings.CONNECTOR_AVAILABILITY,
            value_serializer=lambda _, __, v: v.value if isinstance(v, (enum.Enum, DynamicEnum)) else v
        )

    conn_classes = [
        cls for cls in inspect.getmro(default_settings.__class__)
        if (
            cls.__name__.startswith('ConnectorsData')
            and issubclass(cls, ConnectorsDataBase)
            and cls.__name__ != 'ConnectorsDataBase'
        )
    ]
    connectors_data = {}
    all_connectors_keys = set()
    for name, cls in _uniq_connector_settings(conn_classes=conn_classes).items():
        connectors_data[name] = object_to_dict(cls)
        for key in connectors_data[name].keys():
            all_connectors_keys.add(key)
    for conn_key in all_connectors_keys:
        del config[conn_key]
    config['CONNECTORS_DATA'] = connectors_data
    return config


def defaults_to_yaml(defaults: CommonInstallation) -> str:
    config = defaults_to_dict(defaults)
    return yaml.safe_dump(config)
