from __future__ import annotations

from enum import Enum, unique
from typing import Union, Literal, Any, final, Optional

import attr

from dynamic_enum import DynamicEnum


Align = Literal['start', 'center', 'end']
WidthVariant = Literal['s', 'm', 'l', 'auto']
Width = Union[WidthVariant, int, float]


class MarkdownString(str):
    ...


MarkdownStr = Union[MarkdownString, str]  # marker type to show if a field supports markdown or not


@unique
class FormFieldName(Enum):
    """
    Field key in the API schema
    Enum to eliminate typos
    Left empty, so it can be extended with connector specific fields
    """


@unique
class CommonFieldName(FormFieldName):
    host = 'host'
    port = 'port'
    username = 'username'
    password = 'password'
    db_name = 'db_name'
    raw_sql_level = 'raw_sql_level'
    cache_ttl_sec = 'cache_ttl_sec'
    secure = 'secure'
    mdb_cluster_id = 'mdb_cluster_id'
    mdb_folder_id = 'mdb_folder_id'
    folder_id = 'folder_id'
    access_token = 'access_token'
    token = 'token'
    is_auto_create_dashboard = 'is_auto_create_dashboard'
    service_account_id = 'service_account_id'
    advanced_settings = 'advanced_settings'
    ssl_enable = 'ssl_enable'
    data_export_forbidden = 'data_export_forbidden'
    ssl_ca = 'ssl_ca'


@unique
class BooleanField(Enum):
    on = 'on'
    off = 'off'


@unique
class TopLevelFieldName(Enum):
    """ Fields that are sent into the API but do not represent data filled by the user in the main form """

    type_ = 'type'


@unique
class InnerFieldName(Enum):
    """
    Prepared rows/components can have inner fields that can' be controlled by the config,
    but can be used in conditions

    Each row/component can have its own set of inner fields,
    but their names can't intersect with each other or `FormFieldName`s
    """


TFieldName = Union[FormFieldName, TopLevelFieldName, InnerFieldName]


@unique
class OAuthApplication(Enum):
    """ Identifier of the OAuth application that should be used by the frontend """


@attr.s(kw_only=True, frozen=True)
class CFGMeta:
    METADATA_KEY = '_SERIALIZABLE_CONFIG_META_KEY_'

    inner: bool = attr.ib(
        default=False)  # whether it is a service field involved only in inner logic and which needs to be skipped
    skip_if_null: bool = attr.ib(default=False)  # sometimes it is more convenient for the UI to receive undefined
    key: Optional[str] = attr.ib(default=None)  # remap key

    def attr_meta(self) -> dict[str, CFGMeta]:
        return {self.METADATA_KEY: self}


@attr.s
class SerializableConfig:
    """
    Base class for attrs decorated configs, that prepares values for serialization.
    Include ClassVars and other non-attr fields by overriding as_dict method.
    Set attr field's metadata to `CFGMeta().attr_meta` to change the way it is processed.
    Has to be a parent class for all top-level config classes.
    """

    _SKIP_SENTINEL = object
    _remap_keys_buffer: dict[str | TFieldName, str] = attr.ib(
        init=False, factory=dict, metadata=CFGMeta(inner=True).attr_meta())

    @staticmethod
    @final
    def prepare_value(value: Any) -> Any:
        if isinstance(value, (Enum, DynamicEnum)):
            return value.value
        if isinstance(value, SerializableConfig):
            return value.as_dict()
        if attr.has(value.__class__):
            return attr.asdict(value, value_serializer=SerializableConfig._attr_value_serializer, recurse=False)  # type: ignore
        if isinstance(value, (list, tuple)):
            return [SerializableConfig.prepare_value(item) for item in value]
        if isinstance(value, dict):
            return {
                SerializableConfig.prepare_value(k): SerializableConfig.prepare_value(v)
                for k, v in value.items()
            }
        return value

    @staticmethod
    @final
    def _attr_value_serializer(inst: type, field: attr.Attribute, value: Any) -> Any:
        cls = SerializableConfig
        if CFGMeta.METADATA_KEY in field.metadata:
            if not isinstance(inst, cls):
                raise ValueError(
                    f'Cannot use serialization meta for an object of type {type(inst).__name__},'
                    f' add {cls.__name__} as a parent class to enable this.'
                )
            meta = field.metadata[CFGMeta.METADATA_KEY]
            assert isinstance(meta, CFGMeta), f'Unexpected meta class, {CFGMeta.__name__} expected'
            if meta.inner or meta.skip_if_null and value is None:
                return cls._SKIP_SENTINEL
            if meta.key is not None:
                inst._remap_keys_buffer[field.name] = meta.key

        return cls.prepare_value(value)

    def as_dict(self) -> dict[str, Any]:
        result = attr.asdict(self, value_serializer=self._attr_value_serializer, recurse=False)  # type: ignore
        return {
            k.value if isinstance(k, (Enum, DynamicEnum)) else self._remap_keys_buffer.get(k, k): v
            for k, v in result.items()
            if v is not self._SKIP_SENTINEL
        }


# frequently used meta shortcuts
def skip_if_null() -> dict[str, CFGMeta]:
    return CFGMeta(skip_if_null=True).attr_meta()


def remap(key: str) -> dict[str, CFGMeta]:
    return CFGMeta(key=key).attr_meta()


def remap_skip_if_null(key: str) -> dict[str, CFGMeta]:
    return CFGMeta(skip_if_null=True, key=key).attr_meta()


def inner() -> dict[str, CFGMeta]:
    return CFGMeta(inner=True).attr_meta()
