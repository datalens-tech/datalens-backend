from __future__ import annotations

import enum
from typing import Any, Dict, Type, TYPE_CHECKING, Generic, TypeVar, ClassVar

from cryptography.fernet import Fernet
from marshmallow import Schema, pre_load, post_load, post_dump

if TYPE_CHECKING:
    from bi_core.us_manager.us_manager import USManagerBase  # noqa


class CtxKey(enum.Enum):
    """Schema context keys"""
    fernet = enum.auto()
    us_manager = enum.auto()
    dsc_id = enum.auto()
    ds_conn_type = enum.auto()


_TARGET_TV = TypeVar('_TARGET_TV')


class BaseStorageSchema(Schema, Generic[_TARGET_TV]):
    TARGET_CLS: ClassVar[Type[_TARGET_TV]]

    @classmethod
    def get_target_cls(cls) -> Type[_TARGET_TV]:
        return cls.TARGET_CLS

    @property
    def fernet(self) -> Fernet:
        return self.context[CtxKey.fernet]

    @property
    def usm(self) -> 'USManagerBase':
        return self.context[CtxKey.us_manager]

    # Lifecycle
    def pre_process_input_data(self, data: Dict[str, Any]) -> Dict[str, Any]:  # noqa
        return data

    def post_process_output_data(self, data: Dict[str, Any]) -> Dict[str, Any]:  # noqa
        return data

    def push_ctx(self, data: dict) -> None:
        """Used for pushing data upper-layer data to nested schemas. Note that data is raw dict after pre-processing"""
        pass

    def pop_ctx(self, data: dict) -> None:
        """Cleanup con"""
        pass

    def to_object(self, data: dict) -> _TARGET_TV:
        raise NotImplementedError("This schema is does not implement object deserialization")

    @pre_load(pass_many=False)
    def pre_load(self, data, **_):  # type: ignore  # TODO: fix
        normalized_data = self.pre_process_input_data(data)
        # storing unknown fields
        unknown_keys = normalized_data.keys() - self.fields.keys()
        # TODO FIX: Store unknown fields somewhere
        # unknown = {key: normalized_data.pop(key) for key in unknown_keys}
        for key in unknown_keys:
            normalized_data.pop(key)

        self.push_ctx(normalized_data)
        return normalized_data

    @post_load(pass_many=False)
    def post_load(self, data, **_):  # type: ignore  # TODO: fix
        obj = self.to_object(data)
        self.pop_ctx(data)
        return obj

    @post_dump(pass_many=False)
    def post_dump(self, data: Dict[str, Any], **_) -> Dict[str, Any]:  # type: ignore  # TODO: fix
        normalized_data = self.post_process_output_data(data)
        return normalized_data


class DefaultStorageSchema(BaseStorageSchema[_TARGET_TV], Generic[_TARGET_TV]):
    def to_object(self, data: dict) -> _TARGET_TV:
        return self.get_target_cls()(**data)  # type: ignore


# class CustomNested(fields.Nested):
#     """Nested Field with pre/post processing of value"""
#
#     def __init__(self, nested, pre_dump: str, post_load: str, **kwargs):
#         super().__init__(nested, **kwargs)
#         self._pre_dump_meth_name = pre_dump
#         self._post_load_meth_name = post_load
#
#     def _deserialize(self, value, attr, data, partial=None, **kwargs):
#         loaded_data = super()._deserialize(value, attr, data, partial=partial, **kwargs)
#         return getattr(self.parent, self._post_load_meth_name)(loaded_data)
#
#     def _serialize(self, nested_obj, attr, obj, **kwargs):
#         pre_processed_data = getattr(self.parent, self._pre_dump_meth_name)(nested_obj)
#         return super()._serialize(pre_processed_data, attr, obj, **kwargs)
