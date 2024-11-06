from typing import (
    Any,
    Optional,
    Type,
)

import msgpack

from dl_model_tools.serialization import (
    COMMON_SERIALIZERS,
    NativeTypeSerializer,
    TypeSerializer,
    UnsupportedSerializer,
)
from dl_type_transformer.native_type import GenericNativeType


class DLMessagePackSerializer:
    JSONABLERS_MAP = {cls.typeobj(): cls for cls in COMMON_SERIALIZERS}
    DEJSONABLERS_MAP = {cls.typename: cls for cls in COMMON_SERIALIZERS}

    def _get_preprocessor(self, typeobj: type) -> Optional[Type[TypeSerializer]]:
        if issubclass(typeobj, GenericNativeType):
            return NativeTypeSerializer
        return self.JSONABLERS_MAP.get(typeobj)

    def _default(self, obj: Any) -> Any:
        typeobj = type(obj)
        preprocessor = self._get_preprocessor(typeobj)
        if preprocessor is not None:
            return dict(__dl_type__=preprocessor.typename, value=preprocessor.to_jsonable(obj))
        raise TypeError(f"Object of type {obj.__class__.__name__} is not MessagePack serializable")

    def dumps(self, value: Any) -> bytes:
        return msgpack.packb(value, default=self._default)

    def _object_hook(self, obj: dict[str, Any]) -> Any:
        # WARNING: this might collide with some unexpected objects that have a `__dl_type__` key.
        # A correct roundtrip way would be to wrap all objects with a `__dl_type__` key into another layer.
        dl_type = obj.get("__dl_type__")
        if dl_type is not None:
            postprocessor = self.DEJSONABLERS_MAP.get(dl_type)
            if postprocessor is not None:
                return postprocessor.from_jsonable(obj["value"])
        return obj

    def loads(self, value: bytes) -> Any:
        return msgpack.loads(value, object_hook=self._object_hook)


class DLSafeMessagePackSerializer(DLMessagePackSerializer):
    def _get_preprocessor(self, typeobj: type) -> Optional[Type[TypeSerializer]]:
        if (preprocessor := super()._get_preprocessor(typeobj)) is not None:
            return preprocessor
        return UnsupportedSerializer  # don't raise a TypeError and log warning
