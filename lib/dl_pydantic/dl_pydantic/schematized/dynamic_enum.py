from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    TypeVar,
)

import pydantic
import pydantic_core

from dl_dynamic_enum import DynamicEnum
import dl_pydantic.schematized.base as base


_DYNAMIC_ENUM_TV = TypeVar("_DYNAMIC_ENUM_TV", bound=DynamicEnum)


class _DynamicEnumPydanticAnnotation:
    def __init__(self, enum_cls: type[DynamicEnum]) -> None:
        self._enum_cls = enum_cls

    def __get_pydantic_core_schema__(
        self,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.core_schema.CoreSchema:
        enum_cls = self._enum_cls

        def _validate(value: Any) -> Any:
            if isinstance(value, enum_cls):
                return value
            if isinstance(value, str):
                return enum_cls(value)
            raise ValueError(f"Expected str or {enum_cls.__name__}, got {type(value).__name__}")

        def _serialize(value: Any, info: Any) -> Any:
            if info.mode == "json":
                return value.value
            return value

        from_str_schema = pydantic_core.core_schema.chain_schema(
            [
                pydantic_core.core_schema.str_schema(),
                pydantic_core.core_schema.no_info_plain_validator_function(_validate),
            ]
        )

        return pydantic_core.core_schema.json_or_python_schema(
            json_schema=from_str_schema,
            python_schema=pydantic_core.core_schema.no_info_plain_validator_function(_validate),
            serialization=pydantic_core.core_schema.plain_serializer_function_ser_schema(_serialize, info_arg=True),
        )


if TYPE_CHECKING:
    SchematizedDynamicEnumAnnotation = Annotated[_DYNAMIC_ENUM_TV, ...]
else:

    class SchematizedDynamicEnumAnnotation:
        def __class_getitem__(cls, enum_cls: type[_DYNAMIC_ENUM_TV]) -> Any:
            return Annotated[enum_cls, _DynamicEnumPydanticAnnotation(enum_cls)]

    base.register_schematized_annotation(DynamicEnum, SchematizedDynamicEnumAnnotation)

__all__ = [
    "SchematizedDynamicEnumAnnotation",
]
