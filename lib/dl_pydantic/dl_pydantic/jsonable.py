import abc
import datetime
from typing import (
    Any,
    Generic,
    TypeVar,
)
import uuid

import pydantic
import pydantic_core
from typing_extensions import Self


JsonableTimedelta = datetime.timedelta
JsonableUUID = uuid.UUID


T = TypeVar("T")


class StringJsonableTypeMixin(Generic[T]):
    original_type: type[T]

    @classmethod
    @abc.abstractmethod
    def from_string(cls, value: str) -> Self:
        ...

    @abc.abstractmethod
    def to_string(self) -> str:
        ...

    @classmethod
    @abc.abstractmethod
    def from_original(cls, value: Any) -> Self:
        ...

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        _source_type: Any,
        _handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.core_schema.CoreSchema:
        def _serialize(instance: Any, info: Any) -> Any:
            if info.mode == "json":
                return instance.to_string()
            return instance

        from_str_schema = pydantic_core.core_schema.chain_schema(
            [
                pydantic_core.core_schema.str_schema(),
                pydantic_core.core_schema.no_info_plain_validator_function(cls.from_string),
            ]
        )
        from_original_schema = pydantic_core.core_schema.chain_schema(
            [
                pydantic_core.core_schema.is_instance_schema(cls.original_type),
                pydantic_core.core_schema.no_info_plain_validator_function(cls.from_original),
            ]
        )

        return pydantic_core.core_schema.json_or_python_schema(
            json_schema=from_str_schema,
            python_schema=pydantic_core.core_schema.union_schema(
                [
                    from_str_schema,
                    from_original_schema,
                    pydantic_core.core_schema.is_instance_schema(cls),
                ]
            ),
            serialization=pydantic_core.core_schema.plain_serializer_function_ser_schema(_serialize, info_arg=True),
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls,
        _core_schema: pydantic_core.core_schema.CoreSchema,
        handler: pydantic.GetJsonSchemaHandler,
    ) -> pydantic.json_schema.JsonSchemaValue:
        return handler(pydantic_core.core_schema.str_schema())


class JsonableDate(datetime.date, StringJsonableTypeMixin):
    original_type = datetime.date

    @classmethod
    def from_string(cls, value: str) -> Self:
        return cls.fromisoformat(value)

    def to_string(self) -> str:
        return self.isoformat()

    @classmethod
    def from_original(cls, value: datetime.date) -> Self:
        return cls.fromisoformat(value.isoformat())


class JsonableDatetime(datetime.datetime, StringJsonableTypeMixin):
    original_type = datetime.datetime

    @classmethod
    def from_string(cls, value: str) -> Self:
        if value.endswith("Z"):
            return cls.fromisoformat(value[:-1] + "+00:00")

        return cls.fromisoformat(value)

    def to_string(self) -> str:
        if self.tzinfo is None:
            return self.strftime("%Y-%m-%dT%H:%M:%S.%f")

        if self.tzinfo == datetime.timezone.utc:
            return self.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        return self.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    @classmethod
    def from_original(cls, value: datetime.datetime) -> Self:
        return cls.fromisoformat(value.isoformat())


class JsonableDatetimeWithTimeZone(JsonableDatetime):
    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        result = super().__new__(cls, *args, **kwargs)

        if result.tzinfo is None:
            raise ValueError("tzinfo is required")

        return result
