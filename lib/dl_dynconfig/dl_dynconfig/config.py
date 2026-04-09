import logging
from typing import (
    Any,
    Mapping,
)

import pydantic
from typing_extensions import Self

import dl_dynconfig.sources.base as base
import dl_pydantic


LOGGER = logging.getLogger(__name__)


class DynConfigError(Exception):
    pass


class SourceNotSetError(DynConfigError):
    pass


class FetchError(DynConfigError):
    pass


class DynConfig(dl_pydantic.BaseModel):
    model_config = pydantic.ConfigDict(validate_assignment=True)

    _source: base.BaseSource | None = pydantic.PrivateAttr(default=None)
    _path: list[str] = pydantic.PrivateAttr(default_factory=list)

    @classmethod
    def model_from_source(
        cls,
        source: base.BaseSource,
        path: list[str] | None = None,
        initial_data: Any = None,
    ) -> Self:
        initial_data = initial_data or {}
        if not isinstance(initial_data, Mapping):
            raise pydantic.ValidationError(f"initial_data must be a mapping, got {type(initial_data).__name__}", [])

        instance = cls.model_validate(initial_data)
        instance._source = source
        instance._path = path or []

        for field_name, field_info in cls.model_fields.items():
            annotation = field_info.annotation
            if annotation is not None and issubclass(annotation, DynConfig):
                child = annotation.model_from_source(
                    source=source,
                    path=instance._path + [field_name],
                    initial_data=initial_data.get(field_name, {}),
                )
                setattr(instance, field_name, child)

        return instance

    async def model_store(self) -> None:
        if self._source is None:
            raise SourceNotSetError("Must be created via model_from_source()")

        data = await self._source.fetch()

        if self._path:
            node = data
            for key in self._path[:-1]:
                node = node[key]
            node[self._path[-1]] = self.model_dump()
        else:
            data = self.model_dump()

        await self._source.store(data)

    async def model_fetch(self, force: bool = False) -> None:
        if self._source is None:
            raise SourceNotSetError("Must be created via model_from_source()")

        try:
            data = await self._source.fetch()
        except Exception as e:
            if force:
                raise FetchError("Failed to fetch data from source") from e
            else:
                LOGGER.exception("Failed to fetch data from source")
                return

        node = data
        for key in self._path:
            if not isinstance(node, Mapping):
                raise pydantic.ValidationError(f"Expected a mapping at path {self._path + [key]}", [])
            if key not in node:
                node = {}
                break
            node = node[key]

        self.model_populate(node)

    def model_populate(self, data: Any) -> None:
        if not isinstance(data, Mapping):
            raise pydantic.ValidationError(f"data must be a mapping, got {type(data).__name__}", [])

        for field_name, field_info in type(self).model_fields.items():
            annotation = field_info.annotation
            if annotation is not None and issubclass(annotation, DynConfig):
                if field_name in data:
                    getattr(self, field_name).model_populate(data[field_name])
            elif field_name in data:
                setattr(self, field_name, data[field_name])
            elif not field_info.is_required():
                setattr(self, field_name, field_info.get_default(call_default_factory=True))
            else:
                raise pydantic.ValidationError(f"field '{field_name}' is required for {type(self).__name__}", [])
