import logging
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    TypeVar,
)


_T = TypeVar("_T")
LOGGER = logging.getLogger(__name__)

_REGISTRY: list[tuple[type, Any]] = []


def register_schematized_annotation(base_cls: type, schematized_cls: type) -> None:
    LOGGER.debug("Registering schematized annotation for %s: %s", base_cls.__name__, schematized_cls.__name__)
    _REGISTRY.append((base_cls, schematized_cls))


if TYPE_CHECKING:
    SchematizedAnnotation = Annotated[_T, ...]
else:

    class SchematizedAnnotation:
        def __class_getitem__(cls, target_cls: type) -> Any:
            for base_cls, schematized_cls in _REGISTRY:
                if issubclass(target_cls, base_cls):
                    return schematized_cls[target_cls]

            raise TypeError(
                f"No schematized annotation registered for {target_cls!r}. "
                f"Supported base types: {[base.__name__ for base, _ in _REGISTRY]}"
            )


__all__ = [
    "SchematizedAnnotation",
]
