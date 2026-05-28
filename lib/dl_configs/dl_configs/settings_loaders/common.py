from typing import (
    Any,
    Callable,
    Mapping,
)

SDict = Mapping[str, str]
FallbackFactory = Callable[[Any, Any], Any] | Callable[[Any], Any]
