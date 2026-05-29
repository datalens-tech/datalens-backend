from collections.abc import (
    Callable,
    Mapping,
)
from typing import Any

SDict = Mapping[str, str]
FallbackFactory = Callable[[Any, Any], Any] | Callable[[Any], Any]
