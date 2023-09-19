from typing import (
    Any,
    Callable,
    Mapping,
    Union,
)


SDict = Mapping[str, str]
FallbackFactory = Union[
    Callable[[Any, Any], Any],  # fallback_cfg & app_cfg_type
    Callable[[Any], Any],  # fallback_cfg only
]
