"""
Dummy module that imitates pytest for the sake of importing test base classes
in an environment without `pytest` installed.
"""

from typing import Any, Callable, TypeVar


_RETURN_VALUE_TV = TypeVar('_RETURN_VALUE_TV')


# FIXME: switch to using ParamSpec when mypy starts supporting it
def fixture(**kwargs: Any) -> Callable[[Callable[..., _RETURN_VALUE_TV]], Callable[..., _RETURN_VALUE_TV]]:
    def decorator(func: Callable[..., _RETURN_VALUE_TV]) -> Callable[..., _RETURN_VALUE_TV]:
        return func

    return decorator
