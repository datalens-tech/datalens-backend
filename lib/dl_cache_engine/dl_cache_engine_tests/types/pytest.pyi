"""Type stubs for pytest"""

from typing import (
    Any,
    Callable,
    Optional,
    Type,
    TypeVar,
    Union,
)

_T = TypeVar("_T")

class Config:
    def __init__(self) -> None: ...

def fixture(scope: str = "function", autouse: bool = False) -> Callable[[_T], _T]: ...

class mark:
    @staticmethod
    def asyncio(f: _T) -> _T: ...
    @staticmethod
    def parametrize(*args: Any, **kwargs: Any) -> Callable[[_T], _T]: ...

def raises(
    expected_exception: Union[Type[BaseException], tuple[Type[BaseException], ...]], match: Optional[str] = None
) -> Any: ...
def configure(config: Config) -> None: ...
