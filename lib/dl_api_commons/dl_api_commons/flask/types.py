from collections.abc import (
    Callable,
    Iterable,
)
from typing import Any

WSGIEnviron = dict[str, Any]
WSGIStartResponse = Callable[[str, list[tuple[str, str]], Any], Any]
WSGIReturn = Iterable[bytes]

WSGICallable = Callable[[WSGIEnviron, WSGIStartResponse], WSGIReturn]
