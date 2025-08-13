from typing import (
    Any,
    Callable,
    Iterable,
)


WSGIEnviron = dict[str, Any]
WSGIStartResponse = Callable[[str, list[tuple[str, str]], Any], Any]
WSGIReturn = Iterable[bytes]

WSGICallable = Callable[[WSGIEnviron, WSGIStartResponse], WSGIReturn]
