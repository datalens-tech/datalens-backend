from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Tuple,
)

WSGIEnviron = Dict[str, Any]
WSGIStartResponse = Callable[[str, List[Tuple[str, str]], Any], Any]
WSGIReturn = Iterable[bytes]

WSGICallable = Callable[[WSGIEnviron, WSGIStartResponse], WSGIReturn]
