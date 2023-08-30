from typing import Callable, Tuple, List, Any, Dict, Iterable

WSGIEnviron = Dict[str, Any]
WSGIStartResponse = Callable[[str, List[Tuple[str, str]], Any], Any]
WSGIReturn = Iterable[bytes]

WSGICallable = Callable[[WSGIEnviron, WSGIStartResponse], WSGIReturn]
