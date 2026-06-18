from dl_cls.exc import CLSError
from dl_constants.exc import DLBaseError


def test_cls_error_is_dl_base_exception() -> None:
    assert issubclass(CLSError, DLBaseError)
