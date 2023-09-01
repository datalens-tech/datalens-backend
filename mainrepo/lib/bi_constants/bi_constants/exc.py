from typing import Any, ClassVar, Optional, Type
from typing_extensions import final

from bi_constants.types import TJSONLike


GLOBAL_ERR_PREFIX = 'ERR'
DEFAULT_ERR_CODE_API_PREFIX = 'DS_API'
CODE_OK = 'OK'


# TODO FIX: Simplify exc data structures to simplify serialization
#  Make only 2 arguments in constructor: data & orig
class DLBaseException(Exception):
    # code parts joining by dots on api response preparing.
    # err_code values should be described at https://wiki.yandex-team.ru/datalens/errors/
    err_code: ClassVar[list[str]] = []  # TODO: Implement automatic hierarchial code inheritance
    _message: str

    default_message = 'Internal Server Error'
    formatting_messages: Optional[dict[frozenset[str], str]] = None

    # Auxiliary error info. Will not be shown to user (or will be shown as is only in intranet)
    debug_info: dict[str, Any]

    # Dictionary with some fixed structure per each err_code.
    # Can be used by front to show user some additional info.
    details: dict[str, Any]

    params: dict[str, Any]

    def __init__(
            self,
            message: Optional[str] = None,
            details: Optional[dict[str, Any]] = None,
            orig: Optional[Exception] = None,
            debug_info: Optional[dict[str, Any]] = None,
            params: Optional[dict[str, Any]] = None,
    ):
        self._message = message or self.default_message
        self.details = details or {}
        self.debug_info = debug_info or {}
        self.orig = orig or None
        self.params = params or {}

    @property
    def message(self) -> str:
        """
        Message will be shown directly to user.
        """
        if self.params and self.formatting_messages and frozenset(self.params.keys()) in self.formatting_messages.keys():
            return self.formatting_messages[frozenset(self.params.keys())].format(**self.params)
        else:
            return self._message

    def __str__(self) -> str:
        return self.message or super().__str__()

    # ##
    # Serialization logic for passing from QE
    # ##
    _MAP_CLASS_NAME_CLASS: ClassVar[dict[str, Type['DLBaseException']]] = {}

    def __init_subclass__(cls, **kwargs):  # type: ignore
        cls._MAP_CLASS_NAME_CLASS[cls.__qualname__] = cls

    @classmethod
    @final
    def from_jsonable_dict(cls, data: dict) -> 'DLBaseException':
        data = {**data}
        cls_name = data.pop('cls_name')
        target_cls = cls._MAP_CLASS_NAME_CLASS[cls_name]
        # noinspection PyProtectedMember
        return target_cls._from_jsonable_dict(data)

    @classmethod
    def _from_jsonable_dict(cls, data: dict) -> 'DLBaseException':
        new_exc = cls(message=data.pop('message', None))
        new_exc.details = data.pop('details')
        new_exc.debug_info = data.pop('debug_info')
        new_exc.params = data.pop('params', {})
        return new_exc

    def to_jsonable_dict(self) -> dict[str, TJSONLike]:
        return dict(
            cls_name=type(self).__qualname__,
            message=None if self._message == self.default_message else self._message,
            details=self.details,
            debug_info=self.debug_info,
            params=self.params,
        )
