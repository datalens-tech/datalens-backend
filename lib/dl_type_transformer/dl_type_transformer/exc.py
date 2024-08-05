from dl_constants.exc import DLBaseException


class TypeCastError(DLBaseException):
    err_code = DLBaseException.err_code + ["TYPE_CAST"]


class TypeCastUnsupported(TypeCastError):
    err_code = TypeCastError.err_code + ["UNSUPPORTED"]


class TypeCastFailed(TypeCastError):
    err_code = TypeCastError.err_code + ["FAILED"]
    default_message = "Type casting failed for value"


class UnsupportedNativeTypeError(Exception):
    pass


class TypeTransformerNotFound(Exception):
    pass
