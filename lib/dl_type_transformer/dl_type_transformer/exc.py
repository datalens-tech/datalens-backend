from dl_constants.exc import DLBaseError


class TypeCastError(DLBaseError):
    err_code = (*DLBaseError.err_code, "TYPE_CAST")


class TypeCastUnsupportedError(TypeCastError):
    err_code = (*TypeCastError.err_code, "UNSUPPORTED")


class TypeCastFailedError(TypeCastError):
    err_code = (*TypeCastError.err_code, "FAILED")
    default_message = "Type casting failed for value"


class UnsupportedNativeTypeError(Exception):
    pass


class TypeTransformerNotFoundError(Exception):
    pass
