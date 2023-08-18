from bi_constants.exc import DLBaseException


class DLSBadRequest(DLBaseException):
    pass


class DLSSubjectNotFound(DLSBadRequest):
    pass


class DLSNotAvailable(Exception):
    pass  # TODO: decide what to do
