from __future__ import annotations

from dl_constants.exc import (
    GLOBAL_ERR_PREFIX,
    DLBaseException,
)
from dl_file_uploader_lib.enums import ErrorLevel


def make_err_code(exc: type[DLFileUploaderBaseError] | DLFileUploaderBaseError) -> str:
    return ".".join([GLOBAL_ERR_PREFIX] + exc.err_code)


class DLFileUploaderBaseError(DLBaseException):
    err_code = DLBaseException.err_code + ["FILE"]
    default_level = ErrorLevel.error


class CannotUpdateDataError(DLFileUploaderBaseError):
    err_code = DLFileUploaderBaseError.err_code + ["CANNOT_UPDATE_DATA"]
    default_message = "Source data cannot be updated, sources are most likely still being processed, try again later"


class FileLimitError(DLFileUploaderBaseError):
    err_code = DLFileUploaderBaseError.err_code + ["FILE_LIMIT_EXCEEDED"]
    default_message = "The file is too large"


class TooManyColumnsError(DLFileUploaderBaseError):
    err_code = DLFileUploaderBaseError.err_code + ["TOO_MANY_COLUMNS"]
    default_message = "Too many columns"


class TooFewRowsError(DLFileUploaderBaseError):
    err_code = DLFileUploaderBaseError.err_code + ["TOO_FEW_ROWS"]
    default_message = "Too few rows"


class InvalidFieldCast(DLFileUploaderBaseError):
    err_code = DLFileUploaderBaseError.err_code + ["INVALID_FIELD_CAST"]
    default_message = "Invalid field cast"


class PermissionDenied(DLFileUploaderBaseError):
    err_code = DLFileUploaderBaseError.err_code + ["PERMISSION_DENIED"]
    default_message = "The caller does not have permission"


class DocumentNotFound(DLFileUploaderBaseError):
    err_code = DLFileUploaderBaseError.err_code + ["NOT_FOUND"]
    default_message = "Requested document was not found"


class UnsupportedDocument(DLFileUploaderBaseError):
    err_code = DLFileUploaderBaseError.err_code + ["UNSUPPORTED_DOCUMENT"]
    default_message = "This document is not supported"


class InvalidLink(DLFileUploaderBaseError):
    err_code = DLFileUploaderBaseError.err_code + ["INVALID_LINK"]
    default_message = "Invalid format link provided"


class YaDocsInvalidLinkPrefix(InvalidLink):
    err_code = InvalidLink.err_code + ["YADOCS_INVALID_PUBLIC_LINK_PREFIX"]
    default_message = "Invalid url path prefix"


class EmptyDocument(DLFileUploaderBaseError):
    err_code = DLFileUploaderBaseError.err_code + ["NO_DATA"]
    default_message = "Empty document"


class DownloadFailed(DLFileUploaderBaseError):
    err_code = DLFileUploaderBaseError.err_code + ["DOWNLOAD_FAILED"]
    default_message = "Download failed"


class ParseFailed(DLFileUploaderBaseError):
    err_code = DLFileUploaderBaseError.err_code + ["PARSE_FAILED"]
    default_message = "Parsing failed"


class InvalidExcel(ParseFailed):
    err_code = ParseFailed.err_code + ["INVALID_EXCEL"]
    default_message = "Excel parsing failed"


class InvalidSource(DLFileUploaderBaseError):
    err_code = DLFileUploaderBaseError.err_code + ["INVALID_SOURCE"]
    default_message = "Specified source is not applicable"


class RemoteServerError(DLFileUploaderBaseError):
    err_code = DLFileUploaderBaseError.err_code + ["REMOTE_SERVER_ERROR"]
    default_message = "Remote server error"
    default_level = ErrorLevel.warning


class QuotaExceeded(DLFileUploaderBaseError):
    err_code = DLFileUploaderBaseError.err_code + ["QUOTA_EXCEEDED"]
    default_message = "Quota exceeded, try again later. If the issue persists, please contact support"
    default_level = ErrorLevel.warning


class QuotaExceededReadRequestsPerMinutePerProject(QuotaExceeded):
    err_code = QuotaExceeded.err_code + ["PER_MINUTE_PER_PROJECT"]
    default_message = (
        "Quota exceeded (read requests per minute per project), try again later. "
        "If the issue persists, please contact support"
    )


class QuotaExceededReadRequestsPerMinutePerUser(QuotaExceeded):
    err_code = QuotaExceeded.err_code + ["PER_MINUTE_PER_USER"]
    default_message = (
        "Quota exceeded (read requests per minute per user), try again later. "
        "If the issue persists, please contact support"
    )
