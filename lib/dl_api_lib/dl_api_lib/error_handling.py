from __future__ import annotations

import http
import inspect
import itertools
import logging
from typing import (
    Any,
    ClassVar,
    Optional,
    Type,
)

import attr
from marshmallow import Schema
from marshmallow import ValidationError as MValidationError
from marshmallow import fields

from dl_api_commons.exc import RequestTimeoutError
from dl_api_lib import exc
from dl_constants.exc import (
    DEFAULT_ERR_CODE_API_PREFIX,
    GLOBAL_ERR_PREFIX,
)
from dl_core import exc as common_exc
from dl_formula.core import exc as formula_exc
import dl_query_processing.exc


LOGGER = logging.getLogger(__name__)

status = http.HTTPStatus
EXCEPTION_CODES = {
    dl_query_processing.exc.LogicError: status.BAD_REQUEST,
    dl_query_processing.exc.DatasetError: status.BAD_REQUEST,
    formula_exc.FormulaError: status.BAD_REQUEST,
    common_exc.DatabaseQueryError: status.BAD_REQUEST,
    common_exc.SourceAccessDenied: status.FORBIDDEN,
    common_exc.DataSourceErrorFromComponentError: status.BAD_REQUEST,
    common_exc.DatabaseUnavailable: status.BAD_REQUEST,
    common_exc.DatasetConfigurationError: status.BAD_REQUEST,
    common_exc.ConnectionConfigurationError: status.BAD_REQUEST,
    common_exc.USAccessDeniedException: status.FORBIDDEN,
    common_exc.USObjectNotFoundException: status.NOT_FOUND,
    common_exc.USPermissionCheckError: 530,
    common_exc.USLockUnacquiredException: status.LOCKED,
    common_exc.USBadRequestException: status.BAD_REQUEST,
    common_exc.USReadOnlyModeEnabledException: status.UNAVAILABLE_FOR_LEGAL_REASONS,
    common_exc.USIncorrectTenantIdException: status.CONFLICT,
    common_exc.QueryConstructorError: status.BAD_REQUEST,
    common_exc.ResultRowCountLimitExceeded: status.BAD_REQUEST,
    common_exc.TableNameNotConfiguredError: status.BAD_REQUEST,
    common_exc.NotAvailableError: status.BAD_REQUEST,
    common_exc.InvalidFieldError: status.BAD_REQUEST,
    common_exc.FieldNotFound: status.BAD_REQUEST,
    common_exc.USPermissionRequired: status.FORBIDDEN,
    exc.RLSConfigParsingError: status.BAD_REQUEST,
    common_exc.RLSSubjectNotFound: status.BAD_REQUEST,
    exc.FeatureNotAvailable: status.BAD_REQUEST,
    dl_query_processing.exc.FilterError: status.BAD_REQUEST,
    exc.UnsupportedForEntityType: status.BAD_REQUEST,
    common_exc.SourceAvatarNotFound: status.BAD_REQUEST,
    common_exc.ReferencedUSEntryNotFound: status.BAD_REQUEST,
    common_exc.ReferencedUSEntryAccessDenied: status.FORBIDDEN,
    common_exc.DataSourceTitleConflict: status.BAD_REQUEST,
    common_exc.DataSourcesInconsistent: status.BAD_REQUEST,
    dl_query_processing.exc.EmptyQuery: status.BAD_REQUEST,
    dl_query_processing.exc.InvalidQueryStructure: status.BAD_REQUEST,
    common_exc.PlatformPermissionRequired: status.FORBIDDEN,
    exc.DatasetRevisionMismatch: status.BAD_REQUEST,
    common_exc.EntityUsageNotAllowed: status.BAD_REQUEST,
    exc.DatasetActionNotAllowedError: status.BAD_REQUEST,
    dl_query_processing.exc.LegendError: status.BAD_REQUEST,
    dl_query_processing.exc.PivotError: status.BAD_REQUEST,
    common_exc.TypeCastFailed: status.BAD_REQUEST,
    dl_query_processing.exc.BlockSpecError: status.BAD_REQUEST,
    dl_query_processing.exc.TreeError: status.BAD_REQUEST,
    dl_query_processing.exc.ParameterError: status.BAD_REQUEST,
    common_exc.DashSQLError: status.BAD_REQUEST,
    dl_query_processing.exc.GenericInvalidRequestError: status.BAD_REQUEST,
    dl_query_processing.exc.InvalidGroupByConfiguration: status.BAD_REQUEST,
    common_exc.WrongQueryParameterization: status.BAD_REQUEST,
    RequestTimeoutError: status.FAILED_DEPENDENCY,
}


# TODO CONSIDER: Add private fields to store in logs for further quantity analytics
@attr.s(frozen=True, auto_attribs=True)
class BIError:
    """
    Class that represents errors occurred during BI requests handling.
     Is used for metrics collection and forming API error responses.
    """

    http_code: Optional[int]
    application_code_stack: tuple[str, ...]

    message: str  # treated as safe to display to final user
    details: dict
    debug: dict

    DEFAULT_ERROR_MESSAGE = "Internal Server Error"

    @staticmethod
    def get_default_error_code(
        err: Exception, exc_code_mapping: Optional[dict[Type[Exception], int]] = None
    ) -> Optional[int]:
        """
        :param err: Exception to map to HTTP status code
        :param exc_code_mapping: Override for default `EXCEPTION_CODES` exception to HTTP status code mapping
        :return: Mapped HTTP status code
        """

        if exc_code_mapping is None:
            exc_code_mapping = EXCEPTION_CODES

        for err_cls in inspect.getmro(type(err)):
            if err_cls in exc_code_mapping:
                return exc_code_mapping[err_cls]

        return None

    @classmethod
    def from_exception(
        cls,
        ex: Exception,
        default_message: Optional[str] = None,
        exc_code_mapping: Optional[dict[Type[Exception], int]] = None,
    ) -> "BIError":
        """
        Creates BIError from exception
        :param ex: Exception to create BIError from
        :param default_message: Message for exceptions that are not in whitelist
        :param exc_code_mapping: Override for default exception to HTTP status code mapping
        :return:
        """
        if default_message is None:
            default_message = cls.DEFAULT_ERROR_MESSAGE

        message: str = default_message
        details: dict[str, Any] = {}
        debug: dict[str, Any] = {}
        application_code_stack: tuple[str, ...] = ()
        http_code: Optional[int] = cls.get_default_error_code(ex, exc_code_mapping)

        if isinstance(ex, common_exc.DLBaseException):
            message = ex.message
            details = ex.details
            debug = ex.debug_info
            application_code_stack = tuple(ex.err_code)

        elif isinstance(ex, formula_exc.FormulaError):
            if not isinstance(ex, dl_query_processing.exc.FormulaHandlingError):
                # FIXME: Temporary measure. Later we'll switch to catching this error instead of FormulaError
                LOGGER.exception("Caught a non-wrapped FormulaError")

            if len(ex.errors) == 1:
                # There is only one error, the pass the code directly
                application_code_stack = ex.errors[0].code or tuple(dl_query_processing.exc.DLFormulaError.err_code)
            else:
                application_code_stack = tuple(dl_query_processing.exc.DLFormulaError.err_code)
            message = str(ex)

        elif isinstance(ex, MValidationError):
            message = str(ex)
            http_code = status.BAD_REQUEST

        else:
            # Any unknown exceptions leaves default values
            pass

        return BIError(
            http_code=http_code,
            application_code_stack=application_code_stack,
            message=message,
            details=details,
            debug=debug,
        )


class RegularAPIErrorSchema(Schema):
    code = fields.Method(serialize="serialize_error_code")
    message = fields.String()

    details = fields.Dict()  # In future will be replaced with schemas for each exception
    debug = fields.Dict()

    def get_api_prefix(self) -> str:
        # TODO CONSIDER: Warning in case of no api_prefix in context
        return self.context.get("api_prefix") or DEFAULT_ERR_CODE_API_PREFIX

    def serialize_error_code(self, data: BIError) -> str:
        return ".".join(
            itertools.chain(
                (
                    GLOBAL_ERR_PREFIX,
                    self.get_api_prefix(),
                ),
                data.application_code_stack,
            )
        )


class PublicAPIErrorSchema(RegularAPIErrorSchema):
    class Meta(RegularAPIErrorSchema.Meta):
        PUBLIC_FORWARDED_ERROR_CODES: ClassVar[frozenset[tuple[str, ...]]] = frozenset(
            (tuple(common_exc.MaterializationNotFinished.err_code),)
        )
        PUBLIC_DEFAULT_MESSAGE = "Something went wrong"
        PUBLIC_DEFAULT_ERR_CODE = "ERR.UNKNOWN"

    message = fields.Method(serialize="serialize_message")  # type: ignore  # TODO: fix

    debug = fields.Constant({})  # type: ignore  # TODO: fix
    details = fields.Constant({})  # type: ignore  # TODO: fix

    def serialize_error_code(self, data: BIError) -> str:
        if tuple(data.application_code_stack) in self.Meta.PUBLIC_FORWARDED_ERROR_CODES:
            return super().serialize_error_code(data)

        return self.Meta.PUBLIC_DEFAULT_ERR_CODE

    def serialize_message(self, data: BIError) -> str:
        if tuple(data.application_code_stack) in self.Meta.PUBLIC_FORWARDED_ERROR_CODES:
            return data.message

        else:
            return self.Meta.PUBLIC_DEFAULT_MESSAGE
