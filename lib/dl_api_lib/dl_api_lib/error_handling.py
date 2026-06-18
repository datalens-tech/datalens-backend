from __future__ import annotations

import http
import inspect
import itertools
import logging
from typing import Any

import attr
from marshmallow import Schema
from marshmallow import ValidationError as MValidationError
from marshmallow import fields

from dl_api_commons import exc as api_commons_exc
from dl_api_lib import exc
from dl_constants.exc import DEFAULT_GLOBAL_ERR_CODE_API_PREFIX
from dl_core import exc as common_exc
from dl_dashsql import exc as dashsql_exc
from dl_formula.core import exc as formula_exc
import dl_query_processing.exc
import dl_rls.exc
import dl_type_transformer.exc

LOGGER = logging.getLogger(__name__)

status = http.HTTPStatus
EXCEPTION_CODES = {
    dl_query_processing.exc.LogicError: status.BAD_REQUEST,
    dl_query_processing.exc.DatasetError: status.BAD_REQUEST,
    formula_exc.FormulaError: status.BAD_REQUEST,
    common_exc.DatabaseQueryError: status.BAD_REQUEST,
    common_exc.DatabaseReadOnlyTransactionError: status.BAD_REQUEST,
    common_exc.SourceAccessDeniedError: status.FORBIDDEN,
    common_exc.DataSourceErrorFromComponentError: status.BAD_REQUEST,
    common_exc.DatabaseUnavailableError: status.BAD_REQUEST,
    common_exc.DatasetConfigurationError: status.BAD_REQUEST,
    common_exc.ConnectionConfigurationError: status.BAD_REQUEST,
    common_exc.USAccessDeniedError: status.FORBIDDEN,
    common_exc.USWorkbookIsolationInterruptionError: status.FORBIDDEN,
    common_exc.USObjectNotFoundError: status.NOT_FOUND,
    common_exc.USPermissionCheckError: 530,
    common_exc.USLockUnacquiredError: status.LOCKED,
    common_exc.USBadRequestError: status.BAD_REQUEST,
    common_exc.USValidationError: status.BAD_REQUEST,
    common_exc.USConnectionAccessDeniedError: status.FORBIDDEN,
    common_exc.USConnectionNotFoundError: status.NOT_FOUND,
    common_exc.USInvalidConnectionIDError: status.BAD_REQUEST,
    common_exc.USReadOnlyModeEnabledError: status.UNAVAILABLE_FOR_LEGAL_REASONS,
    common_exc.USIncorrectTenantIdError: status.CONFLICT,
    common_exc.QueryConstructorError: status.BAD_REQUEST,
    common_exc.ResultRowCountLimitExceededError: status.BAD_REQUEST,
    common_exc.TableNameNotConfiguredError: status.BAD_REQUEST,
    common_exc.MalformedCredentialsError: status.BAD_REQUEST,
    common_exc.NotAvailableError: status.BAD_REQUEST,
    common_exc.InvalidFieldError: status.BAD_REQUEST,
    common_exc.FieldNotFoundError: status.BAD_REQUEST,
    common_exc.USPermissionRequiredError: status.FORBIDDEN,
    dl_rls.exc.RLSError: status.BAD_REQUEST,
    exc.FeatureNotAvailableError: status.BAD_REQUEST,
    dl_query_processing.exc.FilterError: status.BAD_REQUEST,
    exc.UnsupportedForEntityTypeError: status.BAD_REQUEST,
    exc.BadConnectionTypeError: status.BAD_REQUEST,
    common_exc.SourceAvatarNotFoundError: status.BAD_REQUEST,
    common_exc.ReferencedUSEntryNotFoundError: status.BAD_REQUEST,
    common_exc.ReferencedUSEntryAccessDeniedError: status.FORBIDDEN,
    common_exc.DataSourceTitleConflictError: status.BAD_REQUEST,
    common_exc.DataSourcesInconsistentError: status.BAD_REQUEST,
    common_exc.TemplateInvalidError: status.BAD_REQUEST,
    common_exc.ParameterValueInvalidError: status.BAD_REQUEST,
    common_exc.ConnectionTemplateDisabledError: status.BAD_REQUEST,
    dl_query_processing.exc.EmptyQueryError: status.BAD_REQUEST,
    dl_query_processing.exc.InvalidQueryStructureError: status.BAD_REQUEST,
    common_exc.PlatformPermissionRequiredError: status.FORBIDDEN,
    exc.DatasetRevisionMismatchError: status.BAD_REQUEST,
    common_exc.EntityUsageNotAllowedError: status.BAD_REQUEST,
    exc.DatasetActionNotAllowedError: status.BAD_REQUEST,
    dl_query_processing.exc.LegendError: status.BAD_REQUEST,
    dl_query_processing.exc.PivotError: status.BAD_REQUEST,
    dl_type_transformer.exc.TypeCastFailedError: status.BAD_REQUEST,
    dl_query_processing.exc.BlockSpecError: status.BAD_REQUEST,
    dl_query_processing.exc.TreeError: status.BAD_REQUEST,
    dashsql_exc.DashSQLError: status.BAD_REQUEST,
    dl_query_processing.exc.GenericInvalidRequestError: status.BAD_REQUEST,
    dl_query_processing.exc.InvalidGroupByConfigurationError: status.BAD_REQUEST,
    common_exc.WrongQueryParameterizationError: status.BAD_REQUEST,
    api_commons_exc.RequestTimeoutError: status.FAILED_DEPENDENCY,
    exc.ConnectorIconNotFoundError: status.NOT_FOUND,
    api_commons_exc.FailedDependencyError: status.FAILED_DEPENDENCY,
    common_exc.UnknownEntryMigrationError: status.NOT_IMPLEMENTED,
    common_exc.FailedToLoadSchemaError: status.BAD_REQUEST,
    common_exc.InvalidRequestError: status.BAD_REQUEST,
    exc.WorkbookImportError: status.BAD_REQUEST,
    exc.WorkbookExportError: status.BAD_REQUEST,
    common_exc.DataSourceNotFoundError: status.BAD_REQUEST,
    exc.CacheInvalidationTestNotEditorError: status.FORBIDDEN,
    exc.CacheInvalidationTestConnectionViewRequiredError: status.FORBIDDEN,
    exc.CacheInvalidationTestError: status.BAD_REQUEST,
    exc.ConnectionTemplateNotFoundError: status.BAD_REQUEST,
    exc.PreviewSourceModificationNotAllowedError: status.FORBIDDEN,
    common_exc.UnexpectedUSEntryTypeError: status.BAD_REQUEST,
}


# TODO CONSIDER: Add private fields to store in logs for further quantity analytics
@attr.s(frozen=True, auto_attribs=True)
class BIError:
    """
    Class that represents errors occurred during BI requests handling.
     Is used for metrics collection and forming API error responses.
    """

    http_code: int | None
    application_code_stack: tuple[str, ...]
    forward_for_anonymous: bool

    message: str  # treated as safe to display to final user
    details: dict
    debug: dict

    DEFAULT_ERROR_MESSAGE = "Internal Server Error"

    @staticmethod
    def get_default_error_code(
        err: Exception, exc_code_mapping: dict[type[Exception], int] | None = None
    ) -> int | None:
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
        default_message: str | None = None,
        exc_code_mapping: dict[type[Exception], int] | None = None,
    ) -> BIError:
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
        forward_for_anonymous = False
        http_code: int | None = cls.get_default_error_code(ex, exc_code_mapping)

        if isinstance(ex, common_exc.DLBaseError):
            message = ex.message
            details = ex.details
            debug = ex.debug_info
            forward_for_anonymous = ex.forward_for_anonymous
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
            forward_for_anonymous=forward_for_anonymous,
            message=message,
            details=details,
            debug=debug,
        )


class RegularAPIErrorSchema(Schema):
    code = fields.Method(serialize="serialize_error_code")
    message = fields.String()

    details = fields.Dict()  # In future will be replaced with schemas for each exception
    debug = fields.Dict()

    def serialize_error_code(self, data: BIError) -> str:
        return ".".join(
            itertools.chain(
                DEFAULT_GLOBAL_ERR_CODE_API_PREFIX,
                data.application_code_stack,
            )
        )


class PublicAPIErrorSchema(RegularAPIErrorSchema):
    PUBLIC_DEFAULT_MESSAGE = "Something went wrong"
    PUBLIC_DEFAULT_ERR_CODE = "ERR.UNKNOWN"

    message = fields.Method(serialize="serialize_message")  # type: ignore  # TODO: fix

    debug = fields.Constant({})  # type: ignore  # TODO: fix
    details = fields.Constant({})  # type: ignore  # TODO: fix

    def serialize_error_code(self, data: BIError) -> str:
        if data.forward_for_anonymous:
            return super().serialize_error_code(data)

        return self.PUBLIC_DEFAULT_ERR_CODE

    def serialize_message(self, data: BIError) -> str:
        if data.forward_for_anonymous:
            return data.message

        return self.PUBLIC_DEFAULT_MESSAGE
