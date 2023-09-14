from __future__ import annotations

from typing import Any, Dict, Optional

import requests

from bi_constants.types import TJSONLike
from bi_constants.exc import DLBaseException


class RSTError(Exception):
    """
    An intermediary error that wraps the "Connection reset by peer"
    ClientOSError cases.
    Made for skipping the `errno` check in a main exception handler.
    """


class AIOHttpConnTimeoutError(Exception):
    """
    An intermediary error that wraps the "Connection timeout"
    aiohttp.client_exceptions.ServerTimeoutError.
    Made for skipping the exc message check in a main exception handler.
    """


class UnsupportedNativeTypeError(Exception):
    pass


class UnsupportedBITypeError(Exception):
    pass


class UnsupportedDatabaseError(Exception):
    pass


class TypeCastError(DLBaseException):
    err_code = DLBaseException.err_code + ['TYPE_CAST']


class TypeCastUnsupported(TypeCastError):
    err_code = TypeCastError.err_code + ['UNSUPPORTED']


class TypeCastFailed(TypeCastError):
    err_code = TypeCastError.err_code + ['FAILED']
    default_message = 'Type casting failed for value'


class SourceAccessDenied(DLBaseException):
    err_code = DLBaseException.err_code + ['SOURCE_ACCESS_DENIED']


class SourceAccessInvalidToken(SourceAccessDenied):
    err_code = SourceAccessDenied.err_code + ['INVALID_TOKEN']
    default_message = 'Invalid user token'


class DatabaseUnavailable(DLBaseException):
    err_code = DLBaseException.err_code + ['DATABASE_UNAVAILABLE']
    default_message = 'Data source is unavailable'


class DataSourceConfigurationError(DLBaseException):
    err_code = DLBaseException.err_code + ['SOURCE_CONFIG']


class TableNameNotConfiguredError(DataSourceConfigurationError):
    err_code = DataSourceConfigurationError.err_code + ['TABLE_NOT_CONFIGURED']
    default_message = 'Table is not ready yet'
    details = {'info': 'No table for data source'}


class TableNameInvalidError(DataSourceConfigurationError):
    err_code = DataSourceConfigurationError.err_code + ['TABLE_NAME_INVALID']


class DatasetConfigurationError(DLBaseException):
    err_code = DLBaseException.err_code + ['DS_CONFIG']


class NoCommonRoleError(DLBaseException):
    err_code = DatasetConfigurationError.err_code + ['NO_COMMON_ROLE']


# Data sources

class DataSourceError(DLBaseException):
    err_code = DLBaseException.err_code + ['SOURCE']
    default_message = 'Data source error'


class DataSourceNotFound(DataSourceError):
    err_code = DataSourceError.err_code + ['NOT_FOUND']
    default_message = 'Data source not found'


class DataSourceTitleError(DataSourceError):
    err_code = DataSourceError.err_code + ['TITLE']
    default_message = 'Invalid data source title'


class DataSourceTitleConflict(DataSourceTitleError):
    err_code = DataSourceTitleError.err_code + ['CONFLICT']
    default_message = 'Data source title conflicts with another source'


class DataSourcesInconsistent(DataSourceError):
    err_code = DataSourceError.err_code + ['INCONSISTENT']
    default_message = 'Data sources are inconsistent'


class DataSourceErrorFromComponentError(DataSourceError):
    err_code = DataSourceError.err_code


# Source avatars

class SourceAvatarError(DLBaseException):
    err_code = DLBaseException.err_code + ['AVATAR']
    default_message = 'Source avatar error'


class SourceAvatarNotFound(SourceAvatarError):
    err_code = SourceAvatarError.err_code + ['NOT_FOUND']
    default_message = 'Source avatar not found'


class SourceAvatarTitleError(SourceAvatarError):
    err_code = SourceAvatarError.err_code + ['TITLE']
    default_message = 'Invalid source avatar title'


class SourceAvatarTitleConflict(SourceAvatarTitleError):
    err_code = SourceAvatarTitleError.err_code + ['CONFLICT']
    default_message = 'Source avatar title conflicts with another avatar'


class UnknownReferencedAvatar(SourceAvatarNotFound):
    err_code = SourceAvatarNotFound.err_code + ['FIELD_REF']
    default_message = 'Field references an unknown source avatar'


class UnboundAvatarError(DatasetConfigurationError):
    err_code = DatasetConfigurationError.err_code + ['UNBOUND_AVATAR']


# Avatar relations

class AvatarRelationError(DLBaseException):
    err_code = DLBaseException.err_code + ['RELATION']
    default_message = 'Avatar relation error'


class AvatarRelationNotFound(AvatarRelationError):
    err_code = AvatarRelationError.err_code + ['NOT_FOUND']
    default_message = 'Avatar relation not found'


class AvatarRelationJoinTypeError(AvatarRelationError):
    err_code = AvatarRelationError.err_code + ['JOIN_TYPE']
    default_message = 'Invalid join type for referenced data sources'


# Obligatory filters

class ObligatoryFilterError(DLBaseException):
    err_code = DLBaseException.err_code + ['OBLIG_FILTER']
    default_message = 'Obligatory filter error'


class ObligatoryFilterNotFound(ObligatoryFilterError):
    err_code = ObligatoryFilterError.err_code + ['NOT_FOUND']
    default_message = 'Obligatory filter not found'


# Fields

class FieldError(DLBaseException):
    err_code = DLBaseException.err_code + ['FIELD']
    default_message = 'Field error'


class InvalidFieldError(FieldError):
    err_code = FieldError.err_code + ['INVALID']
    default_message = 'Field is invalid'


class FieldNotFound(FieldError):
    err_code = FieldError.err_code + ['NOT_FOUND']
    default_message = 'Unknown field'


class FieldTitleError(FieldError):
    err_code = FieldError.err_code + ['TITLE']
    default_message = 'Invalid field title'


class FieldTitleConflict(FieldTitleError):
    err_code = FieldTitleError.err_code + ['CONFLICT']
    default_message = 'Field title conflicts with another field'


# US Client errors

class USReqException(DLBaseException):
    err_code = DLBaseException.err_code + ['US']

    def __init__(
        self,
        message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        orig: Optional[Exception] = None,
        orig_exc: Optional[Exception] = None,
        debug_info: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        self.orig_exc = orig_exc  # TODO: stop using '.orig_exc' and '.orig
        super().__init__(
            message=message,
            details=details,
            orig=orig if orig is not None else orig_exc,
            debug_info=debug_info,
            params=params
        )


class USObjectNotFoundException(USReqException):
    default_message = 'Object not found'
    err_code = USReqException.err_code + ['OBJ_NOT_FOUND']


class USAccessDeniedException(USReqException):
    default_message = 'Access denied'
    err_code = USReqException.err_code + ['ACCESS_DENIED']


class USLockUnacquiredException(USReqException):
    default_message = 'Object locked'
    err_code = USReqException.err_code + ['ENTRY_LOCKED']


class USPermissionCheckError(USReqException):
    err_code = USReqException.err_code + ['PERMISSIONS_ERROR']

    @property
    def message(self) -> str:
        return 'US permission check error'


class USBadRequestException(USReqException):
    err_code = USReqException.err_code + ['BAD_REQUEST']

    @property
    def message(self) -> str:
        try:
            if self.orig_exc and isinstance(getattr(self.orig_exc, 'response'), requests.Response):
                return getattr(self.orig_exc, 'response').json()['message']
        except (ValueError, KeyError, AttributeError):
            pass
        return super().message


class USAlreadyExistsException(USBadRequestException):
    err_code = USBadRequestException.err_code + ['ALREADY_EXISTS']


class USNotCorrectFolderIdException(USReqException):
    @property
    def message(self) -> str:
        return 'Not correct folder_id'


class USInteractionDisabled(USReqException):
    pass


class USInvalidResponse(USReqException):
    pass


# TODO FIX: Validate err_code/message (and think about should this exception be thrown up to user)
class UnexpectedUSEntryType(DLBaseException):
    """Should be thrown by USM if expected type does not match actual US entry type"""
    err_code = DLBaseException.err_code + ['UNEXPECTED_ENTRY_TYPE']
    default_message = 'Unexpected entry type'


# Other

class FailedToLoadSchema(DLBaseException):
    err_code = DLBaseException.err_code + ['COLUMN_SCHEMA_FAILED']
    default_message = 'Failed to load description of table columns.'


class DatabaseQueryError(DLBaseException):
    err_code = DLBaseException.err_code + ['DB']
    default_message = 'Database error.'

    db_message: Optional[str] = None
    query: Optional[str] = None

    def __init__(
        self,
        db_message: Optional[str] = None,
        query: Optional[str] = None,
        message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        orig: Optional[Exception] = None,
        debug_info: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message=message, details=details, orig=orig, debug_info=debug_info, params=params)
        self.db_message = db_message
        if db_message is not None:
            self.details.update(db_message=db_message)
        self.query = query
        # Future note about the `self.debug_info` / `self.details`:
        # Currently (at least on the internal installation),
        # both debug and details should be printed to the user.
        # But `details` are to-be-done, and should contain templatable
        # i18n-able something.
        # Therefore, for now, there's no use case for `details`,
        # and they're not printed to the user.
        self.debug_info.setdefault('db_message', db_message)
        self.debug_info.setdefault('query', query)

    @classmethod
    def _from_jsonable_dict(cls, data: Dict) -> 'DLBaseException':
        new_exc = cls(
            db_message=data.pop('db_message'),
            query=data.pop('query'),
            message=data.pop('message', None),
        )
        new_exc.details = data.pop('details')
        new_exc.debug_info = data.pop('debug_info')
        new_exc.params = data.pop('params', dict())
        return new_exc

    def to_jsonable_dict(self) -> Dict[str, TJSONLike]:
        d = super().to_jsonable_dict()
        d.update(
            db_message=self.db_message,
            query=self.query,
        )
        return d

    def __str__(self) -> str:
        return '{0.db_message} (DB query: {0.query})'.format(self)


class ResultRowCountLimitExceeded(DLBaseException):
    err_code = DLBaseException.err_code + ['ROW_COUNT_LIMIT']
    default_message = 'Received too many result data rows.'


class SourceDoesNotExist(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['SOURCE_DOES_NOT_EXIST']
    default_message = 'Data source (table) does not exist.'
    formatting_messages = {frozenset({'table_definition'}): 'Data source (table) identified by: `{table_definition}` does not exist.'}

    def __init__(
        self,
        db_message: Optional[str] = None,
        query: Optional[str] = None,
        message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        orig: Optional[Exception] = None,
        debug_info: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        super(SourceDoesNotExist, self).__init__(
            db_message=db_message,
            query=query,
            message=message,
            details=details,
            orig=orig,
            debug_info=debug_info,
            params=params or {},
        )

        if self.params.get("table_definition"):
            return


class DatabaseDoesNotExist(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['DATABASE_DOES_NOT_EXIST']
    default_message = 'Data source database does not exist.'


class ColumnDoesNotExist(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['COLUMN_DOES_NOT_EXIST']
    default_message = 'Requested database column does not exist.'


class InvalidQuery(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['INVALID_QUERY']
    default_message = 'Invalid SQL query to the database.'


class SourceResponseError(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['SOURCE_ERROR']
    default_message = 'Data source failed to respond correctly.'


class SourceTimeout(SourceResponseError):
    err_code = SourceResponseError.err_code + ['TIMEOUT']
    default_message = 'Data source timed out.'


class SourceClosedPrematurely(SourceResponseError):
    err_code = SourceResponseError.err_code + ['CLOSED_PREMATURELY']
    default_message = 'Data source ended the response prematurely (disconnected or dropped the connection).'


class SourceProtocolError(SourceResponseError):
    err_code = SourceResponseError.err_code + ['INVALID_RESPONSE']
    default_message = 'Data source failed to respond correctly (invalid HTTP or higher protocol response).'


class InvalidArgumentType(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['INVALID_ARGUMENT_TYPE']
    default_message = 'Invalid argument type.'


class UnknownFunction(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['INVALID_FUNCTION']
    default_message = 'Unknown function.'


class SourceConnectError(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['SOURCE_CONNECT_ERROR']
    default_message = 'Data source refused connection.'


class SourceHostNotKnownError(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['SOURCE_HOST_NOT_KNOWN_ERROR']
    default_message = 'Data source host name or service not known.'


class DatabaseOperationalError(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['DB_OPERATIONAL_ERROR']


class MaterializationNotFinished(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['MATERIALIZATION_NOT_FINISHED']
    default_message = 'Data is not available because materialization is not yet complete.'


class DbMemoryLimitExceeded(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['MEMORY_LIMIT_EXCEEDED']
    default_message = 'Memory limit has been exceeded during query execution.'


class DbAuthenticationFailed(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['AUTHENTICATION_FAILED']
    default_message = 'Database authentication failed.'


class DBIndexNotUsed(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['INDEX_NOT_USED']
    default_message = 'Filtration by any of indexed columns required.'


class DataParseError(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['CANNOT_PARSE']
    default_message = 'Cannot parse value.'


class CannotParseNumber(DataParseError):
    err_code = DataParseError.err_code + ['NUMBER']
    default_message = 'Cannot parse number.'


class CannotParseDateTime(DataParseError):
    err_code = DataParseError.err_code + ['DATETIME']
    default_message = 'Cannot parse datetime.'


class NumberOutOfRange(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['NUMBER_OUT_OF_RANGE']
    default_message = 'Number out of range.'


class UnexpectedInfOrNan(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['UNEXPECTED_INF_OR_NAN']
    default_message = 'Unexpected inf or nan to integer conversion.'


class JoinColumnTypeMismatch(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['JOIN_COLUMN_TYPE_MISMATCH']
    default_message = 'Columns in JOIN have different types.'


class NoSpaceLeft(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['NO_SPACE_LEFT']
    default_message = 'No space left on device.'


class DivisionByZero(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['ZERO_DIVISION']
    default_message = 'Division by zero.'


class UserQueryAccessDenied(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['USER_QUERY_ACCESS_DENIED']
    default_message = 'Query access denied to user'


class WrongQueryParameterization(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['WRONG_QUERY_PARAMETERIZATION']
    default_message = "Wrong query parameterization. Parameter was not found"


class CHYTQueryError(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['CHYT']
    default_message = 'CHYT Error'


class CHYTAuthError(CHYTQueryError):
    err_code = CHYTQueryError.err_code + ['AUTH_FAILED']
    default_message = 'Authentication failed'


class CHYTCliqueError(DatabaseQueryError):
    err_code = CHYTQueryError.err_code + ['CLIQUE']
    default_message = 'Clique Error'


class CHYTCliqueIsNotRunning(CHYTCliqueError):
    err_code = CHYTCliqueError.err_code + ['NOT_RUNNING']
    default_message = 'Clique is not running.'
    formatting_messages = {frozenset({'clique'}): 'Clique {clique} is not running.'}


class CHYTCliqueIsSuspended(CHYTCliqueError):
    err_code = CHYTCliqueError.err_code + ['SUSPENDED']
    default_message = 'Clique is suspended.'
    formatting_messages = {frozenset({'clique'}): 'Clique {clique} is suspended.'}


class CHYTCliqueNotExists(CHYTCliqueError):
    err_code = CHYTCliqueError.err_code + ['INVALID_SPECIFICATION']
    default_message = 'Invalid clique specification. Probably, clique does not exists.'


class CHYTCliqueAccessDenied(CHYTCliqueError):
    err_code = CHYTCliqueError.err_code + ['ACCESS_DENIED']
    default_message = 'Access to clique was denied.'
    formatting_messages = {frozenset({'clique', 'user'}): 'Access to clique {clique} for user {user} was denied.'}


class CHYTTableHasNoSchema(CHYTQueryError):
    err_code = CHYTQueryError.err_code + ['TABLE_HAS_NO_SCHEMA']
    default_message = 'The table has no schema. Only schematized tables are supported.'


class CHYTTableAccessDenied(CHYTQueryError):
    err_code = CHYTQueryError.err_code + ['TABLE_ACCESS_DENIED']
    default_message = 'Access to table was denied.'


class CHYTInvalidSortedJoin(CHYTQueryError):
    err_code = CHYTQueryError.err_code + ['INVALID_SORTED_JOIN']
    default_message = 'Invalid sorted JOIN.'


class CHYTISJNotAKeyColumn(CHYTInvalidSortedJoin):
    err_code = CHYTInvalidSortedJoin.err_code + ['NOT_A_KEY_COLUMN']
    default_message = 'Column used in join expression is not a key column.'
    formatting_messages = {frozenset({'col'}): 'Column {col} used in join expression is not a key column.'}


class CHYTISJNotKeyPrefixColumn(CHYTInvalidSortedJoin):
    err_code = CHYTInvalidSortedJoin.err_code + ['NOT_KEY_PREFIX_COLUMN']
    default_message = 'Joined columns should form prefix of joined table key columns.'


class CHYTISJMoreThanOneTable(CHYTInvalidSortedJoin):
    err_code = CHYTInvalidSortedJoin.err_code + ['MORE_THAN_ONE_TABLE']
    default_message = (
        'Cannot join a concatenation of tables with another table. '
        'Wrap concatenation into an SQL query.'
    )


class CHYTISJTableNotSorted(CHYTInvalidSortedJoin):
    err_code = CHYTInvalidSortedJoin.err_code + ['TABLE_NOT_SORTED']
    default_message = 'Tables should be sorted'
    formatting_messages = {frozenset({'table'}): 'Table {table} should be sorted.'}


class CHYTISJCompoundExpressionsNotSupported(CHYTInvalidSortedJoin):
    err_code = CHYTInvalidSortedJoin.err_code + ['COMPOUND_EXPR_NOT_SUPPORTED']
    default_message = 'CHYT does not support compound expressions in ON/USING clause'


class CHYTISJKeyIsEmpty(CHYTInvalidSortedJoin):
    err_code = CHYTInvalidSortedJoin.err_code + ['KEY_IS_EMPTY']
    default_message = 'Cannot join: key is empty'


class CHYTISJConcatNotSupported(CHYTInvalidSortedJoin):
    err_code = CHYTInvalidSortedJoin.err_code + ['CONCAT_NOT_SUPPORTED']
    default_message = 'Joining concatenation of multiple tables is not supported'


class CHYTISJNotSameKeyPosition(CHYTInvalidSortedJoin):
    err_code = CHYTInvalidSortedJoin.err_code + ['NOT_SAME_KEY_POSITION']
    default_message = 'Joined columns do not occupy same positions in key columns of joined tables'
    formatting_messages = {
        frozenset({'col1', 'col2'}): 'Joined columns {col1} and {col2}'
                                     ' do not occupy same positions in key columns of joined tables',
    }


class CHYTMultipleDynamicTablesNotSupported(CHYTQueryError):
    err_code = CHYTQueryError.err_code + ['MULTI_DYN_NOT_SUPPORTED']
    default_message = (
        'Reading multiple dynamic tables '
        'or dynamic table together with static table is not supported in CHYT (CHYT-526)'
    )


class CHYTSubqueryWeightLimitExceeded(CHYTQueryError):
    err_code = CHYTQueryError.err_code + ['SUBQ_WEIGHT_LIMIT_EXCEEDED']
    default_message = 'Subquery exceeds data weight limit'


class QueryConstructorError(Exception):
    pass


class DBSessionError(Exception):
    pass


class EntryValidationError(Exception):
    def __init__(self, message: str, model_field: Optional[str] = None):
        self.message = message
        self.model_field = model_field


class DataStreamValidationError(Exception):
    line_idx: Optional[int]
    line_value: Optional[str]
    field_idx: Optional[int]
    field_name: Optional[str]
    value: Optional[Any]

    def __init__(
            self, *args: Any, line_idx: Optional[int] = None, line_value: Optional[str] = None,
            field_idx: Optional[int] = None, field_name: Optional[str] = None, value: Any = None):
        self.line_idx = line_idx
        self.line_value = line_value
        self.field_idx = field_idx
        self.field_name = field_name
        self.value = value
        super().__init__(*args)

    # TODO FIX: Move to error schemas
    @staticmethod
    def jsonify_value(val: Any) -> TJSONLike:
        if isinstance(val, bytes):
            return val.decode('utf-8', 'backslashreplace')
        elif isinstance(val, (str, int, float)):
            return val
        elif val is None:
            return val
        else:
            # TODO CONSIDER: different variants for logging & error response
            return {
                "unexpected_value_type": str(type(val)),
                "str_val": str(val)
            }

    # TODO FIX: Replace with schema
    def get_context_dict(self) -> Dict[str, TJSONLike]:
        return dict(
            line_idx=self.jsonify_value(self.line_idx),
            line_value=self.jsonify_value(self.line_value),
            field_idx=self.jsonify_value(self.field_idx),
            field_name=self.jsonify_value(self.field_name),
            value=self.jsonify_value(self.value),
        )


class ReferencedUSEntryNotFound(DLBaseException):
    err_code = DLBaseException.err_code + ['REFERENCED_ENTRY_NOT_FOUND']
    default_message = 'Some referenced entries not found'


class ReferencedUSEntryAccessDenied(DLBaseException):
    err_code = DLBaseException.err_code + ['REFERENCED_ENTRY_ACCESS_DENIED']
    default_message = 'Some referenced entries cannot be loaded: access denied'


class ConnectionConfigurationError(DLBaseException):
    err_code = DLBaseException.err_code + ['CONNECTION_CONFIG']


class SubselectNotAllowed(ConnectionConfigurationError):
    err_code = ConnectionConfigurationError.err_code + ['SUBSELECT_NOT_ALLOWED']
    default_message = 'Subquery source is disallowed in the connection settings'


class DashSQLNotAllowed(ConnectionConfigurationError):
    err_code = ConnectionConfigurationError.err_code + ['DASHSQL_NOT_ALLOWED']
    default_message = 'DashSQL API is disallowed in the connection settings'


class NotAvailableError(DLBaseException):
    err_code = DLBaseException.err_code + ['NOT_AVAILABLE']
    default_message = 'Not available'


class InvalidColumnError(DLBaseException):
    err_code = DLBaseException.err_code + ['INVALID_COLUMN']


class YCPermissionRequired(DLBaseException):
    err_code = DLBaseException.err_code + ['CLOUD_PERMISSION_REQUIRED']


class EntityUsageNotAllowed(DLBaseException):
    err_code = DLBaseException.err_code + ['ENTITY_USAGE_NOT_ALLOWED']
    default_message = 'Some of the entities cannot be used in this context'


class USPermissionRequired(USReqException):
    err_code = USReqException.err_code + ['PERMISSION_REQUIRED']

    entry_id: str
    permission: str

    def __init__(self, entry_id: str, permission: str):
        super().__init__()
        self.details.update(entry_id=entry_id, permission=permission)
        self.entry_id = entry_id
        self.permission = permission

    @property
    def message(self) -> str:
        return f'No permission {self.permission} for entry {self.entry_id}'


class CHRowTooLarge(SourceProtocolError):
    err_code = SourceProtocolError.err_code + ['TOO_LARGE_ROW']
    default_message = 'Data source failed to respond correctly (too large row).'


class DataSourceMigrationImpossible(DLBaseException):
    err_code = DLBaseException.err_code + ['DSRC_MIGRATION_IMPOSSIBLE']


class RLSSubjectNotFound(DLBaseException):
    err_code = DLBaseException.err_code + ['RLS_SUBJECT_NOT_FOUND']
