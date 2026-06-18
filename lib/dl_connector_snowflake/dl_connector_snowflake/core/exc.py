from dl_core.exc import (
    DatabaseUnavailableError,
    SourceAccessInvalidTokenError,
    UserQueryAccessDeniedError,
)


class SnowflakeAccessTokenError(UserQueryAccessDeniedError):
    default_message = "Access token is expired or invalid"


class SnowflakeGetAccessTokenError(DatabaseUnavailableError):
    default_message = "Could not get access toke for the snowflake connection"


class SnowflakeRefreshTokenInvalidError(SourceAccessInvalidTokenError):
    default_message = "OAuth refresh token for the Snowflake connections is invalid or expired"
