from dl_core.exc import (
    DatabaseUnavailable,
    SourceAccessInvalidToken,
    UserQueryAccessDenied,
)


class SnowflakeAccessTokenError(UserQueryAccessDenied):
    default_message = "Access token is expired or invalid"


class SnowflakeGetAccessTokenError(DatabaseUnavailable):
    default_message = "Could not get access toke for the snowflake connection"


class SnowflakeRefreshTokenInvalid(SourceAccessInvalidToken):
    default_message = "OAuth refresh token for the Snowflake connections is invalid or expired"
