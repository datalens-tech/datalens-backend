import dl_testing


class Settings(dl_testing.BaseRootSettings):
    SNOWFLAKE_CONFIG: dict = NotImplemented
    SNOWFLAKE_CLIENT_SECRET: str = NotImplemented
    SNOWFLAKE_REFRESH_TOKEN_EXPIRED: str = NotImplemented
    SNOWFLAKE_REFRESH_TOKEN_X: str = NotImplemented
