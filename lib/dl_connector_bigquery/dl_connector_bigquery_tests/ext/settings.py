import dl_testing


class Settings(dl_testing.BaseRootSettings):
    BIGQUERY_CONFIG: dict = NotImplemented
    BIGQUERY_CREDS: str = NotImplemented
