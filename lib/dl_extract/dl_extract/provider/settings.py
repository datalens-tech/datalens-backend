import dl_settings


class PrimitiveClusterExtractClickhouseProviderSettings(dl_settings.BaseSettings):
    """
    Settings for single-host clickhouse installation
    """

    # Note: connector will use HOSTS[0] as host and entire list as multihost arguments
    HOSTS: list[str]
    PORT: int
    DATABASE: str
    USERNAME: str
    PASSWORD: str
    SECURE: bool
