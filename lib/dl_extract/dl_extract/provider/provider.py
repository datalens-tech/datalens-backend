import abc

import attr

from dl_extract.provider.settings import PrimitiveClusterExtractClickhouseProviderSettings


@attr.s(frozen=True)
class ExtractClickhouseProviderArguments:
    """
    Arguments of clickhouse provider. Subset of fields that are required to
    determine which clickhouse settings to return, but without importing dl_core.

    This type is here to avoid using dl_core as dependency in temporal workflows.
    """

    dataset_id: str = attr.ib()


@attr.s(frozen=True)
class ExtractClickhouseConfig:
    """
    Settings required to establish connection to clickhouse in order to use
    extract table.
    """

    hosts: list[str] = attr.ib()
    port: int = attr.ib()
    database: str = attr.ib()
    table: str = attr.ib()
    username: str = attr.ib()
    password: str = attr.ib()


class ExtractClickhouseProvider(abc.ABC):
    """
    Extract clickhouse settings provider class.

    Returns clickhouse settings that can be used to access dataset's extract data.
    """

    @abc.abstractmethod
    def get_clickhouse_config(self, arguments: ExtractClickhouseProviderArguments) -> ExtractClickhouseConfig: ...


@attr.s(frozen=True)
class StaticExtractClickhouseProvider(ExtractClickhouseProvider):
    """
    Extract clickhouse provider with static values for testing and
    single-database installation.
    """

    hosts: list[str] = attr.ib()
    port: int = attr.ib()
    database: str = attr.ib()
    username: str = attr.ib()
    password: str = attr.ib()

    def get_clickhouse_config(self, arguments: ExtractClickhouseProviderArguments) -> ExtractClickhouseConfig:
        return ExtractClickhouseConfig(
            hosts=self.hosts,
            port=self.port,
            database=self.database,
            table=f"extract_{arguments.dataset_id}",
            username=self.username,
            password=self.password,
        )

    @classmethod
    def from_settings(
        cls,
        settings: PrimitiveClusterExtractClickhouseProviderSettings,
    ) -> "StaticExtractClickhouseProvider":
        return cls(
            hosts=settings.HOSTS,
            port=settings.PORT,
            database=settings.DATABASE,
            username=settings.USERNAME,
            password=settings.PASSWORD,
        )
