from typing import ClassVar, Optional

from bi_configs.connectors_data import ConnectorsDataBase


class ConnectorsDataFileBase(ConnectorsDataBase):
    CONN_FILE_CH_HOST: ClassVar[Optional[str]] = None
    CONN_FILE_CH_PORT: ClassVar[int] = 8443
    CONN_FILE_CH_USERNAME: ClassVar[Optional[str]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'FILE'
