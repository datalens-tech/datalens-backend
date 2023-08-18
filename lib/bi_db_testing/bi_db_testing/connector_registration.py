from typing import Type

from bi_db_testing.connectors.base.connector import DbTestingConnector
from bi_db_testing.database.engine_wrapper import register_engine_wrapper_cls


_REGISTERED: set[Type[DbTestingConnector]] = set()


class DbTestingConnectorRegistrator:
    @classmethod
    def register_connector(cls, connector: Type[DbTestingConnector]) -> None:
        if connector in _REGISTERED:
            return

        for engine_wrapper_cls in connector.engine_wrapper_classes:
            register_engine_wrapper_cls(engine_wrapper_cls)

        _REGISTERED.add(connector)


CONN_REG_DB_TESTING = DbTestingConnectorRegistrator
