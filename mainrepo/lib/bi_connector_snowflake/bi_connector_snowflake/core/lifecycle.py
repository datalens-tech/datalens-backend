import logging

from bi_core.connectors.base.lifecycle import ConnectionLifecycleManager

from bi_connector_snowflake.core.us_connection import ConnectionSQLSnowFlake

LOGGER = logging.getLogger(__name__)


class SnowFlakeConnectionLifecycleManager(ConnectionLifecycleManager[ConnectionSQLSnowFlake]):
    ENTRY_CLS = ConnectionSQLSnowFlake
