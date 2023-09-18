import logging

from dl_connector_snowflake.core.us_connection import ConnectionSQLSnowFlake
from dl_core.connectors.base.lifecycle import ConnectionLifecycleManager

LOGGER = logging.getLogger(__name__)


class SnowFlakeConnectionLifecycleManager(ConnectionLifecycleManager[ConnectionSQLSnowFlake]):
    ENTRY_CLS = ConnectionSQLSnowFlake
