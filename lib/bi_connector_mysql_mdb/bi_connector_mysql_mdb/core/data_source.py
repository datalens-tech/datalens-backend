from __future__ import annotations

from bi_connector_mysql.core.data_source import MySQLDataSource, MySQLSubselectDataSource


class MySQLMDBDataSource(MySQLDataSource):
    """ MDB MySQL table """


class MySQLMDBSubselectDataSource(MySQLSubselectDataSource):
    """ MDB MySQL table from a subquery """
