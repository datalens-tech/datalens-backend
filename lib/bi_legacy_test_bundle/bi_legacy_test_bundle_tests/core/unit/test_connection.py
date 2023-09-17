from __future__ import annotations

import pytest

from bi_core.us_connection import get_connection_class
from bi_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
from bi_connector_metrica.core.us_connection import MetrikaApiConnection
from bi_connector_mssql.core.us_connection import ConnectionMSSQL
from bi_connector_mysql.core.us_connection import ConnectionMySQL
from bi_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from bi_connector_oracle.core.us_connection import ConnectionSQLOracle
from bi_connector_clickhouse.core.clickhouse_base.connection_executors import ClickHouseAsyncAdapterConnExecutor
from bi_connector_clickhouse.core.clickhouse_base.conn_options import CHConnectOptions
from bi_connector_clickhouse.core.clickhouse_base.dto import ClickHouseConnDTO
from bi_core.connection_executors import ExecutionMode
from bi_core.connections_security.base import InsecureConnectionSecurityManager
from bi_core.mdb_utils import MDBDomainManagerFactory, MDBDomainManagerSettings

from bi_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL
from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL
from bi_connector_oracle.core.constants import CONNECTION_TYPE_ORACLE
from bi_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from bi_connector_metrica.core.constants import CONNECTION_TYPE_METRICA_API


def test_get_connection_class():
    assert get_connection_class(CONNECTION_TYPE_CLICKHOUSE) is ConnectionClickhouse
    assert get_connection_class(CONNECTION_TYPE_POSTGRES) is ConnectionPostgreSQL
    assert get_connection_class(CONNECTION_TYPE_MYSQL) is ConnectionMySQL
    assert get_connection_class(CONNECTION_TYPE_ORACLE) is ConnectionSQLOracle
    assert get_connection_class(CONNECTION_TYPE_MSSQL) is ConnectionMSSQL
    assert get_connection_class(CONNECTION_TYPE_METRICA_API) is MetrikaApiConnection


@pytest.mark.asyncio
async def test_managed_network(loop):
    ch_conn_dto_data = dict(
        host='example.mdb.cloud-preprod.yandex.net',
        port=8443,
        cluster_name='',
        conn_id=None,
        db_name='',
        multihosts='',
        endpoint='',
        password='',
        protocol='',
        username=''
    )

    executor = ClickHouseAsyncAdapterConnExecutor(
        conn_dto=ClickHouseConnDTO(**ch_conn_dto_data),
        conn_hosts_pool=tuple(['example.mdb.cloud-preprod.yandex.net']),
        conn_options=CHConnectOptions(),
        tpe=None,
        req_ctx_info=None,
        exec_mode=ExecutionMode.DIRECT,
        remote_qe_data=None,
        sec_mgr=InsecureConnectionSecurityManager(),
        mdb_mgr=MDBDomainManagerFactory(
            settings=MDBDomainManagerSettings(
                managed_network_enabled=True,
                mdb_domains=('.mdb.cloud-preprod.yandex.net',),
                mdb_cname_domains=tuple(),
                renaming_map={'.mdb.cloud-preprod.yandex.net': '.db.yandex.net'}
            )
        ).get_manager(),
        host_fail_callback=lambda: None,
    )

    target_dto_list = await executor._make_target_conn_dto_pool()
    assert target_dto_list[0].host == 'example.db.yandex.net'
