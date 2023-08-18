from typing import ClassVar

from bi_core_testing.configuration import DefaultCoreTestConfiguration
from bi_connector_mysql.formula.constants import MySQLDialect as D
from bi_testing.containers import get_test_container_hostport

from bi_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration


# Infra settings
CORE_TEST_CONFIG = DefaultCoreTestConfiguration(
    host_us_http=get_test_container_hostport('us', fallback_port=52011).host,
    port_us_http=get_test_container_hostport('us', fallback_port=52011).port,
    host_us_pg=get_test_container_hostport('pg-us', fallback_port=52010).host,
    port_us_pg_5432=get_test_container_hostport('pg-us', fallback_port=52010).port,
    us_master_token='AC1ofiek8coB',
)


class CoreConnectionSettings:
    DB_NAME: ClassVar[str] = 'test_data'
    HOST: ClassVar[str] = get_test_container_hostport('db-mysql-8-0', fallback_port=52001).host
    PORT: ClassVar[int] = get_test_container_hostport('db-mysql-8-0', fallback_port=52001).port
    USERNAME: ClassVar[str] = 'datalens'
    PASSWORD: ClassVar[str] = 'qwerty'


SUBSELECT_QUERY_FULL = r"""
select
    number as num,  -- `int(1)`
    cast(number as unsigned) as num_unsigned,  -- `int(1) unsigned`
    cast(number as signed) as num_signed,  -- `int(1)`
    cast(number as decimal) as num_decimal,  -- `decimal(10,0)`
    cast(number as decimal(12, 2)) as num_decimal_12_2,  -- `decimal(12,2)`
    concat('test', number) as num_text, -- `varchar(5)`
    BINARY number as num_binary,  -- `varbinary(1)`
    cast(number as char) as num_char,  -- `varchar(1)``
    cast(number as date) as num_date,  -- `date`
    cast(number as datetime) as num_datetime,  -- `datetime`
    cast(number as nchar) as num_nchar,  -- `varchar(1)`
    cast(number as time) as num_time, -- `time`
    1073741824 as int_30bit,  -- `bigint(10)`
    pow(2, 15) as some_double  -- `double`
from (
    select 0 as number
    union all
    select 1 as number
    union all
    select 6 as number
) as sq
"""

DB_URLS = {
    # “Mysql uses sockets when the host is 'localhost' and tcp/ip when the host is anything else”
    D.MYSQL_5_6: f'mysql://datalens:qwerty@{get_test_container_hostport("db-mysql-5-6", fallback_port=52000).as_pair()}/test_data?charset=utf8',
    D.MYSQL_8_0_12: f'mysql://datalens:qwerty@{get_test_container_hostport("db-mysql-8-0", fallback_port=52001).as_pair()}/test_data?charset=utf8',
}
DB_CORE_URL = DB_URLS[D.MYSQL_8_0_12]

BI_TEST_CONFIG = BiApiTestEnvironmentConfiguration(
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key='_some_test_secret_key_',
)
