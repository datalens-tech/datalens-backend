import pkgutil
from sqlalchemy.dialects import registry

from clickhouse_sqlalchemy.util import compat

from . import log

if compat.PY3:
    import configparser
else:
    import ConfigParser as configparser


registry.register(
    "clickhouse", "clickhouse_sqlalchemy.drivers.http.base", "dialect"
)
registry.register(
    "clickhouse.http", "clickhouse_sqlalchemy.drivers.http.base", "dialect"
)
registry.register(
    "clickhouse.native", "clickhouse_sqlalchemy.drivers.native.base", "dialect"
)

file_config = configparser.ConfigParser()
# file_config.read(['setup.cfg'])
setup_cfg_data = pkgutil.get_data(
    __package__ or __name__.rsplit('.', 1)[0],
    'setup.cfg')
file_config.read_string(setup_cfg_data.decode('utf-8'))

log.configure(file_config.get('log', 'level'))

host = file_config.get('db', 'host')
port = file_config.getint('db', 'port')
http_port = file_config.getint('db', 'http_port')
database = file_config.get('db', 'database')
user = file_config.get('db', 'user')
password = file_config.get('db', 'password')

uri_template = '{schema}://{user}:{password}@{host}:{port}/{database}'

http_uri = uri_template.format(
    schema='clickhouse+http', user=user, password=password, host=host,
    port=http_port, database=database,
)
http_tsv_uri = http_uri
http_jc_uri = http_uri + '?format=JSONCompact'

uri = http_uri

native_uri = uri_template.format(
    schema='clickhouse+native', user=user, password=password, host=host,
    port=port, database=database)

system_http_uri = uri_template.format(
    schema='clickhouse+http', user=user, password=password, host=host,
    port=http_port, database='system')
system_native_uri = uri_template.format(
    schema='clickhouse+native', user=user, password=password, host=host,
    port=port, database='system')
