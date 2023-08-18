from sqlalchemy.engine.url import URL

from bi_configs.utils import ROOT_CERTIFICATES_FILENAME
from bi_testing_db_provision.common_config_models import PGConfig


def create_sa_engine_url(pg_config: PGConfig) -> URL:
    return URL(
        drivername='postgresql',
        username=pg_config.user,
        password=pg_config.password,
        host=','.join(pg_config.host_list),
        port=pg_config.port,
        database=pg_config.database,
        query={
            'sslmode': pg_config.ssl_mode,
            'sslrootcert': ROOT_CERTIFICATES_FILENAME,
            'target_session_attrs': 'read-write',
        }
    )
