from dl_connector_greenplum.core.constants import BACKEND_TYPE_GREENPLUM
from dl_connector_postgresql.core.postgresql_base.sa_types import SQLALCHEMY_POSTGRES_TYPES


SQLALCHEMY_GP_TYPES = {(BACKEND_TYPE_GREENPLUM, nt): sa_type for (_, nt), sa_type in SQLALCHEMY_POSTGRES_TYPES.items()}
