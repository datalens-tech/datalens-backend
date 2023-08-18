from bi_constants.enums import ConnectionType, SourceBackendType, CreateDSFrom


BACKEND_TYPE_POSTGRES = SourceBackendType.declare('POSTGRES')
CONNECTION_TYPE_POSTGRES = ConnectionType.postgres  # TODO: Move declaration here
SOURCE_TYPE_PG_TABLE = CreateDSFrom.PG_TABLE  # TODO: Move declaration here
SOURCE_TYPE_PG_SUBSELECT = CreateDSFrom.PG_SUBSELECT  # TODO: Move declaration here
