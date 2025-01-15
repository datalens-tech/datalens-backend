from dl_core.connectors.base.error_transformer import (
    ChainedDbErrorTransformer,
    DbErrorTransformer,
)


trino_error_transformer: DbErrorTransformer = ChainedDbErrorTransformer([])
