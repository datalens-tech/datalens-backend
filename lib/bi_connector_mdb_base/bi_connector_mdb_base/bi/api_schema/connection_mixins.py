from __future__ import annotations

import marshmallow
from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.extras import FieldExtra


class MDBDatabaseSchemaMixin(marshmallow.Schema):
    mdb_cluster_id = ma_fields.String(
        attribute='data.mdb_cluster_id',
        allow_none=True,
        bi_extra=FieldExtra(editable=True),
    )
    mdb_folder_id = ma_fields.String(
        attribute='data.mdb_folder_id',
        allow_none=True,
        # dump_only=True,   # TODO: uncomment after all MDB connectors migration to MDBConnectionMixin
        bi_extra=FieldExtra(editable=True),
    )
