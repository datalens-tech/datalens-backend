from marshmallow import fields as ma_fields


class ConnectionMDBStorageDataSchemaMixin:
    mdb_cluster_id = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    mdb_folder_id = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    is_verified_mdb_org = ma_fields.Boolean(required=False, load_default=False, dump_default=False)
    skip_mdb_org_check = ma_fields.Boolean(
        required=False,
        load_default=True,  # !!! For backwards compatibility consider all the old connections are verified.
        dump_default=False,
    )
