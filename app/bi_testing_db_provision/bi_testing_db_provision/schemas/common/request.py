from marshmallow import fields

from bi_testing_db_provision.model.request_db import DBRequest
from bi_testing_db_provision.schemas.common.base import BaseSchema, GenericSchemaBase
from bi_testing_db_provision.schemas.common.elements import DBCredsSchema
from bi_testing_db_provision.schemas.common.fields import TupleField


class DBRequestSchema(BaseSchema[DBRequest]):
    TARGET_CLS = DBRequest

    version = fields.String(allow_none=True, load_default=None)
    db_names = TupleField(fields.String)
    creds = TupleField(fields.Nested(DBCredsSchema))


class GenericResourceRequestSchema(GenericSchemaBase):
    sub_schemas = {
        'resource_rq_db_v1': (DBRequestSchema, True),  # type: ignore  # TODO: fix
    }
