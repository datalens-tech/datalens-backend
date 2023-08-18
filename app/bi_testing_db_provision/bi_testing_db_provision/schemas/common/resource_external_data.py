from marshmallow import fields

from bi_testing_db_provision.model.resource_data_db import ResourceExternalDataDBDefaultSQL
from bi_testing_db_provision.schemas.common.base import BaseSchema, GenericSchemaBase
from bi_testing_db_provision.schemas.common.elements import DBCredsSchema
from bi_testing_db_provision.schemas.common.fields import TupleField


class ResourceExternalDataDBDefaultSQLSchema(BaseSchema):
    TARGET_CLS = ResourceExternalDataDBDefaultSQL

    db_host = fields.String()
    db_port = fields.Integer()
    db_names = TupleField(fields.String)
    creds = TupleField(fields.Nested(DBCredsSchema))


class GenericResourceExternalDataSchema(GenericSchemaBase):
    sub_schemas = {
        'resource_ed_db_default_sql_v1': (ResourceExternalDataDBDefaultSQLSchema, True),  # type: ignore  # TODO: fix
    }
