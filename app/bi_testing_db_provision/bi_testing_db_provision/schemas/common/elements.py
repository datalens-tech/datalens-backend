from marshmallow import fields

from bi_testing_db_provision.model.commons_db import DBCreds
from bi_testing_db_provision.schemas.common.base import BaseSchema


class DBCredsSchema(BaseSchema[DBCreds]):
    TARGET_CLS = DBCreds

    username = fields.String()
    password = fields.String()
