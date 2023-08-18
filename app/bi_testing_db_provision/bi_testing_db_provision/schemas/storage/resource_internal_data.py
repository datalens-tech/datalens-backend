from marshmallow import fields

from bi_testing_db_provision.model.resource_docker import DockerResourceInternalData
from bi_testing_db_provision.schemas.common.base import BaseSchema, GenericSchemaBase


class DockerResourceInternalDataSchema(BaseSchema):
    TARGET_CLS = DockerResourceInternalData

    allocation_host = fields.String(required=True)
    container_name_map = fields.Dict(keys=fields.String(), values=fields.String(), required=True)


class GenericResourceInternalDataSchema(GenericSchemaBase):
    sub_schemas = {
        'resource_id_docker_v1': (DockerResourceInternalDataSchema, True),  # type: ignore  # TODO: fix
    }
