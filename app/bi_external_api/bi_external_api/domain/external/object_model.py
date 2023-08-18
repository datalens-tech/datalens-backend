import enum
from typing import ClassVar

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.enums import ExtAPIType


class ObjectParentKind(enum.Enum):
    # collection = 'collection'
    project = 'project'
    organization = 'organization'


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True)
class ObjectParent:
    kind: ClassVar[ObjectParentKind]


@ModelDescriptor(api_types=[ExtAPIType.UNIFIED_DC])
@attr.s(frozen=True)
class ParentProject(ObjectParent):
    kind = ObjectParentKind.project

    project_id: str = attr.ib()


@ModelDescriptor(api_types=[ExtAPIType.UNIFIED_NEBIUS_IL])
@attr.s(frozen=True)
class ParentOrganization(ObjectParent):
    kind = ObjectParentKind.organization

    org_id: str = attr.ib()
