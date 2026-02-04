from typing import (
    Any,
    Sequence,
)

import attr

from dl_attrs_model_mapper.domain import AmmRegularSchema
from dl_attrs_model_mapper.utils import MText


@attr.s(kw_only=True, auto_attribs=True)
class AmmOperationExample:
    title: MText | None = None
    description: MText | None = None
    rq: dict[str, Any]
    rs: dict[str, Any]


@attr.s(kw_only=True)
class AmmOperation:
    description: MText = attr.ib()

    code: str = attr.ib()
    discriminator_attr_name: str = attr.ib()

    amm_schema_rq: AmmRegularSchema = attr.ib()
    amm_schema_rs: AmmRegularSchema = attr.ib()

    examples: Sequence[AmmOperationExample] = attr.ib()
