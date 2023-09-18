from typing import Sequence

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.domain.utils import ensure_tuple

from ..dl_common import DatasetAPIBaseModel


@ModelDescriptor()
@attr.s(frozen=True)
class ComponentError(DatasetAPIBaseModel):
    message: str = attr.ib()
    code: str = attr.ib()

    ignore_not_declared_fields = True


@ModelDescriptor()
@attr.s(frozen=True)
class SingleComponentErrorContainer(DatasetAPIBaseModel):
    type: str = attr.ib()
    id: str = attr.ib()
    errors: Sequence[ComponentError] = attr.ib(converter=ensure_tuple)

    ignore_not_declared_fields = True


@ModelDescriptor()
@attr.s(frozen=True)
class AllComponentErrors(DatasetAPIBaseModel):
    items: Sequence[SingleComponentErrorContainer] = attr.ib(converter=ensure_tuple, factory=tuple)

    ignore_not_declared_fields = True
