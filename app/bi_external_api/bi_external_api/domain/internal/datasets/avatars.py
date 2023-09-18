from typing import ClassVar

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor

from ..dl_common.base import DatasetAPIBaseModel


@ModelDescriptor()
@attr.s(frozen=True)
class Avatar(DatasetAPIBaseModel):
    id: str = attr.ib()
    is_root: bool = attr.ib()
    source_id: str = attr.ib()
    title: str = attr.ib()

    ignored_keys: ClassVar[set[str]] = {
        "valid",
        "virtual",
        "managed_by",
    }
