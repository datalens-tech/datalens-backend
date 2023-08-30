from __future__ import annotations

from typing import ClassVar, Any, Optional

import attr

from bi_api_connector.form_config.models.common import SerializableConfig, skip_if_null, InnerFieldName
from bi_api_connector.form_config.models.rows.base import FormRow


@attr.s(kw_only=True, frozen=True)
class PreparedRow(FormRow):
    type: ClassVar[str]

    class Inner(InnerFieldName):
        pass

    def as_dict(self) -> dict[str, Any]:
        return dict(
            type=self.type,
            **super().as_dict(),
        )


@attr.s(kw_only=True, frozen=True)
class DisabledMixin(SerializableConfig):
    disabled: Optional[bool] = attr.ib(default=None, metadata=skip_if_null())
