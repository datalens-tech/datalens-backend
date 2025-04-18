from __future__ import annotations

from typing import Optional

import attr

from dl_api_connector.form_config.models.common import (
    OAuthApplication,
    SerializableConfig,
    remap_skip_if_null,
)
from dl_api_connector.form_config.models.rows.base import (
    DisplayConditionsMixin,
    FormFieldMixin,
    InnerFieldMixin,
)
from dl_api_connector.form_config.models.rows.prepared.base import (
    DisabledMixin,
    PreparedRow,
)
from dl_api_connector.form_config.models.rows.customizable.components import (
    LabelRowItem,
    RadioGroupRowItem,
)


@attr.s(kw_only=True, frozen=True)
class OAuthTokenRow(PreparedRow, DisplayConditionsMixin, FormFieldMixin, DisabledMixin):
    type = "oauth"

    fake_value: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null("fakeValue"))

    application: OAuthApplication = attr.ib()
    label_text: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null("labelText"))
    button_text: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null("buttonText"))


@attr.s(kw_only=True, frozen=True)
class CacheTTLRow(PreparedRow, DisplayConditionsMixin, FormFieldMixin, DisabledMixin):
    type = "cache_ttl_sec"

    class Inner(PreparedRow.Inner):
        cache_ttl_mode = "cache_ttl_mode"

    label_text: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null("labelText"))


@attr.s(kw_only=True, frozen=True)
class CollapseRow(PreparedRow, DisplayConditionsMixin, FormFieldMixin, InnerFieldMixin):
    type = "collapse"

    @attr.s(kw_only=True, frozen=True)
    class Props(SerializableConfig):
        default_expanded: Optional[bool] = attr.ib(default=None, metadata=remap_skip_if_null("defaultIsExpand"))

    text: str = attr.ib()
    component_props: Optional[Props] = attr.ib(default=None, metadata=remap_skip_if_null("componentProps"))


@attr.s(kw_only=True, frozen=True)
class RawSqlLevelRow(PreparedRow, DisplayConditionsMixin, FormFieldMixin):
    type = "raw_sql_level"

    default_value: str = attr.ib(metadata=remap_skip_if_null("defaultValue"))
    switch_off_value: str = attr.ib(metadata=remap_skip_if_null("switchOffValue"))
    label: LabelRowItem = attr.ib()
    radio_group: RadioGroupRowItem = attr.ib(metadata=remap_skip_if_null("radioGroup"))
