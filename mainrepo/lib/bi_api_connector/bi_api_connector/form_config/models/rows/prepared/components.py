from __future__ import annotations

from typing import Optional

import attr

from bi_api_connector.form_config.models.common import remap_skip_if_null, SerializableConfig, OAuthApplication
from bi_api_connector.form_config.models.rows.base import DisplayConditionsMixin, FormFieldMixin, InnerFieldMixin
from bi_api_connector.form_config.models.rows.prepared.base import PreparedRow, DisabledMixin


@attr.s(kw_only=True, frozen=True)
class OAuthTokenRow(PreparedRow, DisplayConditionsMixin, FormFieldMixin, DisabledMixin):
    type = 'oauth'

    fake_value: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null('fakeValue'))

    application: OAuthApplication = attr.ib()
    label_text: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null('labelText'))
    button_text: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null('buttonText'))


@attr.s(kw_only=True, frozen=True)
class CacheTTLRow(PreparedRow, DisplayConditionsMixin, FormFieldMixin, DisabledMixin):
    type = 'cache_ttl_sec'

    class Inner(PreparedRow.Inner):
        cache_ttl_mode = 'cache_ttl_mode'

    label_text: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null('labelText'))


@attr.s(kw_only=True, frozen=True)
class CollapseRow(PreparedRow, DisplayConditionsMixin, FormFieldMixin, InnerFieldMixin):
    type = 'collapse'

    @attr.s(kw_only=True, frozen=True)
    class Props(SerializableConfig):
        default_expanded: Optional[bool] = attr.ib(default=None, metadata=remap_skip_if_null('defaultIsExpand'))

    text: str = attr.ib()
    component_props: Optional[Props] = attr.ib(default=None, metadata=remap_skip_if_null('componentProps'))
