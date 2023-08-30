from __future__ import annotations

from enum import Enum
from typing import Optional

import attr

from bi_api_connector.form_config.models.common import remap_skip_if_null, remap, SerializableConfig, OAuthApplication
from bi_api_connector.form_config.models.rows.base import DisplayConditionsMixin, FormFieldMixin, InnerFieldMixin
from bi_api_connector.form_config.models.rows.prepared.base import PreparedRow, DisabledMixin
from bi_constants.enums import ConnectionType


@attr.s(kw_only=True, frozen=True)
class OAuthTokenRow(PreparedRow, DisplayConditionsMixin, FormFieldMixin, DisabledMixin):
    type = 'oauth'

    fake_value: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null('fakeValue'))

    application: OAuthApplication = attr.ib()
    label_text: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null('labelText'))
    button_text: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null('buttonText'))


@attr.s(kw_only=True, frozen=True)
class OAuthTokenCHYTRow(PreparedRow, DisplayConditionsMixin, FormFieldMixin, DisabledMixin):  # TODO move to connector?
    type = 'oauth_chyt'

    fake_value: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null('fakeValue'))


@attr.s(kw_only=True, frozen=True)
class CacheTTLRow(PreparedRow, DisplayConditionsMixin, FormFieldMixin, DisabledMixin):
    type = 'cache_ttl_sec'

    class Inner(PreparedRow.Inner):
        cache_ttl_mode = 'cache_ttl_mode'

    label_text: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null('labelText'))


@attr.s(kw_only=True, frozen=True)
class CounterRow(PreparedRow, FormFieldMixin, DisabledMixin):
    label_text: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null('labelText'))
    allow_manual_input: Optional[bool] = attr.ib(default=None, metadata=remap_skip_if_null('allowManualInput'))

    class Inner(PreparedRow.Inner):
        counter_input_method = 'counter_input_method'


@attr.s(kw_only=True, frozen=True)
class MetricaCounterRowItem(CounterRow):
    type = 'metrica_counter'


@attr.s(kw_only=True, frozen=True)
class AppMetricaCounterRowItem(CounterRow):
    type = 'appmetrica_counter'


@attr.s(kw_only=True, frozen=True)
class CollapseRow(PreparedRow, DisplayConditionsMixin, FormFieldMixin, InnerFieldMixin):
    """ https://a.yandex-team.ru/arcadia/data-ui/common/src/components/Collapse """

    type = 'collapse'

    @attr.s(kw_only=True, frozen=True)
    class Props(SerializableConfig):
        default_expanded: Optional[bool] = attr.ib(default=None, metadata=remap_skip_if_null('defaultIsExpand'))

    text: str = attr.ib()
    component_props: Optional[Props] = attr.ib(default=None, metadata=remap_skip_if_null('componentProps'))


@attr.s(kw_only=True, frozen=True)
class AccuracyRow(PreparedRow, FormFieldMixin, DisabledMixin):
    type = 'metrica_accuracy'


@attr.s(kw_only=True, frozen=True)
class MDBFormFillRow(PreparedRow, DisplayConditionsMixin, DisabledMixin):
    type = 'mdb_form_fill'

    class Inner(PreparedRow.Inner):
        mdb_fill_mode = 'mdb_fill_mode'

    class Value(Enum):
        cloud = 'cloud'
        manually = 'manually'


@attr.s(kw_only=True, frozen=True)
class MDBClusterRow(PreparedRow, FormFieldMixin, DisplayConditionsMixin, DisabledMixin):
    type = 'mdb_cluster'

    class Inner(PreparedRow.Inner):
        sql_database_management = 'sql_database_management'
        sql_user_management = 'sql_user_management'

    db_type: ConnectionType = attr.ib(metadata=remap('dbType'))


@attr.s(kw_only=True, frozen=True)
class MDBFieldRow(PreparedRow, FormFieldMixin, DisplayConditionsMixin, DisabledMixin):
    db_type: ConnectionType = attr.ib(metadata=remap('dbType'))


@attr.s(kw_only=True, frozen=True)
class MDBHostRow(MDBFieldRow):
    type = 'mdb_host'


@attr.s(kw_only=True, frozen=True)
class MDBUsernameRow(MDBFieldRow):
    type = 'mdb_username'


@attr.s(kw_only=True, frozen=True)
class MDBDatabaseRow(MDBFieldRow):
    type = 'mdb_database'


@attr.s(kw_only=True, frozen=True)
class CloudTreeSelectRow(PreparedRow, DisplayConditionsMixin, FormFieldMixin, DisabledMixin):
    type = 'cloud_tree_select'


@attr.s(kw_only=True, frozen=True)
class ServiceAccountRow(PreparedRow, DisplayConditionsMixin, FormFieldMixin, DisabledMixin):
    type = 'service_account'
