from typing import Optional

import attr

from bi_api_connector.form_config.models.common import remap_skip_if_null
from bi_api_connector.form_config.models.rows.base import FormFieldMixin
from bi_api_connector.form_config.models.rows.prepared.base import PreparedRow, DisabledMixin


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
