from typing import Optional

import attr

from dl_api_connector.form_config.models.common import remap
from dl_api_connector.form_config.models.rows.base import FormFieldMixin
from dl_api_connector.form_config.models.rows.prepared.base import (
    DisabledMixin,
    PreparedRow,
)


@attr.s(kw_only=True, frozen=True)
class CounterRow(PreparedRow, FormFieldMixin, DisabledMixin):
    label_text: Optional[str] = attr.ib(default=None, metadata=remap("labelText"))
    allow_manual_input: Optional[bool] = attr.ib(default=None, metadata=remap("allowManualInput"))

    class Inner(PreparedRow.Inner):
        counter_input_method = "counter_input_method"


@attr.s(kw_only=True, frozen=True)
class MetricaCounterRowItem(CounterRow):
    type = "metrica_counter"


@attr.s(kw_only=True, frozen=True)
class AppMetricaCounterRowItem(CounterRow):
    type = "appmetrica_counter"


@attr.s(kw_only=True, frozen=True)
class AccuracyRow(PreparedRow, FormFieldMixin, DisabledMixin):
    type = "metrica_accuracy"
