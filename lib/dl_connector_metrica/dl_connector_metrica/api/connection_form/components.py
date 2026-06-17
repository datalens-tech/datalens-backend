from enum import unique

from dl_api_connector.form_config.models.base import ConnectionFormMode
from dl_api_connector.form_config.models.common import (
    CommonFieldName,
    FormFieldName,
)
from dl_api_connector.form_config.models.rows import (
    CustomizableRow,
    InputRowItem,
    LabelRowItem,
)
from dl_i18n.localizer_base import Localizer

from dl_connector_metrica.api.i18n.localizer import Translatable


@unique
class MetricaFieldName(FormFieldName):
    counter_id = "counter_id"
    accuracy = "accuracy"


def oauth_token_row(localizer: Localizer, mode: ConnectionFormMode) -> CustomizableRow:
    return CustomizableRow(
        items=[
            LabelRowItem(
                text=localizer.translate(
                    Translatable("field_oauth-token"),
                ),
            ),
            InputRowItem(
                name=CommonFieldName.token,
                width="l",
                default_value=None,
                fake_value="******" if mode == ConnectionFormMode.edit else None,
                control_props=InputRowItem.Props(type="password"),
            ),
        ]
    )
