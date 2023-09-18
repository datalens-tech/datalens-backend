from __future__ import annotations

from enum import unique
from typing import Optional

from dl_api_commons.base_models import TenantDef
from dl_api_connector.form_config.models.api_schema import (
    FormActionApiSchema,
    FormApiSchema,
    FormFieldApiSchema,
)
from dl_api_connector.form_config.models.base import (
    ConnectionForm,
    ConnectionFormFactory,
    ConnectionFormMode,
)
from dl_api_connector.form_config.models.common import (
    CommonFieldName,
    FormFieldName,
)
import dl_api_connector.form_config.models.rows as C
from dl_api_connector.form_config.models.shortcuts.rows import RowConstructor
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_connector_bigquery.bi.connection_info import BigQueryConnectionInfoProvider
from dl_connector_bigquery.bi.i18n.localizer import Translatable
from dl_constants.enums import RawSQLLevel


@unique
class BigQueryFieldName(FormFieldName):
    project_id = "project_id"
    credentials = "credentials"


class BigQueryConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
        self,
        connector_settings: Optional[ConnectorSettingsBase],
        tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        rc = RowConstructor(localizer=self._localizer)

        common_api_schema_items: list[FormFieldApiSchema] = [
            FormFieldApiSchema(name=BigQueryFieldName.project_id, required=True),
            FormFieldApiSchema(name=BigQueryFieldName.credentials, required=self.mode == ConnectionFormMode.create),
        ]

        edit_api_schema = FormActionApiSchema(
            items=[
                *common_api_schema_items,
                FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
            ]
        )

        create_api_schema = FormActionApiSchema(
            items=[
                *edit_api_schema.items,
                *self._get_top_level_create_api_schema_items(),
            ]
        )

        check_api_schema = FormActionApiSchema(
            items=[
                *common_api_schema_items,
                *self._get_top_level_check_api_schema_items(),
            ]
        )

        return ConnectionForm(
            title=BigQueryConnectionInfoProvider.get_title(self._localizer),
            rows=[
                C.CustomizableRow(
                    items=[
                        C.LabelRowItem(text=self._localizer.translate(Translatable("label_project-id"))),
                        C.InputRowItem(name=BigQueryFieldName.project_id, width="m"),
                    ]
                ),
                C.CustomizableRow(
                    items=[
                        C.LabelRowItem(text=self._localizer.translate(Translatable("label_service-acc-key-file"))),
                        C.FileInputRowItem(name=BigQueryFieldName.credentials),
                    ]
                ),
                rc.raw_sql_level_row(
                    options=[
                        rc.raw_sql_level_to_radio_group_option(RawSQLLevel.off),
                        rc.raw_sql_level_to_radio_group_option(RawSQLLevel.subselect),
                    ],
                ),
                C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
            ],
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
        )
