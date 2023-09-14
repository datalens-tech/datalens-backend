from __future__ import annotations

from enum import unique
from typing import Optional

from bi_api_commons.base_models import TenantDef
from bi_api_connector.form_config.models.api_schema import (
    FormActionApiSchema,
    FormApiSchema,
    FormFieldApiSchema,
)
from bi_api_connector.form_config.models.base import (
    ConnectionForm,
    ConnectionFormFactory,
    ConnectionFormMode,
)
from bi_api_connector.form_config.models.common import (
    CommonFieldName,
    FormFieldName,
)
import bi_api_connector.form_config.models.rows as C
from bi_api_connector.form_config.models.shortcuts.rows import RowConstructor
from bi_configs.connectors_settings import ConnectorSettingsBase

from bi_connector_chyt.bi.connection_info import CHYTConnectionInfoProvider
from bi_connector_chyt.bi.i18n.localizer import Translatable
from bi_connector_chyt.core.settings import CHYTConnectorSettings


@unique
class CHYTFieldName(FormFieldName):
    alias = "alias"


class CHYTConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
        self, connector_settings: Optional[ConnectorSettingsBase], tenant: Optional[TenantDef]
    ) -> ConnectionForm:
        assert connector_settings is not None and isinstance(connector_settings, CHYTConnectorSettings)

        rc = RowConstructor(localizer=self._localizer)

        clique_alias_row = C.CustomizableRow(
            items=[
                C.LabelRowItem(text=self._localizer.translate(Translatable("field_clique-alias"))),
                C.InputRowItem(name=CHYTFieldName.alias, width="l", default_value=connector_settings.DEFAULT_CLIQUE),
            ]
        )

        token_row = C.CustomizableRow(
            items=[
                C.LabelRowItem(text=self._localizer.translate(Translatable("field_chyt-token"))),
                C.InputRowItem(
                    name=CommonFieldName.token,
                    width="m",
                    default_value="" if self.mode == ConnectionFormMode.create else None,
                    fake_value="******" if self.mode == ConnectionFormMode.edit else None,
                    control_props=C.InputRowItem.Props(type="password"),
                ),
            ]
        )

        secure_row = C.CustomizableRow(
            items=[
                C.CheckboxRowItem(name=CommonFieldName.secure, text="HTTPS", default_value=False),
            ]
        )

        common_api_schema_items: list[FormFieldApiSchema] = [
            FormFieldApiSchema(name=CommonFieldName.host, required=True),
            FormFieldApiSchema(name=CommonFieldName.port, required=True),
            FormFieldApiSchema(name=CHYTFieldName.alias, required=True),
            FormFieldApiSchema(name=CommonFieldName.token, required=self.mode == ConnectionFormMode.create),
            FormFieldApiSchema(name=CommonFieldName.secure, type="boolean"),
            FormFieldApiSchema(name=CommonFieldName.data_export_forbidden),
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
            title=CHYTConnectionInfoProvider.get_title(self._localizer),
            rows=[
                rc.host_row(),
                rc.port_row(),
                clique_alias_row,
                token_row,
                C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
                rc.raw_sql_level_row(),
                secure_row,
                rc.collapse_advanced_settings_row(),
                rc.data_export_forbidden_row(),
            ],
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
        )
