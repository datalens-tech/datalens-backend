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
from dl_constants.enums import RawSQLLevel
from dl_core.connectors.settings.base import ConnectorSettings

from dl_connector_chyt.api.connection_info import CHYTConnectionInfoProvider
from dl_connector_chyt.api.i18n.localizer import Translatable
from dl_connector_chyt.core.settings import CHYTConnectorSettings


@unique
class CHYTFieldName(FormFieldName):
    alias = "alias"


class CHYTConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
        self,
        connector_settings: Optional[ConnectorSettings],
        tenant: Optional[TenantDef],
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
                FormFieldApiSchema(name=CommonFieldName.cache_invalidation_throttling_interval_sec, nullable=True),
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

        raw_sql_levels = [RawSQLLevel.subselect, RawSQLLevel.dashsql]
        if connector_settings.ENABLE_DATASOURCE_TEMPLATE:
            raw_sql_levels.append(RawSQLLevel.template)

        form_params = self._get_form_params()
        is_invalidation_cache_enabled = form_params.feature_flags.is_invalidation_cache_enabled

        return ConnectionForm(
            title=CHYTConnectionInfoProvider.get_title(self._localizer),
            rows=self._filter_nulls(
                [
                    rc.host_row(),
                    rc.port_row(),
                    clique_alias_row,
                    token_row,
                    C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec) if not is_invalidation_cache_enabled else None,
                    rc.raw_sql_level_row_v2(raw_sql_levels=raw_sql_levels),
                    *(rc.cache_rows() if is_invalidation_cache_enabled else []),
                    secure_row,
                    rc.collapse_advanced_settings_row(),
                    rc.data_export_forbidden_row(
                        conn_id=form_params.conn_id,
                        exports_history_url_path=form_params.exports_history_url_path,
                        mode=self.mode,
                    ),
                ]
            ),
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
        )
