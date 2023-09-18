from __future__ import annotations

from typing import Optional

from dl_api_commons.base_models import TenantDef
from dl_configs.connectors_settings import ConnectorSettingsBase

import dl_api_connector.form_config.models.rows as C
from dl_api_connector.form_config.models.base import ConnectionFormFactory, ConnectionForm, ConnectionFormMode
from dl_api_connector.form_config.models.shortcuts.rows import RowConstructor
from dl_api_connector.form_config.models.common import CommonFieldName
from dl_api_connector.form_config.models.api_schema import FormApiSchema, FormFieldApiSchema, FormActionApiSchema

from bi_connector_chyt_internal.bi.connection_info import CHYTInternalTokenConnectionInfoProvider
from bi_connector_chyt_internal.bi.connection_form.components import OAuthTokenCHYTRow
from bi_connector_chyt_internal.bi.connection_form.components import cluster_row, clique_alias_row,\
    CHYTInternalFieldName


class CHYTInternalTokenConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
            self,
            connector_settings: Optional[ConnectorSettingsBase],
            tenant: Optional[TenantDef]
    ) -> ConnectionForm:

        rc = RowConstructor(localizer=self._localizer)

        common_api_schema_items: list[FormFieldApiSchema] = [
            FormFieldApiSchema(name=CHYTInternalFieldName.alias, required=True),
            FormFieldApiSchema(name=CHYTInternalFieldName.cluster, required=True),
            FormFieldApiSchema(name=CommonFieldName.token, required=self.mode == ConnectionFormMode.create),

        ]

        edit_api_schema = FormActionApiSchema(items=[
            *common_api_schema_items,
            FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
            FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
            FormFieldApiSchema(name=CommonFieldName.data_export_forbidden),

        ])

        create_api_schema = FormActionApiSchema(items=[
            *edit_api_schema.items,
            *self._get_top_level_create_api_schema_items(),
        ])

        check_api_schema = FormActionApiSchema(items=[
            *common_api_schema_items,
            *self._get_top_level_check_api_schema_items(),
        ])

        return ConnectionForm(
            title=CHYTInternalTokenConnectionInfoProvider.get_title(self._localizer),
            rows=[
                OAuthTokenCHYTRow(
                    name=CommonFieldName.token,
                    fake_value='******' if self.mode == ConnectionFormMode.edit else None
                ),
                cluster_row(self._localizer),
                clique_alias_row(self._localizer, self.mode),
                C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
                rc.raw_sql_level_row(),
                rc.collapse_advanced_settings_row(),
                rc.data_export_forbidden_row(),
            ],
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
        )
