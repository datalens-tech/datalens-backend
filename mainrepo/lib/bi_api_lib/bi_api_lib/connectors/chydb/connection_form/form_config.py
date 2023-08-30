from __future__ import annotations

from enum import unique
from typing import Optional

from bi_configs.connectors_settings import ConnectorSettingsBase

from bi_api_commons.base_models import TenantDef

import bi_api_connector.form_config.models.rows as C
from bi_api_connector.form_config.models.shortcuts.rows import RowConstructor
from bi_api_connector.form_config.models.api_schema import FormActionApiSchema, FormFieldApiSchema, FormApiSchema
from bi_api_connector.form_config.models.base import ConnectionFormFactory, ConnectionForm, ConnectionFormMode
from bi_api_connector.form_config.models.common import CommonFieldName, FormFieldName, OAuthApplication

from bi_api_lib.connectors.chydb.connection_info import CHYDBConnectionInfoProvider
from bi_api_lib.i18n.localizer import Translatable


class CHYDBOAuthApplication(OAuthApplication):
    chydb = 'chydb'


@unique
class CHYDBFieldName(FormFieldName):
    default_ydb_cluster = 'default_ydb_cluster'
    default_ydb_database = 'default_ydb_database'


class CHYDBConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
            self,
            connector_settings: Optional[ConnectorSettingsBase],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        rc = RowConstructor(localizer=self._localizer)

        oauth_row = C.OAuthTokenRow(
            name=CommonFieldName.token,
            application=CHYDBOAuthApplication.chydb,
            fake_value='******' if self.mode == ConnectionFormMode.edit else None,
        )

        host_row = rc.host_row(
            default_value='ydb-clickhouse.yandex.net',
            label_help_text=self._localizer.translate(Translatable('label_ydb-host-tooltip')),
        )

        default_cluster_row = C.CustomizableRow(items=[
            C.LabelRowItem(text=self._localizer.translate(Translatable('field_default-cluster'))),
            C.SelectRowItem(
                name=CHYDBFieldName.default_ydb_cluster,
                width='m',
                available_values=[
                    C.SelectOption(content='ru', value='ru'),
                    C.SelectOption(content='ru-prestable', value='ru-prestable'),
                    C.SelectOption(content='eu', value='eu'),
                ],
                default_value='ru',
                control_props=C.SelectRowItem.Props(show_search=False),
            )
        ])

        default_db_name_row = C.CustomizableRow(items=[
            C.LabelRowItem(text=self._localizer.translate(Translatable('field_default-database'))),
            C.InputRowItem(name=CHYDBFieldName.default_ydb_database, width='m', default_value=''),
        ])

        port_row = rc.port_row(default_value='8123')

        cache_ttl_row = C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec)

        raw_sql_level_row = rc.raw_sql_level_row()

        secure_row = C.CustomizableRow(items=[
            C.CheckboxRowItem(name=CommonFieldName.secure, text='HTTPS', default_value=False),
        ])

        edit_api_schema = FormActionApiSchema(items=[
            FormFieldApiSchema(name=CommonFieldName.token, required=self.mode == ConnectionFormMode.create),
            FormFieldApiSchema(name=CommonFieldName.host, required=True),
            FormFieldApiSchema(name=CommonFieldName.port, required=True),
            FormFieldApiSchema(name=CHYDBFieldName.default_ydb_cluster),
            FormFieldApiSchema(name=CHYDBFieldName.default_ydb_database),
            FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
            FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
            FormFieldApiSchema(name=CommonFieldName.secure, type='boolean'),
        ])

        create_api_schema = FormActionApiSchema(items=[
            *edit_api_schema.items,
            *self._get_top_level_create_api_schema_items(),
        ])

        check_api_schema = FormActionApiSchema(items=[
            FormFieldApiSchema(name=CommonFieldName.token, required=self.mode == ConnectionFormMode.create),
            FormFieldApiSchema(name=CommonFieldName.host, required=True),
            FormFieldApiSchema(name=CommonFieldName.port, required=True),
            *self._get_top_level_check_api_schema_items(),
        ])

        return ConnectionForm(
            title=CHYDBConnectionInfoProvider.get_title(self._localizer),
            rows=[
                oauth_row,
                host_row,
                default_cluster_row,
                default_db_name_row,
                port_row,
                cache_ttl_row,
                raw_sql_level_row,
                secure_row,
            ],
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
        )
