from __future__ import annotations

from typing import Optional

import attr

from bi_connector_mdb_base.bi.form_config.models.common import MDBFieldName
from bi_connector_mdb_base.bi.form_config.models.rows.prepared import components as mdb_components

from bi_api_commons.base_models import TenantDef

import bi_api_connector.form_config.models.rows as C
from bi_api_connector.form_config.models.shortcuts.rows import RowConstructor
from bi_api_connector.form_config.models.api_schema import FormActionApiSchema, FormFieldApiSchema, FormApiSchema
from bi_api_connector.form_config.models.base import ConnectionFormFactory, ConnectionForm, ConnectionFormMode
from bi_api_connector.form_config.models.common import CommonFieldName, OAuthApplication
from bi_api_connector.form_config.models.rows.base import FormRow, DisplayConditionsMixin, FormFieldMixin
from bi_api_connector.form_config.models.rows.prepared.base import PreparedRow, DisabledMixin

from bi_connector_yql.bi.ydb.connection_info import YDBConnectionInfoProvider
from bi_connector_yql.core.ydb.settings import YDBConnectorSettings


class YDBOAuthApplication(OAuthApplication):
    ydb = 'ydb'


@attr.s(kw_only=True, frozen=True)
class YDBDatabaseRow(PreparedRow, DisplayConditionsMixin, FormFieldMixin, DisabledMixin):
    type = 'ydb_database'

    class Inner(PreparedRow.Inner):
        host = 'host'
        port = 'port'
        db_name = 'db_name'


class YDBConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
            self,
            connector_settings: Optional[ConnectorSettingsBase],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        assert connector_settings is not None and isinstance(connector_settings, YDBConnectorSettings)
        rc = RowConstructor(localizer=self._localizer)
        mdb_enabled = connector_settings.USE_MDB_CLUSTER_PICKER

        db_section_rows: list[FormRow]
        common_api_schema_items: list[FormFieldApiSchema]

        edit_api_schema = FormActionApiSchema(items=[
            FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
            FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
        ])

        if mdb_enabled:
            common_api_schema_items = [
                FormFieldApiSchema(name=YDBDatabaseRow.Inner.host, required=True),
                FormFieldApiSchema(name=YDBDatabaseRow.Inner.port, required=True),
                FormFieldApiSchema(name=YDBDatabaseRow.Inner.db_name, required=True),
            ]
            db_section_rows = [
                mdb_components.CloudTreeSelectRow(name=MDBFieldName.folder_id),
                mdb_components.ServiceAccountRow(name=MDBFieldName.service_account_id),
                YDBDatabaseRow(name=MDBFieldName.mdb_cluster_id),
            ]
            common_api_schema_items.extend([
                FormFieldApiSchema(name=MDBFieldName.service_account_id, required=True),
                FormFieldApiSchema(name=MDBFieldName.mdb_cluster_id, required=True),
            ])
            edit_api_schema.items.append(
                FormFieldApiSchema(name=MDBFieldName.folder_id, required=True)
            )
        else:
            common_api_schema_items = [
                FormFieldApiSchema(name=CommonFieldName.host, required=True),
                FormFieldApiSchema(name=CommonFieldName.port, required=True),
                FormFieldApiSchema(name=CommonFieldName.db_name, required=True),
            ]
            db_section_rows = [
                C.OAuthTokenRow(
                    name=CommonFieldName.token,
                    fake_value='******' if self.mode == ConnectionFormMode.edit else None,
                    application=YDBOAuthApplication.ydb,
                ),
                rc.host_row(default_value=connector_settings.DEFAULT_HOST_VALUE),
                rc.port_row(default_value='2135'),
                rc.db_name_row(),
            ]
            common_api_schema_items.append(
                FormFieldApiSchema(name=CommonFieldName.token, required=self.mode == ConnectionFormMode.create)
            )

        edit_api_schema.items.extend(common_api_schema_items)

        create_api_schema = FormActionApiSchema(items=[
            *edit_api_schema.items,
            *self._get_top_level_create_api_schema_items(),
        ])

        check_api_schema = FormActionApiSchema(items=[
            *common_api_schema_items,
            *self._get_top_level_check_api_schema_items(),
        ])

        return ConnectionForm(
            title=YDBConnectionInfoProvider.get_title(self._localizer),
            rows=[
                *db_section_rows,
                C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
                rc.raw_sql_level_row(),
            ],
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            )
        )
