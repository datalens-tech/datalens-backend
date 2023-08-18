from __future__ import annotations

from enum import unique
from typing import Optional

from bi_configs.connectors_settings import ConnectorsSettingsByType

from bi_api_commons.base_models import TenantDef

from bi_connector_postgresql.core.postgresql_base.constants import PGEnforceCollateMode
from bi_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES

import bi_api_connector.form_config.models.rows as C
from bi_api_connector.form_config.models.shortcuts.rows import RowConstructor
from bi_api_connector.form_config.models.api_schema import (
    FormActionApiSchema, FormFieldApiAction, FormFieldApiSchema, FormFieldApiActionCondition, FormFieldSelector,
    FormFieldConditionalApiAction, FormApiSchema)
from bi_api_connector.form_config.models.base import ConnectionFormFactory, ConnectionForm, ConnectionFormMode
from bi_api_connector.form_config.models.common import CommonFieldName, FormFieldName
from bi_api_connector.form_config.models.rows.base import FormRow
from bi_api_connector.form_config.models.shortcuts.mdb import get_db_host_section

from bi_api_lib.connectors.postgresql.connection_info import PostgreSQLConnectionInfoProvider
from bi_api_lib.i18n.localizer import Translatable


@unique
class PostgreSQLFieldName(FormFieldName):
    enforce_collate = 'enforce_collate'


class PostgreSQLConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
            self,
            connectors_settings: Optional[ConnectorsSettingsByType],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        assert connectors_settings is not None and connectors_settings.POSTGRES is not None
        rc = RowConstructor(localizer=self._localizer)
        mdb_enabled = connectors_settings.POSTGRES.USE_MDB_CLUSTER_PICKER

        mdb_form_fill_row = C.MDBFormFillRow()

        host_section: list[FormRow]
        db_name_section: list[FormRow]
        username_section: list[FormRow]

        cloud_tree_selector_row, mdb_cluster_row, mdb_host_row = None, None, None
        if mdb_enabled:
            cloud_tree_selector_row, mdb_cluster_row, mdb_host_row = get_db_host_section(
                is_org=self._is_current_tenant_with_org(tenant),
                db_type=CONNECTION_TYPE_POSTGRES,
            )
            host_section = self._filter_nulls([
                cloud_tree_selector_row,
                mdb_cluster_row,
                mdb_host_row,
                rc.host_row(display_conditions={C.MDBFormFillRow.Inner.mdb_fill_mode: C.MDBFormFillRow.Value.manually}),
            ])

            username_section = [
                C.MDBUsernameRow(
                    name=CommonFieldName.username,
                    db_type=CONNECTION_TYPE_POSTGRES,
                    display_conditions={C.MDBFormFillRow.Inner.mdb_fill_mode: C.MDBFormFillRow.Value.cloud},
                ),
                rc.username_row(
                    display_conditions={C.MDBFormFillRow.Inner.mdb_fill_mode: C.MDBFormFillRow.Value.manually}
                )
            ]

            db_name_section = [
                C.MDBDatabaseRow(
                    name=CommonFieldName.db_name,
                    db_type=CONNECTION_TYPE_POSTGRES,
                    display_conditions={C.MDBFormFillRow.Inner.mdb_fill_mode: C.MDBFormFillRow.Value.cloud},
                ),
                rc.db_name_row(
                    display_conditions={C.MDBFormFillRow.Inner.mdb_fill_mode: C.MDBFormFillRow.Value.manually},
                ),
            ]
        else:
            host_section = [rc.host_row()]
            username_section = [rc.username_row()]
            db_name_section = [rc.db_name_row()]

        edit_api_schema = FormActionApiSchema(
            items=[
                FormFieldApiSchema(name=CommonFieldName.host, required=True),
                FormFieldApiSchema(name=CommonFieldName.port, required=True),
                FormFieldApiSchema(name=CommonFieldName.username, required=True),
                FormFieldApiSchema(name=CommonFieldName.db_name, required=True),
                FormFieldApiSchema(name=CommonFieldName.password, required=self.mode == ConnectionFormMode.create),
                FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
                FormFieldApiSchema(name=PostgreSQLFieldName.enforce_collate),
                FormFieldApiSchema(name=CommonFieldName.ssl_enable),
                FormFieldApiSchema(name=CommonFieldName.ssl_ca),
                FormFieldApiSchema(name=CommonFieldName.data_export_forbidden),
            ],
        )
        if mdb_enabled:
            assert mdb_cluster_row is not None
            edit_api_schema.items.extend(self._filter_nulls([
                FormFieldApiSchema(name=mdb_cluster_row.name, default_action=FormFieldApiAction.skip, required=True),
                FormFieldApiSchema(
                    name=cloud_tree_selector_row.name,
                    default_action=FormFieldApiAction.skip,
                    nullable=True
                ) if cloud_tree_selector_row is not None else None,
            ]))
            edit_api_schema.conditions.append(
                FormFieldApiActionCondition(
                    when=FormFieldSelector(name=C.MDBFormFillRow.Inner.mdb_fill_mode),
                    equals=C.MDBFormFillRow.Value.cloud,
                    then=self._filter_nulls([
                        FormFieldConditionalApiAction(
                            selector=FormFieldSelector(name=mdb_cluster_row.name),
                            action=FormFieldApiAction.include,
                        ),
                        FormFieldConditionalApiAction(
                            selector=FormFieldSelector(name=cloud_tree_selector_row.name),
                            action=FormFieldApiAction.include,
                        ) if cloud_tree_selector_row is not None else None,
                    ])
                )
            )

        create_api_schema = FormActionApiSchema(
            items=[
                *edit_api_schema.items,
                *self._get_top_level_create_api_schema_items(),
            ],
            conditions=edit_api_schema.conditions.copy(),
        ) if self.mode == ConnectionFormMode.create else None

        return ConnectionForm(
            title=PostgreSQLConnectionInfoProvider.get_title(self._localizer),
            rows=self._filter_nulls([
                mdb_form_fill_row if mdb_enabled else None,
                *host_section,
                rc.port_row(default_value='6432'),
                *db_name_section,
                *username_section,
                rc.password_row(mode=self.mode),
                C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
                rc.raw_sql_level_row(),
                rc.collapse_advanced_settings_row(),
                C.CustomizableRow(items=[
                    C.LabelRowItem(
                        text=self._localizer.translate(Translatable('field_enforce-collate')),
                        display_conditions={CommonFieldName.advanced_settings: 'opened'},
                    ),
                    C.RadioButtonRowItem(
                        name=PostgreSQLFieldName.enforce_collate,
                        options=[
                            C.SelectableOption(
                                text=self._localizer.translate(Translatable('value_enforce-collate-auto')),
                                value=PGEnforceCollateMode.auto.value,
                            ),
                            C.SelectableOption(
                                text=self._localizer.translate(Translatable('value_enforce-collate-off')),
                                value=PGEnforceCollateMode.off.value,
                            ),
                            C.SelectableOption(
                                text=self._localizer.translate(Translatable('value_enforce-collate-on')),
                                value=PGEnforceCollateMode.on.value,
                            ),
                        ],
                        default_value=PGEnforceCollateMode.auto.value,
                        display_conditions={CommonFieldName.advanced_settings: 'opened'},
                    ),
                ]),
                *rc.ssl_rows(
                    enabled_name=CommonFieldName.ssl_enable,
                    enabled_help_text=self._localizer.translate(Translatable('label_postgres-ssl-enabled-tooltip')),
                    enabled_default_value=False,
                ),
                rc.data_export_forbidden_row(),
            ]),
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=FormActionApiSchema(items=[
                    FormFieldApiSchema(name=CommonFieldName.host, required=True),
                    FormFieldApiSchema(name=CommonFieldName.port, required=True),
                    FormFieldApiSchema(name=CommonFieldName.db_name, required=True),
                    FormFieldApiSchema(name=CommonFieldName.username, required=True),
                    FormFieldApiSchema(name=CommonFieldName.password, required=self.mode == ConnectionFormMode.create),
                    *self._get_top_level_check_api_schema_items(),
                ]),
            )
        )
