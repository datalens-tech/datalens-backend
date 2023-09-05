from __future__ import annotations

from typing import Optional

from bi_configs.connectors_settings import ConnectorSettingsBase, PostgresConnectorSettings

from bi_api_commons.base_models import TenantDef

from bi_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES

import bi_api_connector.form_config.models.rows as C
from bi_api_connector.form_config.models.shortcuts.rows import RowConstructor
from bi_api_connector.form_config.models.api_schema import (
    FormFieldApiAction, FormFieldApiSchema, FormFieldApiActionCondition, FormFieldSelector,
    FormFieldConditionalApiAction,
)
from bi_api_connector.form_config.models.base import ConnectionForm
from bi_api_connector.form_config.models.common import CommonFieldName
from bi_api_connector.form_config.models.rows.base import FormRow
from bi_api_connector.form_config.models.shortcuts.mdb import get_db_host_section

from bi_connector_postgresql.bi.connection_form.form_config import PostgreSQLConnectionFormFactory


class PostgreSQLMDBConnectionFormFactory(PostgreSQLConnectionFormFactory):
    def get_form_config(
            self,
            connector_settings: Optional[ConnectorSettingsBase],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        assert connector_settings is not None and isinstance(connector_settings, PostgresConnectorSettings)
        rc = RowConstructor(localizer=self._localizer)
        mdb_enabled = connector_settings.USE_MDB_CLUSTER_PICKER

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
                C.MDBFormFillRow(),
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

        edit_api_schema = self.get_base_edit_api_schema()
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
        create_api_schema = self.get_base_create_api_schema(edit_api_schema)

        return self.get_base_form_config(
            host_section=host_section,
            username_section=username_section,
            db_name_section=db_name_section,
            create_api_schema=create_api_schema,
            edit_api_schema=edit_api_schema,
            rc=rc,
        )
