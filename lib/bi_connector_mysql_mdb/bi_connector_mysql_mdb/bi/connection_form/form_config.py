from __future__ import annotations

from typing import Optional

from dl_configs.connectors_settings import ConnectorSettingsBase

from dl_api_commons.base_models import TenantDef

import bi_connector_mdb_base.bi.form_config.models.rows.prepared.components as mdb_c
from dl_api_connector.form_config.models.shortcuts.rows import RowConstructor
from dl_api_connector.form_config.models.api_schema import (
    FormFieldApiAction, FormFieldApiSchema, FormFieldApiActionCondition, FormFieldSelector,
    FormFieldConditionalApiAction,
)
from dl_api_connector.form_config.models.base import ConnectionForm
from dl_api_connector.form_config.models.common import CommonFieldName
from dl_api_connector.form_config.models.rows.base import FormRow
from bi_connector_mdb_base.bi.form_config.models.shortcuts import get_db_host_section

from bi_connector_mysql.bi.connection_form.form_config import MySQLConnectionFormFactory
from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL
from bi_connector_mysql_mdb.core.settings import MysqlConnectorSettings


class MySQLMDBConnectionFormFactory(MySQLConnectionFormFactory):
    def get_form_config(
            self,
            connector_settings: Optional[ConnectorSettingsBase],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        assert connector_settings is not None and isinstance(connector_settings, MysqlConnectorSettings)
        rc = RowConstructor(localizer=self._localizer)
        mdb_enabled = connector_settings.USE_MDB_CLUSTER_PICKER

        db_name_section: list[FormRow]
        cloud_tree_selector_row, mdb_cluster_row, mdb_host_row = None, None, None
        if mdb_enabled:
            cloud_tree_selector_row, mdb_cluster_row, mdb_host_row = get_db_host_section(
                db_type=CONNECTION_TYPE_MYSQL,
            )
            host_section = [
                mdb_c.MDBFormFillRow(),
                cloud_tree_selector_row,
                mdb_cluster_row,
                mdb_host_row,
                rc.host_row(display_conditions={
                    mdb_c.MDBFormFillRow.Inner.mdb_fill_mode: mdb_c.MDBFormFillRow.Value.manually,
                }),
            ]
            username_section = [
                mdb_c.MDBUsernameRow(
                    name=CommonFieldName.username,
                    db_type=CONNECTION_TYPE_MYSQL,
                    display_conditions={
                        mdb_c.MDBFormFillRow.Inner.mdb_fill_mode: mdb_c.MDBFormFillRow.Value.cloud,
                    },
                ),
                rc.username_row(
                    display_conditions={
                        mdb_c.MDBFormFillRow.Inner.mdb_fill_mode: mdb_c.MDBFormFillRow.Value.manually,
                    }
                )
            ]
            db_name_section = [
                mdb_c.MDBDatabaseRow(
                    name=CommonFieldName.db_name,
                    db_type=CONNECTION_TYPE_MYSQL,
                    display_conditions={
                        mdb_c.MDBFormFillRow.Inner.mdb_fill_mode: mdb_c.MDBFormFillRow.Value.cloud,
                    },
                ),
                rc.db_name_row(
                    display_conditions={
                        mdb_c.MDBFormFillRow.Inner.mdb_fill_mode: mdb_c.MDBFormFillRow.Value.manually,
                    },
                ),
            ]
        else:
            host_section = [rc.host_row()]
            username_section = [rc.username_row()]
            db_name_section = [rc.db_name_row()]

        edit_api_schema = self._get_base_edit_api_schema()
        check_api_schema = self._get_base_check_api_schema()
        if mdb_enabled:
            assert mdb_cluster_row is not None
            mdb_cluster_row_api_sch = FormFieldApiSchema(
                name=mdb_cluster_row.name,
                default_action=FormFieldApiAction.skip,
                required=True,
            )
            edit_api_schema.items.append(mdb_cluster_row_api_sch)
            check_api_schema.items.append(mdb_cluster_row_api_sch)

            mdb_cluster_row_api_sch_cond = FormFieldApiActionCondition(
                when=FormFieldSelector(name=mdb_c.MDBFormFillRow.Inner.mdb_fill_mode),
                equals=mdb_c.MDBFormFillRow.Value.cloud,
                then=[
                    FormFieldConditionalApiAction(
                        selector=FormFieldSelector(name=mdb_cluster_row.name),
                        action=FormFieldApiAction.include,
                    ),
                ],
            )
            edit_api_schema.conditions.append(mdb_cluster_row_api_sch_cond)
            check_api_schema.conditions.append(mdb_cluster_row_api_sch_cond)

        create_api_schema = self._get_base_create_api_schema(edit_api_schema)

        return self._get_base_form_config(
            host_section=host_section,
            username_section=username_section,
            db_name_section=db_name_section,
            create_api_schema=create_api_schema,
            edit_api_schema=edit_api_schema,
            check_api_schema=check_api_schema,
            rc=rc,
        )
