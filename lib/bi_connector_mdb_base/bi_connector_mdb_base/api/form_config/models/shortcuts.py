from dl_api_connector.form_config.models.common import CommonFieldName
from dl_constants.enums import ConnectionType

from bi_connector_mdb_base.api.form_config.models.common import MDBFieldName
from bi_connector_mdb_base.api.form_config.models.rows.prepared import components as mdb_components


def get_db_host_section(
    db_type: ConnectionType,
) -> tuple[mdb_components.CloudTreeSelectRow, mdb_components.MDBClusterRow, mdb_components.MDBHostRow,]:
    cloud_tree_selector_row = mdb_components.CloudTreeSelectRow(
        name=MDBFieldName.mdb_folder_id,
        display_conditions={
            mdb_components.MDBFormFillRow.Inner.mdb_fill_mode: mdb_components.MDBFormFillRow.Value.cloud,
        },
    )

    mdb_cluster_row = mdb_components.MDBClusterRow(
        name=MDBFieldName.mdb_cluster_id,
        db_type=db_type,
        display_conditions={
            mdb_components.MDBFormFillRow.Inner.mdb_fill_mode: mdb_components.MDBFormFillRow.Value.cloud,
        },
    )

    mdb_host_row = mdb_components.MDBHostRow(
        name=CommonFieldName.host,
        db_type=db_type,
        display_conditions={
            mdb_components.MDBFormFillRow.Inner.mdb_fill_mode: mdb_components.MDBFormFillRow.Value.cloud,
        },
    )

    return cloud_tree_selector_row, mdb_cluster_row, mdb_host_row
