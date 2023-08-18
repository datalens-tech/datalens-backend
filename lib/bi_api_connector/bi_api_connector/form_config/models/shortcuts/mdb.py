from typing import Optional

from bi_constants.enums import ConnectionType

import bi_api_connector.form_config.models.rows as C
from bi_api_connector.form_config.models.common import CommonFieldName


def get_db_host_section(
        is_org: bool,
        db_type: ConnectionType
) -> tuple[Optional[C.CloudTreeSelectRow], C.MDBClusterRow, C.MDBHostRow]:
    cloud_tree_selector_row = C.CloudTreeSelectRow(
        name=CommonFieldName.mdb_folder_id,
        display_conditions={C.MDBFormFillRow.Inner.mdb_fill_mode: C.MDBFormFillRow.Value.cloud},
    ) if is_org else None

    mdb_cluster_row = C.MDBClusterRow(
        name=CommonFieldName.mdb_cluster_id,
        db_type=db_type,
        display_conditions={C.MDBFormFillRow.Inner.mdb_fill_mode: C.MDBFormFillRow.Value.cloud},
    )

    mdb_host_row = C.MDBHostRow(
        name=CommonFieldName.host,
        db_type=db_type,
        display_conditions={C.MDBFormFillRow.Inner.mdb_fill_mode: C.MDBFormFillRow.Value.cloud},
    )

    return cloud_tree_selector_row, mdb_cluster_row, mdb_host_row
