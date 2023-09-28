from enum import unique

from dl_api_connector.form_config.models.common import FormFieldName


@unique
class MDBFieldName(FormFieldName):
    mdb_cluster_id = "mdb_cluster_id"
    mdb_folder_id = "mdb_folder_id"
    folder_id = "folder_id"
    service_account_id = "service_account_id"
