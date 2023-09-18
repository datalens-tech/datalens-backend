class ChartOpCode:
    GET: str = "chart_get"
    CREATE: str = "chart_create"
    MODIFY: str = "chart_modify"
    REMOVE: str = "chart_remove"


class DashOpCode:
    GET: str = "dash_get"
    CREATE: str = "dash_create"
    MODIFY: str = "dash_modify"
    REMOVE: str = "dash_remove"


class DatasetOpCode:
    CONNECTION_CREATE: str = "connection_create"
    CONNECTION_MODIFY: str = "connection_modify"
    CONNECTION_DELETE: str = "connection_delete"
    CONNECTION_GET: str = "connection_get"
    WORKBOOK_INFO_GET: str = "workbook_info_get"
    DATASET_VALIDATE: str = "dataset_validate"
    DATASET_GET: str = "dataset_get"
    DATASET_CREATE: str = "dataset_create"
    DATASET_MODIFY: str = "dataset_modify"
    DATASET_REMOVE: str = "dataset_remove"
