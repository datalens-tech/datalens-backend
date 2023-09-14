from typing import Optional

import attr


@attr.s
class ConnMDBDataModelMixin:
    mdb_cluster_id: Optional[str] = attr.ib(default=None)
    mdb_folder_id: Optional[str] = attr.ib(default=None)
    is_verified_mdb_org: bool = attr.ib(default=False)
    skip_mdb_org_check: bool = attr.ib(default=False)
