from enum import Enum

import attr

from dl_api_connector.form_config.models.common import remap
from dl_api_connector.form_config.models.rows.base import DisplayConditionsMixin, FormFieldMixin
from dl_api_connector.form_config.models.rows.prepared.base import PreparedRow, DisabledMixin
from dl_constants.enums import ConnectionType


@attr.s(kw_only=True, frozen=True)
class MDBFormFillRow(PreparedRow, DisplayConditionsMixin, DisabledMixin):
    type = 'mdb_form_fill'

    class Inner(PreparedRow.Inner):
        mdb_fill_mode = 'mdb_fill_mode'

    class Value(Enum):
        cloud = 'cloud'
        manually = 'manually'


@attr.s(kw_only=True, frozen=True)
class MDBClusterRow(PreparedRow, FormFieldMixin, DisplayConditionsMixin, DisabledMixin):
    type = 'mdb_cluster'

    class Inner(PreparedRow.Inner):
        sql_database_management = 'sql_database_management'
        sql_user_management = 'sql_user_management'

    db_type: ConnectionType = attr.ib(metadata=remap('dbType'))


@attr.s(kw_only=True, frozen=True)
class MDBFieldRow(PreparedRow, FormFieldMixin, DisplayConditionsMixin, DisabledMixin):
    db_type: ConnectionType = attr.ib(metadata=remap('dbType'))


@attr.s(kw_only=True, frozen=True)
class MDBHostRow(MDBFieldRow):
    type = 'mdb_host'


@attr.s(kw_only=True, frozen=True)
class MDBUsernameRow(MDBFieldRow):
    type = 'mdb_username'


@attr.s(kw_only=True, frozen=True)
class MDBDatabaseRow(MDBFieldRow):
    type = 'mdb_database'


@attr.s(kw_only=True, frozen=True)
class CloudTreeSelectRow(PreparedRow, DisplayConditionsMixin, FormFieldMixin, DisabledMixin):
    type = 'cloud_tree_select'


@attr.s(kw_only=True, frozen=True)
class ServiceAccountRow(PreparedRow, DisplayConditionsMixin, FormFieldMixin, DisabledMixin):
    type = 'service_account'
