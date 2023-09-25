from __future__ import annotations

from dl_connector_bundle_chs3.chs3_base.core.data_source import BaseFileS3DataSource
from dl_connector_bundle_chs3.chs3_gsheets.core.constants import (
    CONNECTION_TYPE_GSHEETS_V2,
    NOTIF_TYPE_GSHEETS_V2_DATA_UPDATE_FAILURE,
    SOURCE_TYPE_GSHEETS_V2,
)
from dl_constants.enums import (
    ComponentErrorLevel,
    CreateDSFrom,
)
from dl_core import exc
from dl_core.reporting.notifications import get_notification_record


class GSheetsFileS3DataSource(BaseFileS3DataSource):
    conn_type = CONNECTION_TYPE_GSHEETS_V2

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            SOURCE_TYPE_GSHEETS_V2,
        }

    def _handle_component_errors(self) -> None:
        conn_src_id = self.origin_source_id
        if conn_src_id is not None and (error_pack := self.connection.data.component_errors.get_pack(conn_src_id)):
            single_error = error_pack.errors[0]

            if single_error.level == ComponentErrorLevel.error:

                class ThisDataSourceError(exc.DataSourceErrorFromComponentError):
                    err_code = exc.DataSourceErrorFromComponentError.err_code + single_error.code
                    default_message = single_error.message

                raise ThisDataSourceError(
                    details=single_error.details,
                )
            else:
                reporting_registry = self._get_connection().us_manager.get_services_registry().get_reporting_registry()
                # this is the only case of connection component errors at the moment
                # may be generalized in the future
                reporting_registry.save_reporting_record(
                    get_notification_record(
                        NOTIF_TYPE_GSHEETS_V2_DATA_UPDATE_FAILURE,
                        err_code=".".join(single_error.code),
                        request_id=single_error.details.get("request-id"),
                    )
                )
