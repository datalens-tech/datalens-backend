from __future__ import annotations

from typing import (
    ClassVar,
    Optional,
)

import attr

from dl_core.us_connection_base import ClassicConnectionSQL  # DataSourceTemplate,

# from dl_i18n.localizer_base import Localizer
from dl_core.utils import secrepr

from dl_connector_trino.core.constants import (
    CONNECTION_TYPE_TRINO,
    SOURCE_TYPE_TRINO_SUBSELECT,
    SOURCE_TYPE_TRINO_TABLE,
    TrinoAuthType,
)
from dl_connector_trino.core.dto import TrinoConnDTO


class ConnectionTrino(ClassicConnectionSQL):
    conn_type = CONNECTION_TYPE_TRINO
    has_schema: ClassVar[bool] = True
    default_schema_name = None
    source_type = SOURCE_TYPE_TRINO_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_TRINO_TABLE, SOURCE_TYPE_TRINO_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True

    @attr.s(kw_only=True)
    class DataModel(ClassicConnectionSQL.DataModel):
        auth_type: TrinoAuthType = attr.ib(default=TrinoAuthType.NONE)
        ssl_ca: Optional[str] = attr.ib(repr=secrepr, default=None)

    def get_conn_dto(self) -> TrinoConnDTO:
        return TrinoConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            port=self.data.port,
            username=self.data.username,
            auth_type=self.data.auth_type,
            password=self.password,
            ssl_ca=self.data.ssl_ca,
        )

    # @property
    # def allow_public_usage(self) -> bool:
    #     return True

    # def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
    #     return self._make_subselect_templates(source_type=SOURCE_TYPE_TRINO_SUBSELECT, localizer=localizer)
