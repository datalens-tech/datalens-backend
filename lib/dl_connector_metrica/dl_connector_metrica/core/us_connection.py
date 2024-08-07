from __future__ import annotations

from datetime import date
import logging
from typing import (
    TYPE_CHECKING,
    Callable,
    ClassVar,
    Optional,
    Sequence,
)

import attr

from dl_constants.enums import (
    ConnectionType,
    UserDataType,
)
from dl_core import exc
from dl_core.db import SchemaColumn
from dl_core.us_connection_base import (
    ConnectionBase,
    DataSourceTemplate,
)
from dl_core.utils import secrepr
import dl_sqlalchemy_metrica_api
from dl_sqlalchemy_metrica_api import api_client as metrika_api_client
from dl_sqlalchemy_metrica_api.api_info.appmetrica import AppMetricaFieldsNamespaces
from dl_sqlalchemy_metrica_api.api_info.metrika import MetrikaApiCounterSource
from dl_type_transformer.type_transformer import get_type_transformer
from dl_utils.utils import DataKey

from dl_connector_metrica.core.constants import (
    SOURCE_TYPE_APPMETRICA_API,
    SOURCE_TYPE_METRICA_API,
)
from dl_connector_metrica.core.dto import (
    AppMetricaAPIConnDTO,
    MetricaAPIConnDTO,
)


if TYPE_CHECKING:
    from dl_core.connection_executors import SyncConnExecutorBase
    from dl_core.services_registry.top_level import ServicesRegistry


LOGGER = logging.getLogger(__name__)


def parse_metrica_ids(ids_str: str) -> Sequence[str]:
    if not ids_str:
        return []
    return [id_.strip() for id_ in ids_str.split(",")]


class MetrikaBaseMixin(ConnectionBase):
    metrica_host: Optional[str] = None

    def __init__(self, *args, **kwargs):  # type: ignore  # TODO: fix
        super().__init__(*args, **kwargs)  # type: ignore  # TODO: fix
        self._initial_counter_id = self.data.counter_id if self._data is not None else None  # type: ignore  # TODO: fix

    @property
    def allow_public_usage(self) -> bool:
        return False

    @property
    def metrika_oauth(self):  # type: ignore  # TODO: fix
        return self.data.token  # type: ignore  # TODO: fix

    @property
    def counter_id(self):  # type: ignore  # TODO: fix
        return self.data.counter_id  # type: ignore  # TODO: fix

    def get_metrica_api_cli(self) -> metrika_api_client.MetrikaApiClient:
        return metrika_api_client.MetrikaApiClient(oauth_token=self.metrika_oauth, host=self.metrica_host)

    def get_counter_creation_date(self):  # type: ignore  # TODO: fix
        assert isinstance(self.counter_id, str)
        ids = list(filter(lambda t: t, parse_metrica_ids(self.counter_id)))
        min_date = min([self.get_metrica_api_cli().get_counter_creation_date(cid) for cid in ids])
        return min_date

    @property
    def counter_creation_date(self):  # type: ignore  # TODO: fix
        return self.data.counter_creation_date  # type: ignore  # TODO: fix

    def get_available_counters(self) -> list[dict]:
        return self.get_metrica_api_cli().get_available_counters()

    async def validate_new_data(
        self,
        services_registry: ServicesRegistry,
        changes: Optional[dict] = None,
        original_version: Optional[ConnectionBase] = None,
    ) -> None:
        await super().validate_new_data(  # type: ignore  # TODO: fix  # mixin
            services_registry=services_registry,
            changes=changes,
            original_version=original_version,
        )
        if original_version is None:
            return  # only validating edits here
        assert isinstance(changes, dict)
        data_changes = changes.get("data") or {}
        if data_changes.get("token"):
            return  # token provided, nothing to check
        current_counter_id = self.data.counter_id  # type: ignore  # TODO: fix  # mixin
        if str(data_changes.get("counter_id") or "") == str(current_counter_id):
            return  # no counter_id change
        raise exc.ConnectionConfigurationError('"token" must be specified if "counter_id" is changing.')


class MetrikaApiConnection(MetrikaBaseMixin, ConnectionBase):  # type: ignore  # TODO: fix
    is_always_internal_source: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True

    metrica_host = metrika_api_client.METRIKA_API_HOST
    source_type = SOURCE_TYPE_METRICA_API

    @attr.s(kw_only=True)
    class DataModel(ConnectionBase.DataModel):
        token: str = attr.ib(repr=secrepr)
        counter_id: str = attr.ib()  # single counter id or comma-separated counters list
        counter_creation_date: Optional[date] = attr.ib(default=None)  # minimal date in case of multiple counters
        accuracy: Optional[float] = attr.ib(default=None)  # sample share (0; 1]

        @classmethod
        def get_secret_keys(cls) -> set[DataKey]:
            return {
                *super().get_secret_keys(),
                DataKey(parts=("token",)),
            }

    @property
    def metrika_oauth(self):  # type: ignore  # TODO: fix
        return self.data.token

    @property
    def table_name(self):  # type: ignore  # TODO: fix
        return self.data.counter_id

    def get_conn_dto(self) -> MetricaAPIConnDTO:
        return MetricaAPIConnDTO(
            conn_id=self.uuid,
            token=self.metrika_oauth,
            accuracy=self.data.accuracy,
        )

    @property
    def cache_ttl_sec_override(self) -> Optional[int]:
        return None

    @classmethod
    def get_api_fields_info(cls):  # type: ignore  # TODO: fix
        return dl_sqlalchemy_metrica_api.api_info.metrika

    @classmethod
    def get_raw_schema(cls, metrica_namespace: str, actual_conn_type: ConnectionType) -> Sequence[SchemaColumn]:
        fields_info = cls.get_api_fields_info().fields_by_namespace.get(
            cls.get_api_fields_info().metrica_fields_namespaces[metrica_namespace], []
        )

        def user_type_converter(type_name: str) -> UserDataType:
            return UserDataType[type_name] if type_name != "datetime" else UserDataType.genericdatetime

        raw_schema = tuple(
            SchemaColumn(
                name=field["name"],
                title=field["title"],
                user_type=user_type_converter(field["type"]),
                nullable=True,
                native_type=get_type_transformer(actual_conn_type).type_user_to_native(
                    user_t=user_type_converter(field["type"])
                ),
                description=field.get("description", ""),
                has_auto_aggregation=not field["is_dim"],
                lock_aggregation=True,
            )
            for field in fields_info
        )

        return raw_schema

    def get_parameter_combinations(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[dict]:
        return [dict(db_name=item.name) for item in MetrikaApiCounterSource]

    def get_data_source_templates(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[DataSourceTemplate]:
        return [
            DataSourceTemplate(
                title=parameters["db_name"],
                group=[],
                source_type=self.source_type,
                connection_id=self.uuid,  # type: ignore  # TODO: fix
                parameters=parameters,
            )
            for parameters in self.get_parameter_combinations(conn_executor_factory=conn_executor_factory)
        ]

    @property
    def allow_public_usage(self) -> bool:
        return False


class AppMetricaApiConnection(MetrikaApiConnection):
    allow_cache: ClassVar[bool] = True

    metrica_host = metrika_api_client.APPMETRICA_API_HOST
    source_type = SOURCE_TYPE_APPMETRICA_API

    def get_conn_dto(self) -> AppMetricaAPIConnDTO:
        return AppMetricaAPIConnDTO(
            conn_id=self.uuid,
            token=self.metrika_oauth,
            accuracy=self.data.accuracy,
        )

    @classmethod
    def get_api_fields_info(cls):  # type: ignore  # TODO: fix
        return dl_sqlalchemy_metrica_api.api_info.appmetrica

    def get_parameter_combinations(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[dict]:
        return [dict(db_name=item.name) for item in AppMetricaFieldsNamespaces]

    @property
    def allow_public_usage(self) -> bool:
        return False
