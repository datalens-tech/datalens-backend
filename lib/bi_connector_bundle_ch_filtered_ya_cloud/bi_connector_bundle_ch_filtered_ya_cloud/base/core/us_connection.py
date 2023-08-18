import abc
from typing import Callable, Optional

import attr

from bi_utils.utils import DataKey

from bi_core import exc
from bi_core.utils import secrepr
from bi_core.us_connection_base import ConnectionBase
from bi_core.connection_executors.sync_base import SyncConnExecutorBase

from bi_core.connectors.clickhouse_base.us_connection import (
    ConnectionCHFilteredHardcodedDataBase, SubselectParameter, SubselectParameterType,
)
from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistry


class ConnectionCHFilteredSubselectByPuidBase(ConnectionCHFilteredHardcodedDataBase, metaclass=abc.ABCMeta):
    passport_user_id: Optional[int] = None

    @attr.s(kw_only=True)
    class DataModel(ConnectionCHFilteredHardcodedDataBase.DataModel):
        token: str = attr.ib(repr=secrepr)

        @classmethod
        def get_secret_keys(cls) -> set[DataKey]:
            return {
                *super().get_secret_keys(),
                DataKey(parts=('token',)),
            }

    def fetch_user_id(self) -> None:
        sr = self.us_manager.get_services_registry()
        yc_sr = sr.get_installation_specific_service_registry(YCServiceRegistry)
        bb_cli = yc_sr.get_blackbox_client()
        assert bb_cli is not None
        self.passport_user_id = bb_cli.get_user_id_by_oauth_token_sync(self.data.token)

    def test(self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase]) -> None:
        # We have to call `fetch_user_id` here, because
        #   - post_init_hook could not be executed (if object has been created from dict right now)
        #   - token could be changed
        self.fetch_user_id()

        if not self.passport_user_id:
            raise exc.SourceAccessInvalidToken()

        super().test(conn_executor_factory=conn_executor_factory)

    @property
    def is_subselect_allowed(self) -> bool:
        return True

    @property
    def subselect_parameters(self) -> list[SubselectParameter]:
        if not self.passport_user_id:
            raise exc.SourceAccessInvalidToken()

        return [
            SubselectParameter(
                name='passport_user_id',
                ss_type=SubselectParameterType.single_value,
                values=self.passport_user_id,
            )
        ]
