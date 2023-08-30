from __future__ import annotations

from typing import TYPE_CHECKING, FrozenSet, Optional, Sequence

import attr
import pytest

from bi_connector_clickhouse.core.us_connection import ConnectionClickhouseBase
from bi_api_commons.base_models import RequestContextInfo
from bi_core.services_registry.entity_checker import EntityUsageChecker
from bi_core.services_registry.env_manager_factory import IntranetEnvManagerFactory
from bi_core.services_registry.sr_factories import DefaultSRFactory
from bi_core.services_registry.typing import ConnectOptionsFactory
from bi_core.united_storage_client import USAuthContextMaster
from bi_core.us_dataset import Dataset
from bi_core.us_manager.us_manager_async import AsyncUSManager
from bi_core.exc import EntityUsageNotAllowed
from bi_core.mdb_utils import MDBDomainManagerSettings

if TYPE_CHECKING:
    from bi_core.us_connection_base import ConnectionBase
    from bi_core.us_manager.us_manager import USManagerBase


def _rci(user_name: str) -> RequestContextInfo:
    return attr.evolve(
        RequestContextInfo.create_empty(),
        user_name=user_name,
    )


@pytest.fixture()
async def usm_factory(rqe_config_subprocess, core_test_config):
    usm_list = []
    us_config = core_test_config.get_us_config()

    def make_sr_usm(
            rci: RequestContextInfo,
            victims: Sequence[str],
            conn_cls_whitelist: Optional[FrozenSet] = None,
            connect_options_factory: Optional[ConnectOptionsFactory] = None,
            entity_usage_checker: Optional[EntityUsageChecker] = None,
    ) -> AsyncUSManager:
        sr_factory = DefaultSRFactory(
            async_env=True,
            rqe_config=rqe_config_subprocess,
            bleeding_edge_users=victims,
            conn_cls_whitelist=conn_cls_whitelist,
            connect_options_factory=connect_options_factory,
            entity_usage_checker=entity_usage_checker,
            env_manager_factory=IntranetEnvManagerFactory(),
            mdb_domain_manager_settings=MDBDomainManagerSettings(),
        )
        usm = AsyncUSManager(
            us_base_url=us_config.us_host,
            us_auth_context=USAuthContextMaster(us_config.us_master_token),
            crypto_keys_config=core_test_config.get_crypto_keys_config(),
            bi_context=rci,
            services_registry=sr_factory.make_service_registry(rci),
        )
        usm_list.append(usm)
        usm.get_services_registry()
        return usm

    yield make_sr_usm

    for usm_to_close in usm_list:
        await usm_to_close.get_services_registry().close_async()
        await usm_to_close.close()


@pytest.mark.asyncio
async def test_entity_checker(usm_factory, saved_ch_connection, saved_pg_connection):
    rci = _rci('some_user')

    class LocalEntityUsageChecker(EntityUsageChecker):
        def ensure_dataset_can_be_used(
            self, rci: RequestContextInfo, dataset: Dataset,
            us_manager: USManagerBase,
        ) -> None:
            pass

        def ensure_data_connection_can_be_used(self, rci: RequestContextInfo, conn: ConnectionBase):
            if isinstance(conn, ConnectionClickhouseBase):
                raise EntityUsageNotAllowed("No Clickhouse!")

    entity_usage_checker = LocalEntityUsageChecker()

    usm = usm_factory(rci, (), entity_usage_checker=entity_usage_checker)

    conn_exec_factory = usm.get_services_registry().get_conn_executor_factory()

    ch_conn = await usm.get_by_id(saved_ch_connection.uuid)
    pg_conn = await usm.get_by_id(saved_pg_connection.uuid)

    assert conn_exec_factory.get_async_conn_executor(pg_conn) is not None
    with pytest.raises(EntityUsageNotAllowed, match="^No Clickhouse!$"):
        conn_exec_factory.get_async_conn_executor(ch_conn)
