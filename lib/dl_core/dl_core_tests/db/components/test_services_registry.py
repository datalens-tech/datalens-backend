import typing

import pytest

from dl_api_commons.base_models import RequestContextInfo
from dl_core.services_registry import ServicesRegistry
from dl_core.services_registry.entity_checker import (
    EntityUsageChecker,
    EntityUsageNotAllowed,
)
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_dataset import Dataset
from dl_core.us_manager.us_manager import USManagerBase
from dl_core_tests.db.base import DefaultCoreTestClass
from dl_i18n.localizer_base import Localizer


class LocalEntityUsageChecker(EntityUsageChecker):
    IS_DASHSQL_ALLOWED: typing.ClassVar[bool] = True

    def ensure_dataset_can_be_used(
        self,
        rci: RequestContextInfo,
        dataset: Dataset,
        us_manager: USManagerBase,
        localizer: Localizer | None = None,
    ) -> None:
        if not dataset.data.load_preview_by_default:
            raise EntityUsageNotAllowed("Preview should be enabled by default!")

    def ensure_data_connection_can_be_used(self, rci: RequestContextInfo, conn: ConnectionBase):
        if not self.IS_DASHSQL_ALLOWED:
            raise EntityUsageNotAllowed("DashSQL should be allowed!")


class TestServicesRegistry(DefaultCoreTestClass):
    @pytest.fixture(scope="session")
    def conn_async_service_registry(
        self,
        root_certificates: bytes,
        conn_bi_context: RequestContextInfo,
    ) -> ServicesRegistry:
        return self.service_registry_factory(
            conn_exec_factory_async_env=True,
            conn_bi_context=conn_bi_context,
            root_certificates_data=root_certificates,
            entity_usage_checker=LocalEntityUsageChecker(),
        )

    def test_conn_executor_caches(self, conn_async_service_registry, saved_connection):
        ce_factory = conn_async_service_registry.get_conn_executor_factory()

        initial_ce = ce_factory.get_async_conn_executor(saved_connection)
        ce_wo_changes = ce_factory.get_async_conn_executor(saved_connection)
        assert initial_ce is ce_wo_changes

        # Modifying connection
        saved_connection.data.host = "example.com"
        ce_after_connection_change = ce_factory.get_async_conn_executor(saved_connection)
        assert ce_after_connection_change is not initial_ce

    def test_entity_checker(self, conn_async_service_registry, saved_connection, empty_saved_dataset, sync_us_manager):
        ce_factory = conn_async_service_registry.get_conn_executor_factory()

        # test ensure_data_connection_can_be_used
        assert ce_factory.get_async_conn_executor(saved_connection) is not None
        LocalEntityUsageChecker.IS_DASHSQL_ALLOWED = False
        with pytest.raises(EntityUsageNotAllowed, match="^DashSQL should be allowed!$"):
            ce_factory.get_async_conn_executor(saved_connection)

        # test ensure_dataset_can_be_used; can only be tested directly
        checker = LocalEntityUsageChecker()
        rci = RequestContextInfo.create_empty()
        assert checker.ensure_dataset_can_be_used(rci, empty_saved_dataset, sync_us_manager) is None
        empty_saved_dataset.data.load_preview_by_default = False
        empty_saved_dataset.data.template_enabled = False
        empty_saved_dataset.data.data_export_forbidden = False
        with pytest.raises(EntityUsageNotAllowed, match="^Preview should be enabled by default!$"):
            checker.ensure_dataset_can_be_used(rci, empty_saved_dataset, sync_us_manager)
