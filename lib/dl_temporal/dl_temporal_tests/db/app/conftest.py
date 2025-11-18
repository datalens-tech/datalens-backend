import logging
import os
from typing import (
    AsyncGenerator,
    ClassVar,
)

import attr
import pytest
import pytest_asyncio
from typing_extensions import override

import dl_temporal
import dl_temporal.app
import dl_temporal_tests.db.activities as activities
import dl_temporal_tests.db.workflows as workflows
import dl_testing


DIR_PATH = os.path.dirname(__file__)
LOGGER = logging.getLogger(__name__)


class Settings(dl_temporal.app.BaseTemporalWorkerAppSettings):
    ...


class App(dl_temporal.app.BaseTemporalWorkerApp):
    ...


@attr.define(kw_only=True, slots=False)
class Factory(dl_temporal.app.BaseTemporalWorkerAppFactory[App]):
    settings: Settings
    app_class: ClassVar[type[App]] = App

    @override
    async def _get_temporal_workflows(
        self,
    ) -> list[type[dl_temporal.WorkflowProtocol]]:
        return [workflows.Workflow]

    @override
    async def _get_temporal_activities(
        self,
    ) -> list[dl_temporal.ActivityProtocol]:
        return [activities.Activity()]

    @override
    async def _get_temporal_client_metadata_provider(
        self,
    ) -> dl_temporal.MetadataProvider:
        return dl_temporal.EmptyMetadataProvider()

    @override
    async def _get_logger(
        self,
    ) -> logging.Logger:
        return LOGGER


@pytest.fixture(name="app_settings")
def fixture_app_settings(
    monkeypatch: pytest.MonkeyPatch,
    temporal_task_queue: str,
    temporal_namespace: str,
    temporal_hostport: dl_testing.HostPort,
) -> Settings:
    monkeypatch.setenv("CONFIG_PATH", os.path.join(DIR_PATH, "config.yaml"))

    monkeypatch.setenv("TEMPORAL_WORKER__TASK_QUEUE", temporal_task_queue)
    monkeypatch.setenv("TEMPORAL_CLIENT__NAMESPACE", temporal_namespace)
    monkeypatch.setenv("TEMPORAL_CLIENT__HOST", temporal_hostport.host)
    monkeypatch.setenv("TEMPORAL_CLIENT__PORT", str(temporal_hostport.port))

    return Settings()


@pytest_asyncio.fixture(name="app", autouse=True)
async def fixture_app(
    app_settings: Settings,
) -> AsyncGenerator[dl_temporal.app.BaseTemporalWorkerApp, None]:
    factory = Factory(settings=app_settings)
    app = await factory.create_application()

    async with app.run_in_task_context() as app:
        yield app
