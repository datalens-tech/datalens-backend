import abc
import os
from typing import (
    Generic,
    Optional,
    TypeVar,
)

import attr

from dl_api_commons.tenant_resolver import (
    CommonTenantResolver,
    TenantResolver,
)
from dl_core.aio.web_app_services.gsheets import GSheetsSettings
from dl_core.aio.web_app_services.s3 import S3Service
from dl_core.loader import (
    CoreLibraryConfig,
    load_core_lib,
)
from dl_file_uploader_lib.settings_utils import init_redis_service
from dl_file_uploader_task_interface.context import (
    FileUploaderTaskContext,
    SecureReaderSettings,
)
from dl_file_uploader_task_interface.tasks import CleanS3LifecycleRulesTask
from dl_file_uploader_worker_lib.settings import FileUploaderWorkerSettings
from dl_file_uploader_worker_lib.tasks import REGISTRY
from dl_task_processor.arq_wrapper import (
    CronSchedule,
    create_arq_redis_settings,
    create_redis_pool,
    make_cron_task,
)
from dl_task_processor.context import BaseContextFabric
from dl_task_processor.executor import ExecutorFabric
from dl_task_processor.state import (
    DummyStateImpl,
    TaskState,
)
from dl_task_processor.worker import (
    ArqWorker,
    WorkerMetricsSenderProtocol,
    WorkerSettings,
)
from dl_utils.aio import ContextVarExecutor


_TSettings = TypeVar("_TSettings", bound=FileUploaderWorkerSettings)


@attr.s
class FileUploaderContextFab(BaseContextFabric):
    _settings: FileUploaderWorkerSettings = attr.ib()
    _ca_data: bytes = attr.ib()
    _tenant_resolver: TenantResolver = attr.ib(factory=lambda: CommonTenantResolver())

    async def make(self) -> FileUploaderTaskContext:
        core_conn_whitelist = ["clickhouse", "file", "gsheets_v2", "yadocs"]
        load_core_lib(core_lib_config=CoreLibraryConfig(core_connector_ep_names=core_conn_whitelist))

        redis_service = init_redis_service(self._settings)
        s3_service = S3Service(
            access_key_id=self._settings.S3.ACCESS_KEY_ID,
            secret_access_key=self._settings.S3.SECRET_ACCESS_KEY,
            endpoint_url=self._settings.S3.ENDPOINT_URL,
            tmp_bucket_name=self._settings.S3_TMP_BUCKET_NAME,
            persistent_bucket_name=self._settings.S3_PERSISTENT_BUCKET_NAME,
        )
        await redis_service.initialize()
        await s3_service.initialize()
        redis_pool = await create_redis_pool(create_arq_redis_settings(self._settings.REDIS_ARQ))
        return FileUploaderTaskContext(
            settings=self._settings,
            s3_service=s3_service,
            redis_service=redis_service,
            tpe=ContextVarExecutor(max_workers=min(32, (os.cpu_count() or 0) * 3 + 4)),
            gsheets_settings=GSheetsSettings(
                api_key=self._settings.GSHEETS_APP.API_KEY,
                client_id=self._settings.GSHEETS_APP.CLIENT_ID,
                client_secret=self._settings.GSHEETS_APP.CLIENT_SECRET,
            ),
            redis_pool=redis_pool,
            crypto_keys_config=self._settings.CRYPTO_KEYS_CONFIG,
            secure_reader_settings=SecureReaderSettings(
                socket=self._settings.SECURE_READER.SOCKET,
                endpoint=self._settings.SECURE_READER.ENDPOINT,
                cafile=self._settings.SECURE_READER.CAFILE,
            ),
            tenant_resolver=self._tenant_resolver,
            ca_data=self._ca_data,
        )

    async def tear_down(self, inst: FileUploaderTaskContext) -> None:  # type: ignore  # 2024-01-30 # TODO: Argument 1 of "tear_down" is incompatible with supertype "BaseContextFabric"; supertype defines the argument type as "BaseContext"  [override]
        await inst.s3_service.tear_down()
        await inst.redis_service.tear_down()
        inst.tpe.shutdown()


@attr.s(kw_only=True)
class FileUploaderWorkerFactory(Generic[_TSettings], abc.ABC):
    _ca_data: bytes = attr.ib()
    _settings: _TSettings = attr.ib()

    @abc.abstractmethod
    def _get_tenant_resolver(self) -> TenantResolver:
        raise NotImplementedError()

    def _get_metrics_sender(self) -> Optional[WorkerMetricsSenderProtocol]:
        return None

    def create_worker(self, state: Optional[TaskState] = None) -> ArqWorker:
        if state is None:
            state = TaskState(DummyStateImpl())
        cron_tasks = []
        if self._settings.ENABLE_REGULAR_S3_LC_RULES_CLEANUP:
            cron_tasks.append(
                make_cron_task(
                    CleanS3LifecycleRulesTask(),
                    schedule=CronSchedule(hour={23}, minute={0}, second={0}),
                )
            )
        worker = ArqWorker(
            redis_settings=create_arq_redis_settings(self._settings.REDIS_ARQ),
            executor_fab=ExecutorFabric(
                registry=REGISTRY,
                state=state,
            ),
            context_fab=FileUploaderContextFab(
                settings=self._settings,
                ca_data=self._ca_data,
                tenant_resolver=self._get_tenant_resolver(),
            ),
            worker_settings=WorkerSettings(max_concurrent_jobs=self._settings.MAX_CONCURRENT_JOBS),
            cron_tasks=cron_tasks,
            metrics_sender=self._get_metrics_sender(),
        )
        return worker
