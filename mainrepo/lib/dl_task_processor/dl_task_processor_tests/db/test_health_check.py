import asyncio

import pytest

from dl_configs.enums import RedisMode
from dl_configs.settings_submodels import RedisSettings
from dl_task_processor.worker import (
    HealthChecker,
    WorkerSettings,
)


def _get_broken_redis_settings():
    return RedisSettings(
        MODE=RedisMode.single_host,
        CLUSTER_NAME="",
        HOSTS=("127.0.0.1",),
        PORT=1010,
        DB=10,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("worker_settings", [WorkerSettings(health_check_suffix="bla")], indirect=True)
async def test_health_check_success(task_processor_arq_worker, worker_settings):
    # lets wait for the first task pool iteration
    # i really dont know how i can do it better
    await asyncio.sleep(10)
    with pytest.raises(SystemExit) as exc:
        await HealthChecker(worker=task_processor_arq_worker).check()
    assert exc.value.code == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("redis_settings", "worker_settings"),
    [
        (_get_broken_redis_settings(), WorkerSettings(health_check_suffix="notbla")),
    ],
    indirect=True,
)
async def test_health_check_fail(init_task_processor_arq_worker, redis_settings, worker_settings):
    with pytest.raises(SystemExit) as exc:
        await HealthChecker(worker=init_task_processor_arq_worker).check()
    assert exc.value.code == 1
