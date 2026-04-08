import datetime

import pytest

import dl_dynconfig
from dl_temporal.schedule.config import TemporalSchedulesDynConfig


@pytest.mark.asyncio
async def test_temporal_schedules_dynconfig_from_source() -> None:
    source = dl_dynconfig.InMemorySource(
        data={
            "SYNC_INTERVAL": 120,
            "SCHEDULES": [
                {
                    "NAME": "full_sync",
                    "WORKFLOW_NAME": "vsw_dashboard_slice_full_sync",
                    "TASK_QUEUE": "vsw",
                    "INTERVAL": 300,
                }
            ],
        }
    )
    config = TemporalSchedulesDynConfig.model_from_source(source=source)

    await config.model_fetch()

    assert config.SYNC_INTERVAL == datetime.timedelta(seconds=120)
    assert len(config.SCHEDULES) == 1
    assert config.SCHEDULES[0].NAME == "full_sync"
    assert config.SCHEDULES[0].WORKFLOW_NAME == "vsw_dashboard_slice_full_sync"
    assert config.SCHEDULES[0].TASK_QUEUE == "vsw"
    assert config.SCHEDULES[0].INTERVAL == datetime.timedelta(seconds=300)
