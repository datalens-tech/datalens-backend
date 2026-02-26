import datetime

import pytest

import dl_app_api_base


@pytest.mark.asyncio
async def test_get_all_statuses_all_healthy() -> None:
    service = dl_app_api_base.ReadinessService(
        subsystems=[
            dl_app_api_base.SubsystemReadinessSyncCallback(name="sub1", is_ready=lambda: True),
            dl_app_api_base.SubsystemReadinessSyncCallback(name="sub2", is_ready=lambda: True),
        ],
    )
    result = await service.get_all_statuses()
    assert result.statuses["sub1"].value is True
    assert result.statuses["sub1"].critical is True
    assert result.statuses["sub2"].value is True


@pytest.mark.asyncio
async def test_get_all_statuses_some_unhealthy() -> None:
    service = dl_app_api_base.ReadinessService(
        subsystems=[
            dl_app_api_base.SubsystemReadinessSyncCallback(name="sub1", is_ready=lambda: True),
            dl_app_api_base.SubsystemReadinessSyncCallback(name="sub2", is_ready=lambda: False),
        ],
    )
    result = await service.get_all_statuses()
    assert result.statuses["sub1"].value is True
    assert result.statuses["sub2"].value is False


@pytest.mark.asyncio
async def test_get_all_statuses_async_callback() -> None:
    async def check() -> bool:
        return True

    service = dl_app_api_base.ReadinessService(
        subsystems=[dl_app_api_base.SubsystemReadinessAsyncCallback(name="async_sub", is_ready=check)],
    )
    result = await service.get_all_statuses()
    assert result.statuses["async_sub"].value is True


@pytest.mark.asyncio
async def test_status_has_request_dt() -> None:
    before = datetime.datetime.now()
    service = dl_app_api_base.ReadinessService(
        subsystems=[dl_app_api_base.SubsystemReadinessSyncCallback(name="sub1", is_ready=lambda: True)],
    )
    result = await service.get_all_statuses()
    after = datetime.datetime.now()
    assert before <= result.statuses["sub1"].request_dt <= after


@pytest.mark.asyncio
async def test_status_has_critical_from_callback() -> None:
    service = dl_app_api_base.ReadinessService(
        subsystems=[
            dl_app_api_base.SubsystemReadinessSyncCallback(name="crit", is_ready=lambda: True, critical=True),
            dl_app_api_base.SubsystemReadinessSyncCallback(name="non_crit", is_ready=lambda: True, critical=False),
        ],
    )
    result = await service.get_all_statuses()
    assert result.statuses["crit"].critical is True
    assert result.statuses["non_crit"].critical is False


@pytest.mark.asyncio
async def test_is_ready_true() -> None:
    service = dl_app_api_base.ReadinessService(
        subsystems=[
            dl_app_api_base.SubsystemReadinessSyncCallback(name="sub1", is_ready=lambda: True),
            dl_app_api_base.SubsystemReadinessSyncCallback(name="sub2", is_ready=lambda: True),
        ],
    )
    statuses = await service.get_all_statuses()
    assert statuses.is_ready() is True


@pytest.mark.asyncio
async def test_is_ready_false() -> None:
    service = dl_app_api_base.ReadinessService(
        subsystems=[
            dl_app_api_base.SubsystemReadinessSyncCallback(name="sub1", is_ready=lambda: True),
            dl_app_api_base.SubsystemReadinessSyncCallback(name="sub2", is_ready=lambda: False),
        ],
    )
    statuses = await service.get_all_statuses()
    assert statuses.is_ready() is False


@pytest.mark.asyncio
async def test_is_critical_ready_with_non_critical_failure() -> None:
    service = dl_app_api_base.ReadinessService(
        subsystems=[
            dl_app_api_base.SubsystemReadinessSyncCallback(name="critical_sub", is_ready=lambda: True, critical=True),
            dl_app_api_base.SubsystemReadinessSyncCallback(
                name="non_critical_sub", is_ready=lambda: False, critical=False
            ),
        ],
    )
    statuses = await service.get_all_statuses()
    assert statuses.is_critical_ready() is True


@pytest.mark.asyncio
async def test_is_critical_ready_with_critical_failure() -> None:
    service = dl_app_api_base.ReadinessService(
        subsystems=[
            dl_app_api_base.SubsystemReadinessSyncCallback(name="critical_sub", is_ready=lambda: False, critical=True),
            dl_app_api_base.SubsystemReadinessSyncCallback(
                name="non_critical_sub", is_ready=lambda: True, critical=False
            ),
        ],
    )
    statuses = await service.get_all_statuses()
    assert statuses.is_critical_ready() is False


@pytest.mark.asyncio
async def test_cached_within_ttl() -> None:
    call_count = 0

    def check() -> bool:
        nonlocal call_count
        call_count += 1
        return True

    service = dl_app_api_base.ReadinessService(
        subsystems=[dl_app_api_base.SubsystemReadinessSyncCallback(name="sub1", is_ready=check)],
        ttl=datetime.timedelta(seconds=10),
    )
    await service.get_all_statuses()
    await service.get_all_statuses()
    assert call_count == 1


@pytest.mark.asyncio
async def test_no_cache_with_zero_ttl() -> None:
    call_count = 0

    def check() -> bool:
        nonlocal call_count
        call_count += 1
        return True

    service = dl_app_api_base.ReadinessService(
        subsystems=[dl_app_api_base.SubsystemReadinessSyncCallback(name="sub1", is_ready=check)],
        ttl=datetime.timedelta(seconds=0),
    )
    await service.get_all_statuses()
    await service.get_all_statuses()
    assert call_count == 2
