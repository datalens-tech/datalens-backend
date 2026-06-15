import datetime

import pytest

import dl_app_api_base
import dl_prometheus


@pytest.fixture(name="readiness_subsystem_status")
def fixture_readiness_subsystem_status() -> dl_app_api_base.ReadinessSubsystemStatus:
    gauge = dl_app_api_base.ReadinessSubsystemStatus.from_settings(dl_app_api_base.ReadinessSubsystemStatusSettings())
    # Registering the gauge with a registry is required before .set() can be called.
    dl_prometheus.MetricsRegistry(metrics=(gauge,))
    return gauge


def _sample_value(
    metrics_registry: dl_prometheus.MetricsRegistryProtocol,
    name: str,
    labels: dict[str, str],
) -> float | None:
    for sample in metrics_registry.get_samples():
        if sample.name == name and sample.labels == labels:
            return sample.value
    return None


@pytest.mark.asyncio
async def test_get_all_statuses_all_healthy(
    readiness_subsystem_status: dl_app_api_base.ReadinessSubsystemStatus,
) -> None:
    service = dl_app_api_base.ReadinessService(
        subsystems=[
            dl_app_api_base.SubsystemReadinessSyncCallback(name="sub1", is_ready=lambda: True),
            dl_app_api_base.SubsystemReadinessSyncCallback(name="sub2", is_ready=lambda: True),
        ],
        readiness_subsystem_status=readiness_subsystem_status,
    )
    result = await service.get_all_statuses()
    assert result.statuses["sub1"].value is True
    assert result.statuses["sub1"].critical is True
    assert result.statuses["sub2"].value is True


@pytest.mark.asyncio
async def test_get_all_statuses_some_unhealthy(
    readiness_subsystem_status: dl_app_api_base.ReadinessSubsystemStatus,
) -> None:
    service = dl_app_api_base.ReadinessService(
        subsystems=[
            dl_app_api_base.SubsystemReadinessSyncCallback(name="sub1", is_ready=lambda: True),
            dl_app_api_base.SubsystemReadinessSyncCallback(name="sub2", is_ready=lambda: False),
        ],
        readiness_subsystem_status=readiness_subsystem_status,
    )
    result = await service.get_all_statuses()
    assert result.statuses["sub1"].value is True
    assert result.statuses["sub2"].value is False


@pytest.mark.asyncio
async def test_get_all_statuses_async_callback(
    readiness_subsystem_status: dl_app_api_base.ReadinessSubsystemStatus,
) -> None:
    async def check() -> bool:
        return True

    service = dl_app_api_base.ReadinessService(
        subsystems=[dl_app_api_base.SubsystemReadinessAsyncCallback(name="async_sub", is_ready=check)],
        readiness_subsystem_status=readiness_subsystem_status,
    )
    result = await service.get_all_statuses()
    assert result.statuses["async_sub"].value is True


@pytest.mark.asyncio
async def test_status_has_request_dt(
    readiness_subsystem_status: dl_app_api_base.ReadinessSubsystemStatus,
) -> None:
    before = datetime.datetime.now(datetime.UTC)
    service = dl_app_api_base.ReadinessService(
        subsystems=[dl_app_api_base.SubsystemReadinessSyncCallback(name="sub1", is_ready=lambda: True)],
        readiness_subsystem_status=readiness_subsystem_status,
    )
    result = await service.get_all_statuses()
    after = datetime.datetime.now(datetime.UTC)
    assert before <= result.statuses["sub1"].request_dt <= after


@pytest.mark.asyncio
async def test_status_has_critical_from_callback(
    readiness_subsystem_status: dl_app_api_base.ReadinessSubsystemStatus,
) -> None:
    service = dl_app_api_base.ReadinessService(
        subsystems=[
            dl_app_api_base.SubsystemReadinessSyncCallback(name="crit", is_ready=lambda: True, critical=True),
            dl_app_api_base.SubsystemReadinessSyncCallback(name="non_crit", is_ready=lambda: True, critical=False),
        ],
        readiness_subsystem_status=readiness_subsystem_status,
    )
    result = await service.get_all_statuses()
    assert result.statuses["crit"].critical is True
    assert result.statuses["non_crit"].critical is False


@pytest.mark.asyncio
async def test_is_ready_true(
    readiness_subsystem_status: dl_app_api_base.ReadinessSubsystemStatus,
) -> None:
    service = dl_app_api_base.ReadinessService(
        subsystems=[
            dl_app_api_base.SubsystemReadinessSyncCallback(name="sub1", is_ready=lambda: True),
            dl_app_api_base.SubsystemReadinessSyncCallback(name="sub2", is_ready=lambda: True),
        ],
        readiness_subsystem_status=readiness_subsystem_status,
    )
    statuses = await service.get_all_statuses()
    assert statuses.is_ready() is True


@pytest.mark.asyncio
async def test_is_ready_false(
    readiness_subsystem_status: dl_app_api_base.ReadinessSubsystemStatus,
) -> None:
    service = dl_app_api_base.ReadinessService(
        subsystems=[
            dl_app_api_base.SubsystemReadinessSyncCallback(name="sub1", is_ready=lambda: True),
            dl_app_api_base.SubsystemReadinessSyncCallback(name="sub2", is_ready=lambda: False),
        ],
        readiness_subsystem_status=readiness_subsystem_status,
    )
    statuses = await service.get_all_statuses()
    assert statuses.is_ready() is False


@pytest.mark.asyncio
async def test_is_critical_ready_with_non_critical_failure(
    readiness_subsystem_status: dl_app_api_base.ReadinessSubsystemStatus,
) -> None:
    service = dl_app_api_base.ReadinessService(
        subsystems=[
            dl_app_api_base.SubsystemReadinessSyncCallback(name="critical_sub", is_ready=lambda: True, critical=True),
            dl_app_api_base.SubsystemReadinessSyncCallback(
                name="non_critical_sub", is_ready=lambda: False, critical=False
            ),
        ],
        readiness_subsystem_status=readiness_subsystem_status,
    )
    statuses = await service.get_all_statuses()
    assert statuses.is_critical_ready() is True


@pytest.mark.asyncio
async def test_is_critical_ready_with_critical_failure(
    readiness_subsystem_status: dl_app_api_base.ReadinessSubsystemStatus,
) -> None:
    service = dl_app_api_base.ReadinessService(
        subsystems=[
            dl_app_api_base.SubsystemReadinessSyncCallback(name="critical_sub", is_ready=lambda: False, critical=True),
            dl_app_api_base.SubsystemReadinessSyncCallback(
                name="non_critical_sub", is_ready=lambda: True, critical=False
            ),
        ],
        readiness_subsystem_status=readiness_subsystem_status,
    )
    statuses = await service.get_all_statuses()
    assert statuses.is_critical_ready() is False


@pytest.mark.asyncio
async def test_cached_within_ttl(
    readiness_subsystem_status: dl_app_api_base.ReadinessSubsystemStatus,
) -> None:
    call_count = 0

    def check() -> bool:
        nonlocal call_count
        call_count += 1
        return True

    service = dl_app_api_base.ReadinessService(
        subsystems=[dl_app_api_base.SubsystemReadinessSyncCallback(name="sub1", is_ready=check)],
        ttl=datetime.timedelta(seconds=10),
        readiness_subsystem_status=readiness_subsystem_status,
    )
    await service.get_all_statuses()
    await service.get_all_statuses()
    assert call_count == 1


@pytest.mark.asyncio
async def test_no_cache_with_zero_ttl(
    readiness_subsystem_status: dl_app_api_base.ReadinessSubsystemStatus,
) -> None:
    call_count = 0

    def check() -> bool:
        nonlocal call_count
        call_count += 1
        return True

    service = dl_app_api_base.ReadinessService(
        subsystems=[dl_app_api_base.SubsystemReadinessSyncCallback(name="sub1", is_ready=check)],
        ttl=datetime.timedelta(seconds=0),
        readiness_subsystem_status=readiness_subsystem_status,
    )
    await service.get_all_statuses()
    await service.get_all_statuses()
    assert call_count == 2


@pytest.mark.asyncio
async def test_readiness_subsystem_status_gauge_reflects_statuses() -> None:
    gauge = dl_app_api_base.ReadinessSubsystemStatus.from_settings(dl_app_api_base.ReadinessSubsystemStatusSettings())
    metrics_registry = dl_prometheus.MetricsRegistry(metrics=(gauge,))

    service = dl_app_api_base.ReadinessService(
        subsystems=[
            dl_app_api_base.SubsystemReadinessSyncCallback(name="crit", is_ready=lambda: True, critical=True),
            dl_app_api_base.SubsystemReadinessSyncCallback(name="non_crit", is_ready=lambda: False, critical=False),
        ],
        readiness_subsystem_status=gauge,
    )

    await service.get_all_statuses()

    assert (
        _sample_value(metrics_registry, "readiness_subsystem_status", {"subsystem": "crit", "critical": "true"}) == 1.0
    )
    assert (
        _sample_value(metrics_registry, "readiness_subsystem_status", {"subsystem": "non_crit", "critical": "false"})
        == 0.0
    )


@pytest.mark.asyncio
async def test_readiness_subsystem_status_gauge_not_updated_within_ttl() -> None:
    gauge = dl_app_api_base.ReadinessSubsystemStatus.from_settings(dl_app_api_base.ReadinessSubsystemStatusSettings())
    metrics_registry = dl_prometheus.MetricsRegistry(metrics=(gauge,))

    ready = True

    service = dl_app_api_base.ReadinessService(
        subsystems=[dl_app_api_base.SubsystemReadinessSyncCallback(name="sub1", is_ready=lambda: ready)],
        ttl=datetime.timedelta(seconds=10),
        readiness_subsystem_status=gauge,
    )

    await service.get_all_statuses()
    ready = False
    await service.get_all_statuses()

    # Within the TTL the subsystem is not re-checked, so the gauge keeps the cached value.
    assert (
        _sample_value(metrics_registry, "readiness_subsystem_status", {"subsystem": "sub1", "critical": "true"}) == 1.0
    )


def test_readiness_subsystem_status_set_from_subsystem_status() -> None:
    gauge = dl_app_api_base.ReadinessSubsystemStatus.from_settings(dl_app_api_base.ReadinessSubsystemStatusSettings())
    metrics_registry = dl_prometheus.MetricsRegistry(metrics=(gauge,))

    gauge.set_from_subsystem_status(
        "sub1",
        dl_app_api_base.SubsystemStatus(value=False, critical=True, request_dt=datetime.datetime.now(datetime.UTC)),
    )

    assert (
        _sample_value(metrics_registry, "readiness_subsystem_status", {"subsystem": "sub1", "critical": "true"}) == 0.0
    )
