import datetime
import typing
from typing import Self

import attr

import dl_prometheus
import dl_pydantic
import dl_settings


@attr.define(frozen=True, kw_only=True)
class SubsystemReadinessAsyncCallback:
    name: str
    is_ready: typing.Callable[[], typing.Awaitable[bool]]
    critical: bool = True


@attr.define(frozen=True, kw_only=True)
class SubsystemReadinessSyncCallback:
    name: str
    is_ready: typing.Callable[[], bool]
    critical: bool = True


SubsystemReadinessCallback = SubsystemReadinessAsyncCallback | SubsystemReadinessSyncCallback


@attr.define(frozen=True, kw_only=True)
class SubsystemStatus:
    value: bool
    critical: bool
    request_dt: datetime.datetime


class SubsystemStatusSchema(dl_pydantic.BaseSchema):
    value: bool
    critical: bool

    @classmethod
    def from_dataclass(cls, status: SubsystemStatus) -> Self:
        return cls(value=status.value, critical=status.critical)


@attr.define(frozen=True, kw_only=True)
class SubsystemStatuses:
    statuses: dict[str, SubsystemStatus]

    def is_ready(self) -> bool:
        return all(s.value for s in self.statuses.values())

    def is_critical_ready(self) -> bool:
        return all(s.value for s in self.statuses.values() if s.critical)


class ReadinessSubsystemStatusSettings(dl_settings.BaseSettings):
    pass


class ReadinessSubsystemStatus(dl_prometheus.Gauge):
    @classmethod
    def from_settings(cls, settings: ReadinessSubsystemStatusSettings) -> Self:
        return cls(
            name="readiness_subsystem_status",
            documentation="Per-subsystem readiness (1=ready, 0=not ready), partitioned by subsystem and criticality",
            labelnames=(
                "subsystem",
                "critical",
            ),
        )

    def set_from_subsystem_status(self, name: str, status: SubsystemStatus) -> None:
        self.set(
            1.0 if status.value else 0.0,
            labels={
                "subsystem": name,
                "critical": str(status.critical).lower(),
            },
        )


@attr.define(kw_only=True)
class ReadinessService:
    subsystems: typing.Sequence[SubsystemReadinessCallback]
    readiness_subsystem_status: ReadinessSubsystemStatus
    ttl: datetime.timedelta = datetime.timedelta(seconds=10)
    _cached_statuses: dict[str, SubsystemStatus] = attr.field(factory=dict, init=False)

    async def _check_subsystem_readiness(self, subsystem: SubsystemReadinessCallback) -> bool:
        if isinstance(subsystem, SubsystemReadinessAsyncCallback):
            return await subsystem.is_ready()
        if isinstance(subsystem, SubsystemReadinessSyncCallback):
            return subsystem.is_ready()
        raise ValueError(f"Unknown subsystem type: {type(subsystem)}")

    async def _update_statuses(self) -> None:
        now = datetime.datetime.now(datetime.UTC)

        for subsystem in self.subsystems:
            cached = self._cached_statuses.get(subsystem.name)
            if cached is not None and cached.request_dt + self.ttl > now:
                continue

            value = await self._check_subsystem_readiness(subsystem)
            status = SubsystemStatus(
                value=value,
                critical=subsystem.critical,
                request_dt=now,
            )
            self._cached_statuses[subsystem.name] = status
            self.readiness_subsystem_status.set_from_subsystem_status(subsystem.name, status)

    async def get_all_statuses(self) -> SubsystemStatuses:
        await self._update_statuses()
        return SubsystemStatuses(statuses=dict(self._cached_statuses))
