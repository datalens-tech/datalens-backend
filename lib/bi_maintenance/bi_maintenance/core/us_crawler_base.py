from __future__ import annotations

import contextlib
import copy
import enum
import logging
import time
from typing import Any, AsyncGenerator, AsyncIterable, ClassVar, Optional, Sequence, Type

import attr
import shortuuid

from dl_api_commons.logging import extra_with_evt_code, format_dict
from dl_api_commons.base_models import TenantDef
from dl_core.us_entry import USEntry, USMigrationEntry
from bi_maintenance.diff_utils import get_pre_save_top_level_dict, get_diff_text
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_utils.task_runner import TaskRunner, ConcurrentTaskRunner

LOGGER = logging.getLogger(__name__)


class EntryHandlingResult(enum.Enum):
    SUCCESS = enum.auto()
    FAILED = enum.auto()
    SKIPPED = enum.auto()


# Logging event code
EVT_CODE_RUN_START = 'us_entry_crawler_run_start'
EVT_CODE_ENTRY_HANDLING_START = 'us_entry_crawler_entry_handling_start'
EVT_CODE_ENTRY_PROCESSING_SUCCESS = 'us_entry_crawler_entry_processing_success'
EVT_CODE_DIFF_CALC_EXC = 'us_entry_crawler_entry_diff_calc_exc'
EVT_CODE_ENTRY_HANDLING_END = 'us_entry_crawler_entry_handling_done'
EVT_CODE_RUN_END = 'us_entry_crawler_run_end'


@attr.s
class USEntryCrawler:
    ENTRY_TYPE: ClassVar[Type[USEntry]] = None  # type: ignore  # TODO: fix  # Must be set in subclass

    _dry_run: bool = attr.ib()  # should always be specified explicitly.
    _usm: Optional[AsyncUSManager] = attr.ib(default=None)
    _target_tenant: Optional[TenantDef] = attr.ib(default=None)
    _concurrency_limit: int = attr.ib(default=10)
    # internals
    _run_fired: bool = attr.ib(init=False, default=False)
    _run_id: str = attr.ib(init=False, factory=shortuuid.uuid)
    _task_runner: TaskRunner = attr.ib(kw_only=True,
                                       default=attr.Factory(lambda self: ConcurrentTaskRunner(self._concurrency_limit),
                                                            takes_self=True))

    def __attrs_post_init__(self) -> None:
        if self._usm is not None:
            self.ensure_usm_settings_fix_if_possible(self._usm)

    @property
    def usm(self) -> AsyncUSManager:
        assert self._usm is not None
        return self._usm

    @property
    def type_name(self) -> str:
        return type(self).__qualname__

    @property
    def run_id(self) -> str:
        return self._run_id

    def ensure_usm_settings_fix_if_possible(self, usm: AsyncUSManager) -> None:
        if self._target_tenant:
            # Setting tenant override is required to use regular listing method instead of inter-tenant
            usm.set_tenant_override(self._target_tenant)

    def set_usm(self, usm: AsyncUSManager) -> None:
        if self._usm is not None:
            raise ValueError("USManager already set for this crawler")
        self.ensure_usm_settings_fix_if_possible(usm)
        self._usm = usm

    def copy(self, reset_usm: bool = True) -> USEntryCrawler:
        changes: dict[str, Any] = {}
        if reset_usm:
            changes.update(usm=None)
        return attr.evolve(self, **changes)

    def get_raw_entry_iterator(self, crawl_all_tenants: bool) -> AsyncIterable[dict[str, Any]]:
        """
        Must returns iterable of US responses for targets to crawl.
        Should be implemented in subclasses.
        """
        raise NotImplementedError()

    async def process_entry_get_save_flag(self, entry: USEntry, logging_extra: dict[str, Any], usm: Optional[AsyncUSManager] = None) -> tuple[bool, str]:
        raise NotImplementedError()

    @contextlib.asynccontextmanager
    async def locked_entry_cm(
            self, entry_id: str, logging_extra: dict[str, Any], usm: AsyncUSManager,
    ) -> AsyncGenerator[Optional[USEntry], None]:
        if self._dry_run:
            try:
                entry = await usm.get_by_id(entry_id, expected_type=self.ENTRY_TYPE)  # type: ignore  # TODO: fix
            except Exception:
                logging_extra.update(us_entry_crawler_exc_stage='entry_locked_load')
                raise
            else:
                yield entry
        else:
            async with contextlib.AsyncExitStack() as cmstack:
                try:
                    entry = await cmstack.enter_async_context(
                        usm.locked_entry_cm(
                            expected_type=self.ENTRY_TYPE, entry_id=entry_id,
                            duration_sec=30, wait_timeout_sec=60,
                        )
                    )
                except Exception:
                    logging_extra.update(us_entry_crawler_exc_stage='entry_locked_load')
                    raise
                else:
                    yield entry

    async def save_entry(self, entry: USEntry, usm: AsyncUSManager) -> None:
        if self._dry_run:
            return
        else:
            await usm.save(entry)

    async def run(self) -> None:
        if self._run_fired:
            raise ValueError("Attempt to consequent call of USEntryCrawler.run()")
        self._run_fired = True

        crawler_run_extra: dict[str, Any] = dict(
            us_entry_crawler_name=self.type_name,
            us_entry_crawler_dry_run=self._dry_run,
            us_entry_crawler_run_id=self._run_id,
        )
        LOGGER.info(
            "Starting US entry crawler run: %s",
            format_dict(
                crawler_run_extra, name='us_entry_crawler_name',
                run_id='us_entry_crawler_run_id', is_dry_run='us_entry_crawler_dry_run',
            ),
            extra=extra_with_evt_code(EVT_CODE_RUN_START, crawler_run_extra)
        )

        started_ts = time.monotonic()

        try:
            map_handling_status_entry_id = await self._run(crawler_run_extra)
        except Exception:  # noqa
            crawler_run_extra.update(us_entry_crawler_run_success=False)
            LOGGER.exception(
                "Crawler run failure: %s %s", self.type_name, self._run_id,
                extra=extra_with_evt_code(EVT_CODE_RUN_END, crawler_run_extra),
            )
        else:
            time_elapsed = int(round(time.monotonic() - started_ts))
            # TODO FIX: Add availability to define post-run hook in subclass
            counters = {key: len(val) for key, val in map_handling_status_entry_id.items()}

            crawler_run_extra.update(
                us_entry_crawler_run_success=True,
                us_entry_crawler_run_cnt_processed=counters[EntryHandlingResult.SUCCESS],
                us_entry_crawler_run_cnt_skipped=counters[EntryHandlingResult.SKIPPED],
                us_entry_crawler_run_cnt_failed=counters[EntryHandlingResult.FAILED],
                us_entry_crawler_run_cnt_total=sum(counters.values()),
                us_entry_crawler_run_time_elapsed=time_elapsed,
            )

            LOGGER.info(
                "Crawler run finished successfully: %s",
                format_dict(
                    crawler_run_extra,
                    total='us_entry_crawler_run_cnt_total',
                    processed='us_entry_crawler_run_cnt_processed',
                    skipped='us_entry_crawler_run_cnt_skipped',
                    failed='us_entry_crawler_run_cnt_failed',
                    time_elapsed='us_entry_crawler_run_time_elapsed',
                ),
                extra=extra_with_evt_code(EVT_CODE_RUN_END, crawler_run_extra),
            )

    async def _run(self, crawler_run_extra: dict[str, Any]) -> dict[EntryHandlingResult, Sequence[str]]:
        entry_id_distribution: dict[EntryHandlingResult, list[str]] = {
            result_code: [] for result_code in EntryHandlingResult
        }

        await self._task_runner.initialize()

        entry_idx = 0
        async for raw_entry in self.get_raw_entry_iterator(crawl_all_tenants=(self._target_tenant is None)):
            await self._task_runner.schedule(
                self.single_entry_run(
                    entry_idx=entry_idx, raw_entry=raw_entry,
                    crawler_run_extra=crawler_run_extra,
                    entry_id_distribution=entry_id_distribution,
                )
            )

            entry_idx += 1

        await self._task_runner.finalize()

        return entry_id_distribution  # type: ignore

    async def single_entry_run(
            self, entry_idx: int, raw_entry: dict, crawler_run_extra: dict,
            entry_id_distribution: dict[EntryHandlingResult, list[str]],
    ) -> None:
        entry_id = raw_entry['entryId']
        entry_handling_extra = dict(
            crawler_run_extra,
            us_entry_id=entry_id,
            us_entry_key=raw_entry['key'],
            us_entry_tenant_id=raw_entry.get('tenantId', None),
            us_entry_type=raw_entry['type'],
            us_entry_scope=raw_entry['scope'],
            us_entry_crawler_idx=entry_idx,
        )
        LOGGER.info(
            "Crawler going to handle entry: %s", entry_id,
            extra=extra_with_evt_code(EVT_CODE_ENTRY_HANDLING_START, entry_handling_extra)
        )
        try:
            result = await self._handle_single_entry(
                raw_entry=raw_entry,
                entry_handling_extra=entry_handling_extra,
            )
            entry_handling_extra.update(entry_handling_status=result.name)
        except Exception:  # noqa
            result = EntryHandlingResult.FAILED
            entry_handling_extra.update(entry_handling_status=result.name)
            LOGGER.exception(
                "Unexpected exception during handling entry %s", entry_id,
                extra=extra_with_evt_code(EVT_CODE_ENTRY_HANDLING_END, entry_handling_extra)
            )
        else:
            LOGGER.info(
                "Crawler complete entry handling: %s %s", entry_id, result.name,
                extra=extra_with_evt_code(EVT_CODE_ENTRY_HANDLING_END, entry_handling_extra)
            )

        entry_id_distribution[result].append(entry_id)

    async def _handle_single_entry(
            self, raw_entry: dict[str, Any], entry_handling_extra: dict[str, Any]
    ) -> EntryHandlingResult:
        entry_id = raw_entry['entryId']
        usm = self.usm
        async with self.locked_entry_cm(entry_id, entry_handling_extra, usm=usm) as target_entry:  # type: ignore  # TODO: fix
            assert target_entry is not None
            # For future diff calculation reliability
            # (see `bi_maintenance.diff_utils.get_pre_save_top_level_dict`)
            assert target_entry._us_resp is not None
            us_resp: dict = copy.deepcopy(target_entry._us_resp)
            target_entry._us_resp = us_resp

            entry_handling_extra.update(
                us_entry_rev=us_resp['revId'],
            )
            try:
                need_save, processing_msg = await self.process_entry_get_save_flag(
                    target_entry,
                    logging_extra=dict(entry_handling_extra),
                    usm=usm,
                )
            except Exception:
                entry_handling_extra.update(us_entry_crawler_exc_stage='entry_processing')
                raise

            target_entry_diff_str = self._calculate_diff_str(target_entry, entry_handling_extra)
            entry_handling_extra.update(
                us_entry_crawler_need_save=need_save,
                us_entry_crawler_proc_msg=processing_msg,
                us_entry_crawler_entry_diff=target_entry_diff_str
            )

            diff_str = get_diff_text(target_entry, us_manager=usm)
            if diff_str:
                diff_str = '\n' + diff_str
            LOGGER.info(
                "Entry was processed by crawler: %s%s",
                format_dict(
                    entry_handling_extra,
                    idx='us_entry_crawler_idx', entry_id='us_entry_id', scope='us_entry_scope', type='us_entry_type',
                    need_save='us_entry_crawler_need_save', msg='us_entry_crawler_proc_msg',
                    diff='us_entry_crawler_entry_diff',
                ),
                diff_str,
                extra=extra_with_evt_code(EVT_CODE_ENTRY_PROCESSING_SUCCESS, entry_handling_extra)
            )

            try:
                if need_save:
                    await self.save_entry(target_entry, usm=usm)
                    return EntryHandlingResult.SUCCESS
                else:
                    return EntryHandlingResult.SKIPPED

            except Exception:
                entry_handling_extra.update(us_entry_crawler_exc_stage='entry_save')
                raise

    @staticmethod
    def _calculate_diff_str(target_entry: USEntry, entry_handling_extra: dict[str, Any]) -> Optional[str]:
        try:
            if isinstance(target_entry, USMigrationEntry):
                entry_diff = get_pre_save_top_level_dict(target_entry)
            else:
                return 'N/A'
        except Exception:  # noqa
            LOGGER.warning(
                "Exception during diff calculation",
                extra=extra_with_evt_code(EVT_CODE_DIFF_CALC_EXC, entry_handling_extra)
            )
            return 'N/A'

        try:
            return entry_diff.short_str()
        except Exception:  # noqa
            LOGGER.warning(
                "Can not pretty stringify diff for entry: %s", target_entry.uuid,
                extra=extra_with_evt_code(EVT_CODE_DIFF_CALC_EXC, entry_handling_extra)
            )
            return str(entry_diff)
