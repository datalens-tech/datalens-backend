import asyncio
from typing import Any, AsyncIterable, Callable, Dict, Tuple, Optional

import attr

from bi_api_client.dsmaker.primitives import Dataset
from bi_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1

from bi_core.maintenance.us_crawler_base import USEntryCrawler
from bi_core.us_entry import USEntry, USMigrationEntry
from bi_core.us_manager.us_manager_async import AsyncUSManager

from bi_utils.task_runner import TaskRunner, ImmediateTaskRunner

from bi_api_lib.maintenance.crawler_runner import run_crawler


@attr.s
class SimpleCrawler(USEntryCrawler):
    ENTRY_TYPE = USMigrationEntry

    _target_dataset_id: str = attr.ib(kw_only=True)
    _new_avatar_title: str = attr.ib(kw_only=True)

    def get_raw_entry_iterator(self, crawl_all_tenants: bool = True) -> AsyncIterable[Dict[str, Any]]:
        usm = self._usm
        assert usm is not None
        return usm.get_raw_collection(entry_scope='dataset', all_tenants=crawl_all_tenants)

    async def process_entry_get_save_flag(self, entry: USEntry, logging_extra: Dict[str, Any], usm: Optional[AsyncUSManager] = None) -> Tuple[bool, str]:
        if entry.uuid == self._target_dataset_id:
            avatars = entry.data['source_avatars']
            avatars[0]['title'] = self._new_avatar_title
            return True, 'Found target dataset'

        return False, '...'


def _check_crawler_runner(
        api_v1: SyncHttpDatasetApiV1, loop: asyncio.AbstractEventLoop,
        dataset_id: str, usm: AsyncUSManager,
        task_runner_factory: Callable[[], TaskRunner],
):
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset
    old_title = ds.source_avatars[0].title

    crawler = SimpleCrawler(
        target_dataset_id=dataset_id, new_avatar_title='First',
        task_runner=task_runner_factory(), dry_run=True,
    )
    loop.run_until_complete(run_crawler(crawler, usm=usm))

    ds = api_v1.load_dataset(dataset=ds).dataset
    assert ds.source_avatars[0].title == old_title

    crawler = SimpleCrawler(
        target_dataset_id=dataset_id, new_avatar_title='Second',
        task_runner=task_runner_factory(), dry_run=False,
    )
    loop.run_until_complete(run_crawler(crawler, usm=usm))

    ds = api_v1.load_dataset(dataset=ds).dataset
    assert ds.source_avatars[0].title == 'Second'


def test_crawler_runner_with_immediate_task_runner(api_v1, loop, dataset_id, default_async_usm_per_test):
    _check_crawler_runner(
        api_v1=api_v1, loop=loop, dataset_id=dataset_id,
        usm=default_async_usm_per_test,
        task_runner_factory=ImmediateTaskRunner,
    )
