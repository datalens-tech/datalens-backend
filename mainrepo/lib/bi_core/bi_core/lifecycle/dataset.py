from __future__ import annotations

import logging

from bi_constants.enums import DataSourceRole

from bi_core.lifecycle.base import EntryLifecycleManager
from bi_core.us_dataset import Dataset
from bi_core.data_source_spec.collection import DataSourceCollectionSpec
from bi_core.components.editor import DatasetComponentEditor


LOGGER = logging.getLogger(__name__)


class DatasetLifecycleManager(EntryLifecycleManager[Dataset]):
    ENTRY_CLS = Dataset

    def pre_save_hook(self) -> None:
        super().pre_save_hook()

        self.entry._dump_rls()

        try:
            self.entry.links = self.collect_links()
        except Exception:
            LOGGER.exception('Failed to collect links for ds %s', self.entry.uuid)

    def post_copy_hook(self) -> None:
        super().post_copy_hook()

        own_collection_specs = [
            dsrc_coll_spec for dsrc_coll_spec in self.entry.data.source_collections
            if isinstance(dsrc_coll_spec, DataSourceCollectionSpec)
        ]

        ds_editor = DatasetComponentEditor(dataset=self.entry)
        for dsrc_coll_spec in own_collection_specs:
            ds_editor.remove_data_source(
                source_id=dsrc_coll_spec.id, role=DataSourceRole.materialization,
                delete_mat_table=False)
            ds_editor.remove_data_source(
                source_id=dsrc_coll_spec.id, role=DataSourceRole.sample,
                delete_mat_table=False)

    def collect_links(self) -> dict[str, str]:
        links = {}
        for dsrc_coll_spec in self.entry.data.source_collections:
            links.update(dsrc_coll_spec.collect_links())

        return links
