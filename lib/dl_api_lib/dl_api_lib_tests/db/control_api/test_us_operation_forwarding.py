import json
from typing import Any
import uuid

import pytest

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_core.us_manager.us_manager_sync import SyncUSManager


SAMPLE_OPERATION = {"id": "op-test-123", "done": False, "metadata": {}}


class TestUsOperationForwarding(DefaultApiTestBase):
    @pytest.fixture(autouse=True)
    def patch_us_operation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        original_save = SyncUSManager._save

        def patched_save(
            us_manager: SyncUSManager,
            entry: Any,
            update_revision: bool | None = None,
            original_entry: Any = None,
        ) -> None:
            original_save(
                self=us_manager,
                entry=entry,
                update_revision=update_revision,
                original_entry=original_entry,
            )
            if entry.operation is None:
                entry._us_resp = {**entry._us_resp, "operation": SAMPLE_OPERATION}

        monkeypatch.setattr(SyncUSManager, "_save", patched_save)

    def test_create_dataset_returns_operation(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        dataset_params: dict,
    ) -> None:
        ds = Dataset()
        ds.sources["s"] = ds.source(connection_id=saved_connection_id, **dataset_params)
        ds.source_avatars["a"] = ds.sources["s"].avatar()
        ds = control_api.apply_updates(dataset=ds).dataset

        resp = control_api.save_dataset(dataset=ds, fail_ok=True)
        assert resp.status_code == 200
        assert resp.json.get("operation") == SAMPLE_OPERATION

    def test_create_connection_returns_operation(
        self,
        control_api: SyncHttpDatasetApiV1,
        enriched_connection_params: dict,
    ) -> None:
        data = dict(
            name=str(uuid.uuid4()),
            type=self.conn_type.name,
            **enriched_connection_params,
        )
        resp = control_api.client.post(
            "/api/v1/connections",
            data=json.dumps(data),
            content_type="application/json",
        )
        assert resp.status_code == 200
        assert resp.json.get("operation") == SAMPLE_OPERATION
