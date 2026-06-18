import unittest.mock

import pytest

from dl_api_lib.utils.base import need_delete_permission_on_entry
from dl_core.base_models import (
    CollectionEntryLocation,
    EntryLocation,
    PathEntryLocation,
    WorkbookEntryLocation,
)
from dl_core.exc import USPermissionRequiredError


def _make_entry(entry_key: EntryLocation, *, edit: bool, admin: bool) -> unittest.mock.MagicMock:
    entry = unittest.mock.MagicMock()
    entry.entry_key = entry_key
    entry.permissions = {"edit": edit, "admin": admin}
    entry.uuid = "test-uuid"
    return entry


# --- workbook entries ---


def test_workbook_entry_allowed_with_edit_only() -> None:
    entry = _make_entry(WorkbookEntryLocation(workbook_id="wb1", entry_name="ds"), edit=True, admin=False)
    need_delete_permission_on_entry(entry)  # must not raise


def test_workbook_entry_denied_without_edit() -> None:
    entry = _make_entry(WorkbookEntryLocation(workbook_id="wb1", entry_name="ds"), edit=False, admin=False)
    with pytest.raises(USPermissionRequiredError):
        need_delete_permission_on_entry(entry)


# --- non-workbook entries (path / collection) ---


def test_path_entry_allowed_with_admin() -> None:
    entry = _make_entry(PathEntryLocation(path="root/ds"), edit=True, admin=True)
    need_delete_permission_on_entry(entry)  # must not raise


def test_path_entry_denied_without_admin() -> None:
    entry = _make_entry(PathEntryLocation(path="root/ds"), edit=True, admin=False)
    with pytest.raises(USPermissionRequiredError):
        need_delete_permission_on_entry(entry)


def test_collection_entry_denied_without_admin() -> None:
    entry = _make_entry(CollectionEntryLocation(collection_id="coll1", entry_name="ds"), edit=True, admin=False)
    with pytest.raises(USPermissionRequiredError):
        need_delete_permission_on_entry(entry)
