import pytest

from dl_api_connector.api_schema.top_level import resolve_entry_loc_from_api_req_body
from dl_core.base_models import (
    CollectionEntryLocation,
    PathEntryLocation,
    WorkbookEntryLocation,
)


class TestEntryLocationValidation:
    """
    Test validation logic for entry location resolution, specifically collection_id and workbook_id constraints
    """

    def test_resolve_entry_loc_workbook_id_only(self):
        """
        Test that workbook_id alone creates WorkbookEntryLocation
        """
        result = resolve_entry_loc_from_api_req_body(
            name="test_entry",
            dir_path=None,
            collection_id=None,
            workbook_id="wb_123",
        )

        assert isinstance(result, WorkbookEntryLocation)
        assert result.workbook_id == "wb_123"
        assert result.entry_name == "test_entry"

    def test_resolve_entry_loc_collection_id_only(self):
        """
        Test that collection_id alone creates CollectionEntryLocation
        """
        result = resolve_entry_loc_from_api_req_body(
            name="test_entry",
            dir_path=None,
            collection_id="coll_456",
            workbook_id=None,
        )

        assert isinstance(result, CollectionEntryLocation)
        assert result.collection_id == "coll_456"
        assert result.entry_name == "test_entry"

    def test_resolve_entry_loc_dir_path_only(self):
        """
        Test that dir_path alone creates PathEntryLocation
        """
        result = resolve_entry_loc_from_api_req_body(
            name="test_entry",
            dir_path="/some/path",
            collection_id=None,
            workbook_id=None,
        )

        assert isinstance(result, PathEntryLocation)
        assert result.path == "/some/path/test_entry"

    def test_resolve_entry_loc_both_ids_set_error(self):
        """
        Test that setting both workbook_id and collection_id raises AssertionError
        """
        with pytest.raises(AssertionError, match="collection_id and workbook_id can not both be set"):
            resolve_entry_loc_from_api_req_body(
                name="test_entry",
                dir_path=None,
                collection_id="coll_456",
                workbook_id="wb_123",
            )

    def test_resolve_entry_loc_no_location_info_error(self):
        """
        Test that providing no location information raises AssertionError
        """
        with pytest.raises(AssertionError, match="dir_path can not be None"):
            resolve_entry_loc_from_api_req_body(
                name="test_entry",
                dir_path=None,
                collection_id=None,
                workbook_id=None,
            )

    def test_resolve_entry_loc_name_none_error(self):
        """
        Test that None name raises AssertionError
        """
        with pytest.raises(AssertionError, match="name can not be None"):
            resolve_entry_loc_from_api_req_body(
                name=None,
                dir_path="/some/path",
                collection_id=None,
                workbook_id=None,
            )

    def test_resolve_entry_loc_collection_id_priority_over_dir_path(self):
        """
        Test that collection_id takes priority over dir_path when both are provided
        """
        result = resolve_entry_loc_from_api_req_body(
            name="test_entry",
            dir_path="/some/path",
            collection_id="coll_456",
            workbook_id=None,
        )

        assert isinstance(result, CollectionEntryLocation)
        assert result.collection_id == "coll_456"
        assert result.entry_name == "test_entry"

    def test_resolve_entry_loc_workbook_id_priority_over_dir_path(self):
        """
        Test that workbook_id takes priority over dir_path when both are provided
        """
        result = resolve_entry_loc_from_api_req_body(
            name="test_entry",
            dir_path="/some/path",
            collection_id=None,
            workbook_id="wb_123",
        )

        assert isinstance(result, WorkbookEntryLocation)
        assert result.workbook_id == "wb_123"
        assert result.entry_name == "test_entry"
