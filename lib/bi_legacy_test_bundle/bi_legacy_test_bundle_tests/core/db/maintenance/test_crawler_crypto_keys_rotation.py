from cryptography import fernet
import pytest
import shortuuid

from bi_maintenance.core.crawlers.crypto_keys_rotation import RotateCryptoKeyInConnection
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_core.base_models import PathEntryLocation
from dl_core.us_entry import USMigrationEntry
from dl_core.us_manager.crypto.main import CryptoController
from dl_core.us_manager.us_manager_sync_mock import MockedSyncUSManager
from dl_utils.aio import await_sync

_CRYPTO_KEYS_CONFIG = CryptoKeysConfig(
    map_id_key=dict(
        old=fernet.Fernet.generate_key().decode("ascii"),
        new=fernet.Fernet.generate_key().decode("ascii"),
    ),
    actual_key_id="new",
)

_CRYPTO_CONTROLLER = CryptoController(_CRYPTO_KEYS_CONFIG)


def _get_ok_message():
    return "All sensitive fields are encrypted with actual keys"


def _get_non_actual_key_message(*fields):
    return f"Some sensitive fields are encrypted with not actual keys: {list(sorted(fields))}"


_DEFAULT_PG_CONN_DATA = dict(
    host="127.0.0.1",
    port=6432,
    db_name="db1",
    username="user1",
    table_name=None,
    cache_ttl_sec=None,
    raw_sql_level="off",
    mdb_cluster_id=None,
    mdb_folder_id=None,
    enforce_collate="auto",
    sample_table_name=None,
)
_DEFAULT_PG_CONN_META = dict(
    state="saved",
    version=11,
    mdb_cluster_id=None,
    mdb_folder_id=None,
)
_BASE_PG_CONN_ENTRY_DATA = dict(
    scope="connection",
    type="postgres",
    data=_DEFAULT_PG_CONN_DATA,
    meta=_DEFAULT_PG_CONN_META,
)

_DEFAULT_GSHEETS_V2_CONN_DATA = dict(
    sources=[],
    refresh_enabled=True,
)
_DEFAULT_GSHEETS_V2_CONN_META = dict(
    state="saved",
    version=11,
    mdb_cluster_id=None,
    mdb_folder_id=None,
)
_BASE_GSHEETS_V2_CONN_ENTRY_DATA = dict(
    scope="connection",
    type="gsheets_v2",
    data=_DEFAULT_GSHEETS_V2_CONN_DATA,
    meta=_DEFAULT_GSHEETS_V2_CONN_META,
)


@pytest.mark.parametrize(
    "entry_data,expected_sensitive_fields,expected_should_save,expected_message",
    [
        # Default case with actual key
        (
            dict(
                **_BASE_PG_CONN_ENTRY_DATA,
                unversioned_data=dict(
                    password=_CRYPTO_CONTROLLER.encrypt("new", "some_password"),
                ),
            ),
            ("password",),
            False,
            _get_ok_message(),
        ),
        # Single non actual fields case
        (
            dict(
                **_BASE_PG_CONN_ENTRY_DATA,
                unversioned_data=dict(
                    password=_CRYPTO_CONTROLLER.encrypt("old", "some_password"),
                ),
            ),
            ("password",),
            True,
            _get_non_actual_key_message("password"),
        ),
        # `None` in sensitive field
        (
            dict(
                **_BASE_PG_CONN_ENTRY_DATA,
                unversioned_data=dict(
                    password=None,
                ),
            ),
            ("password",),
            False,
            _get_ok_message(),
        ),
        # Missing sensitive field
        (
            dict(
                **_BASE_PG_CONN_ENTRY_DATA,
                unversioned_data=dict(),
            ),
            ("password",),
            False,
            _get_ok_message(),
        ),
        # # # attrs DataModel
        # Default case with actual key
        (
            dict(
                **_BASE_GSHEETS_V2_CONN_ENTRY_DATA,
                unversioned_data=dict(
                    refresh_token=_CRYPTO_CONTROLLER.encrypt("new", "some_token"),
                ),
            ),
            ("refresh_token",),
            False,
            _get_ok_message(),
        ),
        # Single non actual fields case
        (
            dict(
                **_BASE_GSHEETS_V2_CONN_ENTRY_DATA,
                unversioned_data=dict(
                    refresh_token=_CRYPTO_CONTROLLER.encrypt("old", "some_token"),
                ),
            ),
            ("refresh_token",),
            True,
            _get_non_actual_key_message("refresh_token"),
        ),
        # `None` in sensitive field
        (
            dict(
                **_BASE_GSHEETS_V2_CONN_ENTRY_DATA,
                unversioned_data=dict(
                    refresh_token=None,
                ),
            ),
            ("refresh_token",),
            False,
            _get_ok_message(),
        ),
        # Missing sensitive field
        (
            dict(
                **_BASE_GSHEETS_V2_CONN_ENTRY_DATA,
                unversioned_data=dict(),
            ),
            ("refresh_token",),
            False,
            _get_ok_message(),
        ),
    ],
)
def test_process_entry_get_save_flag(
    entry_data,
    expected_sensitive_fields,
    expected_should_save,
    expected_message,
    caplog,
    bi_context,
    default_service_registry,
):
    usm = MockedSyncUSManager(
        bi_context=bi_context,
        crypto_keys_config=_CRYPTO_KEYS_CONFIG,
        services_registry=default_service_registry,
    )

    def get_sensitive_fields_decrypted_values(the_entry: USMigrationEntry):
        return {
            f_name: _CRYPTO_CONTROLLER.decrypt(the_entry.unversioned_data[f_name])
            for f_name in usm.get_sensitive_fields_key_info(the_entry)
            if f_name in the_entry.unversioned_data
        }

    def ensure_unversioned_data_is_sync(the_entry: USMigrationEntry):
        assert dict(the_entry.unversioned_data) == the_entry._us_resp["unversionedData"]

    resp = usm._us_client.create_entry(
        scope=entry_data["scope"],
        type_=entry_data["type"],
        data=entry_data["data"],
        unversioned_data=entry_data["unversioned_data"],
        key=PathEntryLocation(shortuuid.uuid()),
    )
    entry_id = resp["entryId"]

    entry = usm.get_by_id(entry_id, USMigrationEntry)
    # Check that entry can be correctly deserialized
    _ = usm.get_by_id(entry_id)

    ensure_unversioned_data_is_sync(entry)
    sensitive_keys_info = usm.get_sensitive_fields_key_info(entry)
    assert set(expected_sensitive_fields) == set(sensitive_keys_info.keys())
    sensitive_fields_before_update = get_sensitive_fields_decrypted_values(entry)

    crawler = RotateCryptoKeyInConnection(dry_run=False)
    crawler.set_usm(usm)
    actual_should_save, actual_message = await_sync(crawler.process_entry_get_save_flag(entry, {}))

    assert (actual_should_save, actual_message) == (expected_should_save, expected_message)

    # Is needed due to .get_sensitive_fields_key_info(entry) takes key info from US response
    if actual_should_save:
        usm.save(entry)  # Also check that entry can be correctly serialized

    ensure_unversioned_data_is_sync(entry)
    keys_info_after_update = usm.get_sensitive_fields_key_info(entry)

    # Filter not presented fields and fields with `None` in sensitive fields
    presented_fields_key_info = {
        f_name: key_info
        for f_name, key_info in keys_info_after_update.items()
        if key_info is not None and key_info.key_id is not None
    }

    # Check that all key IDs was updates
    assert all([key_info.key_id == "new" for key_info in presented_fields_key_info.values()])

    # Check that decrypted values are not changed
    sensitive_fields_after_update = get_sensitive_fields_decrypted_values(entry)
    assert sensitive_fields_after_update == sensitive_fields_before_update
