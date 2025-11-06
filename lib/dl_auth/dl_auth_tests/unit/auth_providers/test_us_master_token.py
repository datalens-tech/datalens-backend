import dl_auth


def test_us_master_token() -> None:
    us_master_token = "test-us-master-token"
    provider = dl_auth.USMasterTokenAuthProvider(token=us_master_token)
    assert provider.get_headers() == {"X-US-Master-Token": us_master_token}
