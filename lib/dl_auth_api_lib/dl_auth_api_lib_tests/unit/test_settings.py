def test_settings(oauth_app_settings):
    settings = oauth_app_settings.model_dump()
    assert settings["auth_clients"]["app_metrica"]["client_secret"] == "123pass"
    assert settings["auth_clients"]["ya_client"]["client_secret"] == "pass1234"
