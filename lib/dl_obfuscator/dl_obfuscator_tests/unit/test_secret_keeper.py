from dl_obfuscator import SecretKeeper


class TestSecretKeeper:
    def test_init(self) -> None:
        sc = SecretKeeper()
        assert len(sc.get_secrets()) == 0
        assert len(sc.get_params()) == 0

    def test_add_secret(self) -> None:
        sc = SecretKeeper()

        sc.add_secret("token", "abc123")
        sc.add_secret("api_key", "key456")

        secrets = sc.get_secrets()
        assert "abc123" in secrets
        assert "key456" in secrets
        assert len(secrets) == 2

    def test_add_param(self) -> None:
        sc = SecretKeeper()

        sc.add_param("filter", "user_id=123")
        sc.add_param("param", "value456")

        params = sc.get_params()
        assert "user_id=123" in params
        assert "value456" in params
        assert len(params) == 2

    def test_clear(self) -> None:
        sc = SecretKeeper()

        sc.add_secret("token", "abc123")
        sc.add_param("param", "value456")

        assert len(sc.get_secrets()) == 1
        assert len(sc.get_params()) == 1

        sc.clear()

        assert len(sc.get_secrets()) == 0
        assert len(sc.get_params()) == 0

    def test_duplicate_values(self) -> None:
        sc = SecretKeeper()

        sc.add_secret("token1", "same_value")
        sc.add_secret("token2", "same_value")  # This should overwrite

        secret_values = sc.get_secrets()
        assert "same_value" in secret_values
        assert len(secret_values) == 1

    def test_repr_false(self) -> None:
        sc = SecretKeeper()

        sc.add_secret("token", "abc123")
        sc.add_param("param", "value456")

        printed = print(sc)
        assert not printed
