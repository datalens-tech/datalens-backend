from dl_obfuscator import SecretKeeper


class TestSecretKeeper:
    def test_init(self) -> None:
        sc = SecretKeeper()
        assert len(sc.secrets) == 0
        assert len(sc.params) == 0

    def test_add_secret(self) -> None:
        sc = SecretKeeper()

        sc.add_secret("abc123", "token")
        sc.add_secret("key456", "api_key")

        secrets = sc.secrets
        assert "abc123" in secrets
        assert "key456" in secrets
        assert len(secrets) == 2

    def test_add_param(self) -> None:
        sc = SecretKeeper()

        sc.add_param("user_id=123", "filter")
        sc.add_param("value456", "param")

        params = sc.params
        assert "user_id=123" in params
        assert "value456" in params
        assert len(params) == 2

    def test_clear(self) -> None:
        sc = SecretKeeper()

        sc.add_secret("abc123", "token")
        sc.add_param("value456", "param")

        assert len(sc.secrets) == 1
        assert len(sc.params) == 1

        sc.clear()

        assert len(sc.secrets) == 0
        assert len(sc.params) == 0

    def test_duplicate_values(self) -> None:
        sc = SecretKeeper()

        sc.add_secret("same_value", "token1")
        sc.add_secret(
            "same_value", "token2"
        )  # Adding same secret under a different name should not create a duplicate entry

        secret_values = sc.secrets
        assert "same_value" in secret_values
        assert len(secret_values) == 1

    def test_repr_false(self) -> None:
        sc = SecretKeeper()

        sc.add_secret("abc123", "token")
        sc.add_param("value456", "param")

        representation = repr(sc)
        assert "abc123" not in representation
        assert "value456" not in representation
