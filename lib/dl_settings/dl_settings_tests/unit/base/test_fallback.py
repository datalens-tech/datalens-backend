import warnings

import attr
import pytest

import dl_settings


def test_no_fallback_raises() -> None:
    class Settings(dl_settings.BaseRootSettings):
        ...

    settings = Settings()

    with pytest.raises(AttributeError):
        assert settings.test  # type: ignore


def test_default() -> None:
    class Settings(dl_settings.WithFallbackGetAttr, dl_settings.BaseRootSettings):
        test: str

    settings = Settings(test="value")

    assert settings.test == "value"


def test_no_fallback_map() -> None:
    class Settings(dl_settings.WithFallbackGetAttr, dl_settings.BaseRootSettings):
        ...

    settings = Settings()

    with pytest.raises(AttributeError):
        assert settings.test


def test_fallback_getattr() -> None:
    class Settings(dl_settings.WithFallbackGetAttr, dl_settings.BaseRootSettings):
        ...

    @attr.s(auto_attribs=True)
    class Fallback:
        test: str

    fallback = Fallback(test="test")
    settings = Settings(fallback=fallback)

    assert settings.test == "test"


def test_fallback_getattr_upper() -> None:
    class Settings(dl_settings.WithFallbackGetAttr, dl_settings.BaseRootSettings):
        ...

    @attr.s(auto_attribs=True)
    class Fallback:
        TEST: str

    fallback = Fallback(TEST="test")
    settings = Settings(fallback=fallback)

    assert settings.test == "test"


def test_fallback_getattr_lower() -> None:
    class Settings(dl_settings.WithFallbackGetAttr, dl_settings.BaseRootSettings):
        ...

    @attr.s(auto_attribs=True)
    class Fallback:
        test: str

    fallback = Fallback(test="test")
    settings = Settings(fallback=fallback)

    assert settings.TEST == "test"


@pytest.mark.parametrize(
    "env_key, fallback_key",
    [
        ("TEST_KEY", "ANOTHER_KEY"),
        ("TEST_KEY", "another_key"),
        ("test_key", "ANOTHER_KEY"),
        ("test_key", "another_key"),
    ],
)
def test_fallback_env_source(
    env_key: str,
    fallback_key: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Settings(dl_settings.WithFallbackEnvSource, dl_settings.BaseRootSettings):
        TEST_KEY: str = NotImplemented

        fallback_env_keys = {
            env_key: fallback_key,
        }

    monkeypatch.setenv(fallback_key, "value")

    with warnings.catch_warnings(record=True) as w:
        settings = Settings()
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "Deprecated env var found" in str(w[0].message)

    assert settings.TEST_KEY == "value"


def test_fallback_env_source_nested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Nested(dl_settings.BaseSettings):
        test: str = NotImplemented

    class Settings(dl_settings.WithFallbackEnvSource, dl_settings.BaseRootSettings):
        nested: Nested = NotImplemented

        fallback_env_keys = {
            "NESTED__TEST": "NESTED_TEST",
        }

    monkeypatch.setenv("NESTED_TEST", "value")

    with warnings.catch_warnings(record=True):
        settings = Settings()

    assert settings.nested.test == "value"
