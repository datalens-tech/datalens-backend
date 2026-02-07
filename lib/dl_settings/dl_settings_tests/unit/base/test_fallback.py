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


def test_fallback_env_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Settings(dl_settings.WithFallbackEnvSource, dl_settings.BaseRootSettings):
        TEST_KEY: str = NotImplemented

        fallback_env_keys = {
            "TEST_KEY": "ANOTHER_KEY",
        }

    monkeypatch.setenv("ANOTHER_KEY", "value")

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
        TEST: str = NotImplemented

    class Settings(dl_settings.WithFallbackEnvSource, dl_settings.BaseRootSettings):
        NESTED: Nested = NotImplemented

        fallback_env_keys = {
            "NESTED__TEST": "NESTED_TEST",
        }

    monkeypatch.setenv("NESTED_TEST", "value")

    with warnings.catch_warnings(record=True):
        settings = Settings()

    assert settings.NESTED.TEST == "value"


def test_fallback_extra_env_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Settings(dl_settings.WithFallbackEnvSource, dl_settings.BaseRootSettings):
        TEST_KEY: str = NotImplemented

    extra_fallback_env_keys = {"TEST_KEY": "ANOTHER_KEY"}
    monkeypatch.setenv("ANOTHER_KEY", "value")

    with warnings.catch_warnings(record=True) as w:
        settings = Settings(extra_fallback_env_keys=extra_fallback_env_keys)
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "Deprecated env var found" in str(w[0].message)

    assert settings.TEST_KEY == "value"


def test_fallback_extra_env_source_merged_with_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Settings(dl_settings.WithFallbackEnvSource, dl_settings.BaseRootSettings):
        TEST_KEY: str = NotImplemented
        ANOTHER_KEY: str = NotImplemented

        fallback_env_keys = {
            "TEST_KEY": "OLD_TEST_KEY",
        }

    extra_fallback_env_keys = {"ANOTHER_KEY": "OLD_ANOTHER_KEY"}
    monkeypatch.setenv("OLD_TEST_KEY", "value1")
    monkeypatch.setenv("OLD_ANOTHER_KEY", "value2")

    with warnings.catch_warnings(record=True) as w:
        settings = Settings(extra_fallback_env_keys=extra_fallback_env_keys)
        assert len(w) == 2

    assert settings.TEST_KEY == "value1"
    assert settings.ANOTHER_KEY == "value2"
