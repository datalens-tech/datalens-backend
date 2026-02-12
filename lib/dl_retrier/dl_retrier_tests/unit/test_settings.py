import pydantic
import pytest

import dl_retrier
import dl_settings


def test_defaults() -> None:
    dl_retrier.RetryPolicySettings()
    dl_retrier.RetryPolicyFactorySettings()


def test_factory_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RootSettings(dl_settings.BaseRootSettings):
        RETRY_POLICY_FACTORY: dl_retrier.RetryPolicyFactorySettings = pydantic.Field(
            default_factory=dl_retrier.RetryPolicyFactorySettings
        )

    monkeypatch.setenv("RETRY_POLICY_FACTORY__RETRY_POLICIES__TEST__TOTAL_TIMEOUT", "10")

    settings = RootSettings()
    assert settings.RETRY_POLICY_FACTORY.RETRY_POLICIES == {"TEST": dl_retrier.RetryPolicySettings(TOTAL_TIMEOUT=10)}
