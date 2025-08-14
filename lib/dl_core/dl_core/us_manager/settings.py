from pydantic import Field

from dl_api_commons.retrier.settings import RetryPolicyFactorySettings
import dl_settings


class USClientSettings(dl_settings.BaseSettings):
    RETRY_POLICY: RetryPolicyFactorySettings = Field(default_factory=RetryPolicyFactorySettings)
