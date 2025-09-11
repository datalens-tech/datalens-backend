from pydantic import Field

import dl_retrier
import dl_settings


class USClientSettings(dl_settings.BaseSettings):
    RETRY_POLICY: dl_retrier.RetryPolicyFactorySettings = Field(default_factory=dl_retrier.RetryPolicyFactorySettings)
