import pydantic

import dl_retrier
import dl_settings


class USClientSettings(dl_settings.BaseSettings):
    RETRY_POLICY: dl_retrier.RetryPolicyFactorySettings = pydantic.Field(
        default_factory=dl_retrier.RetryPolicyFactorySettings
    )
