import pydantic
from typing_extensions import Self

import dl_settings


class CryptoKeysSettings(dl_settings.BaseSettings):
    MAP_ID_KEY: dict[str, pydantic.SecretStr]
    ACTUAL_KEY_ID: str

    @pydantic.model_validator(mode="after")
    def _validate_actual_key_id_in_map(self) -> Self:
        if self.ACTUAL_KEY_ID not in self.MAP_ID_KEY:
            raise ValueError(f"ACTUAL_KEY_ID {self.ACTUAL_KEY_ID!r} is not in MAP_ID_KEY")
        return self

    @property
    def map_id_key(self) -> dict[str, str]:
        return {k: v.get_secret_value() for k, v in self.MAP_ID_KEY.items()}

    @property
    def actual_key_id(self) -> str:
        return self.ACTUAL_KEY_ID
