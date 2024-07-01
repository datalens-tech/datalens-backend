import os
from typing import (
    Annotated,
    Any,
    Optional,
    Type,
)

import pydantic
import pydantic_settings

from dl_auth_api_lib.utils.pydantic import make_dict_factory


class BaseOAuthClient(pydantic.BaseModel):
    conn_type: str
    auth_type: str

    @classmethod
    def factory(cls, data: Any) -> "BaseOAuthClient":
        if isinstance(data, BaseOAuthClient):
            return data

        assert isinstance(data, dict), "OAuthClient settings must be a dict"
        assert "auth_type" in data, "No auth_type in client"
        assert data["auth_type"] in _REGISTRY, f"No such OAuth type: {data['auth_type']}"

        config_class = _REGISTRY[data["auth_type"]]
        return config_class.model_validate(data)


class YandexOAuthClient(BaseOAuthClient):
    auth_type: str = "yandex"

    client_id: str
    client_secret: str
    redirect_uri: str
    scope: Optional[str] = pydantic.Field(default=None)


class AuthAPISettings(pydantic_settings.BaseSettings):
    model_config = pydantic_settings.SettingsConfigDict(env_nested_delimiter="__")

    auth_clients: Annotated[
        dict[str, pydantic.SerializeAsAny[BaseOAuthClient]],
        pydantic.BeforeValidator(make_dict_factory(BaseOAuthClient.factory)),
    ] = pydantic.Field(default=dict())

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[pydantic_settings.BaseSettings],
        init_settings: pydantic_settings.PydanticBaseSettingsSource,
        env_settings: pydantic_settings.PydanticBaseSettingsSource,
        dotenv_settings: pydantic_settings.PydanticBaseSettingsSource,
        file_secret_settings: pydantic_settings.PydanticBaseSettingsSource,
    ) -> tuple[pydantic_settings.PydanticBaseSettingsSource, ...]:
        return (
            env_settings,
            pydantic_settings.YamlConfigSettingsSource(
                settings_cls,
                yaml_file=os.environ.get("CONFIG_PATH", None),
            ),
            init_settings,
        )


_REGISTRY = {
    "yandex": YandexOAuthClient,
}
