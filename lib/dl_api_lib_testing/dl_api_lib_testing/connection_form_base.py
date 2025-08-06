import json
import os
import typing
from typing import (
    ClassVar,
    Optional,
    Type,
    final,
)

import attrs
import pytest

from dl_api_commons.base_models import (
    FormConfigParams,
    TenantCommon,
    TenantDef,
)
from dl_api_connector.form_config.models.base import (
    ConnectionForm,
    ConnectionFormFactory,
    ConnectionFormMode,
)
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_i18n.localizer_base import (
    LocalizerLoader,
    TranslationConfig,
)


class ConnectionFormTestBase:
    CONN_FORM_FACTORY_CLS: ClassVar[Type[ConnectionFormFactory]]
    TRANSLATION_CONFIGS: ClassVar[list[TranslationConfig]]
    OVERWRITE_EXPECTED_FORMS: ClassVar[bool] = False
    EXPECTED_FORMS_DIR: ClassVar[str] = "expected_forms"

    @pytest.fixture
    def connectors_settings(self) -> Optional[ConnectorSettingsBase]:
        """Parametrize if a form has extra settings"""

        return None

    @pytest.fixture
    def tenant(self) -> TenantDef:
        """Parametrize if a form depends on tenant type"""

        return TenantCommon()

    @final
    @pytest.fixture(
        params=[mode.name for mode in ConnectionFormMode],
    )
    def mode(self, request: pytest.FixtureRequest) -> ConnectionFormMode:
        return ConnectionFormMode(request.param)

    @pytest.fixture
    def form_config(
        self,
        connectors_settings: Optional[ConnectorSettingsBase],
        tenant: TenantDef,
        mode: ConnectionFormMode,
    ) -> ConnectionForm:
        loader = LocalizerLoader(
            configs=self.TRANSLATION_CONFIGS,
        )
        factory = loader.load()
        localizer = factory.get_for_locale("en")
        form_params = FormConfigParams()
        form_factory = self.CONN_FORM_FACTORY_CLS(mode=mode, localizer=localizer, form_params=form_params)
        form_config = form_factory.get_form_config(connectors_settings, tenant)
        return form_config

    @pytest.fixture(name="config_dir")
    def fixture_config_dir(self, request: pytest.FixtureRequest) -> str:
        dirname = os.path.dirname(request.module.__file__)
        dirname = os.path.join(dirname, self.EXPECTED_FORMS_DIR)
        os.makedirs(dirname, exist_ok=True)
        return dirname

    # This fixture need to be overloaded for proper name
    @pytest.fixture(name="expected_form_config_file")
    def fixture_expected_form_config_file(
        self,
        config_dir: str,
        connectors_settings: Optional[ConnectorSettingsBase],
        tenant: TenantDef,
        mode: ConnectionFormMode,
    ) -> str:
        parts: list[str] = []
        if connectors_settings is not None:
            for value in attrs.astuple(connectors_settings):
                parts.append(str(value))
        parts.append(str(tenant))
        parts.append(mode.value)

        filename = "_".join(parts) + ".json"
        return os.path.join(config_dir, filename)

    @pytest.fixture(name="expected_form_config")
    def fixture_expected_form_config(
        self,
        expected_form_config_file: str,
        form_config: ConnectionForm,
    ) -> dict[str, typing.Any]:
        if not os.path.exists(expected_form_config_file) or self.OVERWRITE_EXPECTED_FORMS:
            with open(expected_form_config_file, mode="w") as f:
                f.write(json.dumps(form_config.as_dict(), indent=4))

        with open(expected_form_config_file, mode="r") as f:
            return json.load(f)

    def test_validate_conditional_fields(self, form_config: ConnectionForm) -> None:
        form_config.validate_conditional_fields()

    def test_validate_api_schema_fields(self, form_config: ConnectionForm) -> None:
        form_config.validate_api_schema_fields()

    def test_serialize(self, form_config: ConnectionForm) -> None:
        serializable = form_config.as_dict()
        json.dumps(serializable)

    def test_config_json(
        self,
        expected_form_config: dict[str, typing.Any],
        form_config: ConnectionForm,
    ) -> None:
        if self.OVERWRITE_EXPECTED_FORMS:
            pytest.skip("Overwriting expected forms")
        assert form_config.as_dict() == expected_form_config


class DatasourceTemplateConnectionFormTestMixin:
    @pytest.fixture(name="enable_datasource_template", params=[True, False])
    def fixture_enable_datasource_template(self, request: pytest.FixtureRequest) -> bool:
        return request.param
