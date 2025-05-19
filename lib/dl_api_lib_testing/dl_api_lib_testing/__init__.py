import pytest


# This is needed to make assert rewriting work in child tests
pytest.register_assert_rewrite("dl_api_lib_testing.datasource_template_base")

from .datasource_template_base import (
    BaseSubselectTestSourceTemplate,
    BaseTableTestSourceTemplate,
    BaseTestControlApiSourceTemplate,
    BaseTestControlApiSourceTemplateConnectionDisabled,
    BaseTestControlApiSourceTemplateSettingsDisabled,
    BaseTestDataApiSourceTemplate,
    BaseTestSourceTemplate,
    DatasetFactoryProtocol,
    ParameterFieldsFactoryProtocol,
)


__all__ = [
    "BaseSubselectTestSourceTemplate",
    "BaseTableTestSourceTemplate",
    "BaseTestControlApiSourceTemplate",
    "BaseTestControlApiSourceTemplateConnectionDisabled",
    "BaseTestControlApiSourceTemplateSettingsDisabled",
    "BaseTestDataApiSourceTemplate",
    "BaseTestSourceTemplate",
    "DatasetFactoryProtocol",
    "ParameterFieldsFactoryProtocol",
]
