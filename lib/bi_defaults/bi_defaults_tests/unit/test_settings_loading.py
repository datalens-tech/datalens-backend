import pytest

from bi_configs.environments import LegacyDefaults
from bi_defaults.environments import EnvAliasesMap

from bi_configs.settings_loaders.fallback_cfg_resolver import YEnvFallbackConfigResolver
from bi_defaults.environments import InstallationsMap


@pytest.mark.parametrize("yenv_type_val, expected_fallback", (
    ("int-production", InstallationsMap.int_prod),
    ("int-testing", InstallationsMap.int_testing),
    ("production", InstallationsMap.ext_prod),
    ("production-public", InstallationsMap.ext_prod),
    ("production-sec-embeds", InstallationsMap.ext_prod),
    ("testing", InstallationsMap.ext_testing),
    ("testing-public", InstallationsMap.ext_testing),
    ("testing-sec-embeds", InstallationsMap.ext_testing),
))
def test_yenv_cfg_revolver(yenv_type_val, expected_fallback):
    resolver = YEnvFallbackConfigResolver(
        env_map=EnvAliasesMap,
        installation_map=InstallationsMap,
    )
    fallback = resolver.resolve(dict(YENV_TYPE=yenv_type_val))
    assert isinstance(fallback, LegacyDefaults)
    assert fallback is expected_fallback
