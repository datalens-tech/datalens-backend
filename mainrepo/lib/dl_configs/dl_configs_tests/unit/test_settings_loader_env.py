# TODO FIX: https://st.yandex-team.ru/BI-2497 Try to use test-classed for subject classed scoping
# Commented due to attr.resolve_types() does not work with classes declared in non-global context
# from __future__ import annotations
import enum
import json
from os import path
import tempfile
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Type,
)

import attr
import pytest
import shortuuid

from dl_configs.settings_loaders.common import SDict
from dl_configs.settings_loaders.fallback_cfg_resolver import ConstantFallbackConfigResolver
from dl_configs.settings_loaders.loader_env import (
    ConfigFieldMissing,
    EnvSettingsLoader,
)
from dl_configs.settings_loaders.meta_definition import (
    required,
    s_attrib,
)
from dl_configs.settings_loaders.settings_obj_base import SettingsBase


def load_settings(
    env: SDict,
    settings_type: Type[SettingsBase],
    key_prefix: Optional[str] = None,
    fallback_env: Any = None,
) -> Any:
    fallback_cfg_resolver = ConstantFallbackConfigResolver(fallback_env)
    loader = EnvSettingsLoader(env)
    return loader.load_settings(
        settings_type,
        key_prefix=key_prefix,
        fallback_cfg_resolver=fallback_cfg_resolver,
    )


def perform_loader_env_check(
    env: SDict,
    expected_settings: Any,
    key_prefix: Optional[str] = None,
    fallback_env: Any = None,
) -> None:
    actual_settings = load_settings(
        env=env,
        settings_type=type(expected_settings),
        key_prefix=key_prefix,
        fallback_env=fallback_env,
    )

    assert actual_settings == expected_settings


def test_simple_scalars_loading():
    @attr.s
    class SomeSettings(SettingsBase):
        a: int = s_attrib("a")
        b: str = s_attrib("b")
        c: float = s_attrib("c")
        bool_0: bool = s_attrib("bool_0")
        bool_py_true: bool = s_attrib("bool_py_true")
        bool_py_false: bool = s_attrib("bool_py_false")
        bool_true: bool = s_attrib("bool_true")
        bool_false: bool = s_attrib("bool_false")
        bool_1: bool = s_attrib("bool_1")
        z: Dict[str, str] = s_attrib("z", env_var_converter=json.loads)

    perform_loader_env_check(
        key_prefix="DL",
        env=dict(
            DL_A="123",
            DL_B="abc",
            DL_C="1.2",
            DL_BOOL_0="0",
            DL_BOOL_1="1",
            DL_BOOL_PY_TRUE="True",
            DL_BOOL_PY_FALSE="False",
            DL_BOOL_TRUE="true",
            DL_BOOL_FALSE="false",
            DL_Z="""{"a": "b"}""",
        ),
        expected_settings=SomeSettings(
            a=123,
            b="abc",
            c=1.2,
            bool_0=False,
            bool_1=True,
            bool_py_true=True,
            bool_py_false=False,
            bool_true=True,
            bool_false=False,
            z=dict(a="b"),
        ),
    )


def test_map_loading_simple():
    @attr.s
    class SettingsMapStrStr(SettingsBase):
        m: Dict[str, str] = s_attrib("M_MAP")

    perform_loader_env_check(
        dict(M_MAP_k1="val11", M_MAP_k2="val22"), expected_settings=SettingsMapStrStr(m=dict(k1="val11", k2="val22"))
    )


def test_map_loading_nested():
    @attr.s
    class Nested(SettingsBase):
        m: Dict[str, str] = s_attrib("M_MAP")

    @attr.s
    class SettingsMapStrStr(SettingsBase):
        nested: Nested = s_attrib("NESTED")

    perform_loader_env_check(
        env=dict(
            NESTED_M_MAP_k1="val11",
            NESTED_M_MAP_k2="val22",
        ),
        expected_settings=SettingsMapStrStr(nested=Nested(m=dict(k1="val11", k2="val22"))),
    )


def test_map_loading_int():
    @attr.s
    class SettingsMapStrStr(SettingsBase):
        m: Dict[str, int] = s_attrib("M_MAP")

    perform_loader_env_check(
        env=dict(
            M_MAP_k1="11",
            M_MAP_k2="22",
        ),
        expected_settings=SettingsMapStrStr(m=dict(k1=11, k2=22)),
    )


def test_map_loading_empty():
    @attr.s
    class SettingsMapStrStr(SettingsBase):
        m: Dict[str, int] = s_attrib("M_MAP", missing_factory=dict)

    perform_loader_env_check(env=dict(), expected_settings=SettingsMapStrStr(m={}))


def test_composite_loading():
    @attr.s
    class SuperNestedSettings(SettingsBase):
        param_1: bool = s_attrib("param_1")

    @attr.s
    class NestedSettings(SettingsBase):
        n1: int = s_attrib("n1")
        n2: int = s_attrib("n2")
        nested: SuperNestedSettings = s_attrib("nested")

    @attr.s
    class SomeSettings(SettingsBase):
        a: int = s_attrib("a")
        nested: NestedSettings = s_attrib("nested")
        nullable_nested: Optional[NestedSettings] = s_attrib("nullable_nested", missing=None)

    perform_loader_env_check(
        key_prefix=None,
        env=dict(A="1", NESTED_N1="11", NESTED_N2="12", NESTED_NESTED_PARAM_1="1"),
        expected_settings=SomeSettings(
            a=1,
            nested=NestedSettings(n1=11, n2=12, nested=SuperNestedSettings(param_1=True)),
            nullable_nested=None,
        ),
    )


def test_scalar_defaults():
    class Fallback:
        THE_A = 75
        THE_B = 88

    @attr.s
    class SomeSettings(SettingsBase):
        a: int = s_attrib("a", fallback_cfg_key="THE_A")
        b: int = s_attrib("b", fallback_cfg_key="THE_B")
        c: Optional[int] = s_attrib("c", fallback_cfg_key="THE_NON_EXISTING_KEY", missing=None)

    perform_loader_env_check(
        key_prefix=None,
        env=dict(
            B="99",
        ),
        expected_settings=SomeSettings(
            a=75,
            b=99,
            c=None,
        ),
        fallback_env=Fallback,
    )


def test_composite_default():
    class Fallback:
        HOST = "example.com"
        PORT = 443

    @attr.s
    class BackendGroup(SettingsBase):
        host: str = s_attrib("host")
        port: int = s_attrib("port")
        proto: str = s_attrib("proto")

    @attr.s
    class SomeSettings(SettingsBase):
        bg: BackendGroup = s_attrib(
            "bg", fallback_factory=lambda env: BackendGroup(host=env.HOST, port=env.PORT, proto="http")
        )

    perform_loader_env_check(
        key_prefix=None,
        env=dict(BG_PROTO="https", BG_PORT=8443),
        expected_settings=SomeSettings(
            bg=BackendGroup(host=Fallback.HOST, port=8443, proto="https")  # From env  # From env
        ),
        fallback_env=Fallback,
    )


def test_scalar_fallback_order():
    class FallbackEnv:
        THE_B = "THE B FROM FALLBACK"

    @attr.s
    class SomeSettings:
        a: str = s_attrib("dl_a", fallback_cfg_key="THE_A", missing="attrs default a")
        b: str = s_attrib("dl_b", fallback_cfg_key="THE_B", missing="attrs default b")
        c: str = s_attrib("dl_c", fallback_cfg_key="THE_C", missing="attrs default c")

    perform_loader_env_check(
        env=dict(DL_A="a from proc env"),
        expected_settings=SomeSettings(
            # A was declared in user env so we expect value from it
            a="a from proc env",
            # There is no B in user env but there is value in fallback env. So we expect value from fallback
            b=FallbackEnv.THE_B,
            # There is no C neither in user env nor in fallback one. So we expect default from attrs
            c="attrs default c",
        ),
        fallback_env=FallbackEnv,
    )


def test_ignore():
    class Fallback:
        EP_HOST: str = "host-from-fb"
        EP_PORT: int = 0

    @attr.s(frozen=True)
    class Nested(SettingsBase):
        host: str = s_attrib("HOST")
        port: int = s_attrib("PORT")

    @attr.s(frozen=True)
    class DefaultedSettings(SettingsBase):
        endpoint: Optional[Nested] = s_attrib(
            "EP",
            enabled_key_name="ENABLED",
            fallback_factory=lambda fb: Nested(host=fb.EP_HOST, port=fb.EP_PORT),
            missing=Nested(host="localhost", port=321),
        )

    @attr.s(frozen=True)
    class NonDefaultedSettings(SettingsBase):
        endpoint: Optional[Nested] = s_attrib("EP", enabled_key_name="ENABLED")

    # Check that endpoint is not despite of fallback
    perform_loader_env_check(
        env=dict(EP_ENABLED="0"),
        expected_settings=DefaultedSettings(endpoint=None),
        fallback_env=Fallback,
    )

    # Check that we got a fallback if no keys in env and ignore key is not true
    perform_loader_env_check(
        env=dict(
            EP_ENABLED="1",
            EP_PORT=9999,
        ),
        expected_settings=DefaultedSettings(endpoint=Nested(host=Fallback.EP_HOST, port=9999)),
        fallback_env=Fallback,
    )

    # Check that we will not fail with ignored non-defaulted key
    perform_loader_env_check(
        env=dict(
            EP_ENABLED="0",
        ),
        expected_settings=NonDefaultedSettings(endpoint=None),
    )


def test_sensitive_simple():
    # TODO FIX: https://st.yandex-team.ru/BI-2497 add loading report check when be ready
    @attr.s
    class Nested(SettingsBase):
        a: int = s_attrib("a", sensitive=True)

    @attr.s
    class SomeSettings(SettingsBase):
        nested: Nested = s_attrib("n")
        nested_sensitive: Nested = s_attrib("n_sens", sensitive=True)
        sensitive: str = s_attrib("sens", sensitive=True)

    assert attr.fields_dict(Nested)["a"].repr is False

    assert attr.fields_dict(SomeSettings)["nested"].repr is True
    assert attr.fields_dict(SomeSettings)["nested_sensitive"].repr is False
    assert attr.fields_dict(SomeSettings)["sensitive"].repr is False


def test_required_in_fallback_factory():
    class Fallback:
        DB_HOST = "127.0.0.1"

    @attr.s
    class DBConfig(SettingsBase):
        host: str = s_attrib("HOST")
        user: str = s_attrib("USER")
        password: str = s_attrib("PASSWORD", sensitive=True)

    @attr.s
    class SomeSettings(SettingsBase):
        db: DBConfig = s_attrib(
            "DB",
            fallback_factory=lambda fb: DBConfig(
                host=fb.DB_HOST,
                user=required(str),
                password=required(str),
            ),
        )

    perform_loader_env_check(
        env=dict(
            DB_USER="user_from_env",
            DB_PASSWORD="password_from_env",
        ),
        expected_settings=SomeSettings(
            db=DBConfig(host=Fallback.DB_HOST, user="user_from_env", password="password_from_env")
        ),
        fallback_env=Fallback,
    )

    with pytest.raises(ConfigFieldMissing) as exc:
        load_settings(env=dict(), settings_type=SomeSettings, fallback_env=Fallback)

    assert exc.value.field_set == {"db.password", "db.user"}

    with pytest.raises(ConfigFieldMissing) as exc:
        load_settings(env=dict(DB_USER="user_from_env"), settings_type=SomeSettings, fallback_env=Fallback)

    assert exc.value.field_set == {"db.password"}


def test_fallback_factory_static_method():
    class Fallback1:
        fb_1_only = "fb_1_only"

    class Fallback2:
        fb_2_only = "fb_2_only"

    @attr.s
    class Nested(SettingsBase):
        @staticmethod
        def a_default(env):
            if isinstance(env, Fallback1):
                return env.fb_1_only
            elif isinstance(env, Fallback2):
                return env.fb_2_only
            else:
                raise ValueError("Unexpected fallback type")

        a: str = s_attrib("a", fallback_factory=a_default)

    perform_loader_env_check(env={}, expected_settings=Nested(a=Fallback1.fb_1_only), fallback_env=Fallback1())

    perform_loader_env_check(env={}, expected_settings=Nested(a=Fallback2.fb_2_only), fallback_env=Fallback2())


def test_app_type():
    class Fallback:
        SOME_KEY = "whatever"

    class Mode(enum.Enum):
        mode_a = enum.auto()
        mode_b = enum.auto()

    @attr.s
    class TypedSettings(SettingsBase):
        mode: Mode = s_attrib("APP_MODE", is_app_cfg_type=True, env_var_converter=lambda s: Mode[s])

        @staticmethod
        def default_depends_on_mode(fallback_config: Fallback, mode: Mode):
            return f"{mode.name} + {fallback_config.SOME_KEY}"

        depends_on_mode: str = s_attrib("DEPENDS_ON_MODE", fallback_factory=default_depends_on_mode)

        @staticmethod
        def default_not_depends_on_mode(fallback_config: Fallback):
            return fallback_config.SOME_KEY

        not_depends_on_mode: str = s_attrib("NOT_DEPENDS_ON_MODE", fallback_factory=default_not_depends_on_mode)

    perform_loader_env_check(
        dict(APP_MODE="mode_a"),
        TypedSettings(
            mode=Mode.mode_a,
            depends_on_mode=f"{Mode.mode_a.name} + {Fallback.SOME_KEY}",
            not_depends_on_mode=Fallback.SOME_KEY,
        ),
        fallback_env=Fallback(),
    )


def test_app_type_field_override():
    class Fallback:
        SOME_KEY = "whatever"

    class Mode(enum.Enum):
        mode_a = enum.auto()
        mode_b = enum.auto()

    @attr.s
    class TypedSettings(SettingsBase):
        mode: Mode = s_attrib("APP_MODE", is_app_cfg_type=True, env_var_converter=lambda s: Mode[s])
        override_me: str = s_attrib("OVERRIDE_ME")

        @staticmethod
        def default_depends_on_mode(fallback_config: Fallback, mode: Mode):
            return f"{mode.name} + {fallback_config.SOME_KEY}"

        depends_on_mode: str = s_attrib("DEPENDS_ON_MODE", fallback_factory=default_depends_on_mode)

    env = dict(OVERRIDE_ME="yes")

    extractor = EnvSettingsLoader(env).build_top_level_extractor(
        TypedSettings,
        app_cfg_type=Mode.mode_a,
        fallback_cfg=Fallback,
    )
    actual_settings = extractor.extract(env)

    assert actual_settings == TypedSettings(
        mode=Mode.mode_a, depends_on_mode=f"{Mode.mode_a.name} + {Fallback.SOME_KEY}", override_me="yes"
    )

    env = dict(OVERRIDE_ME="yes")
    # TODO FIX: Make dedicated test when tests will migrate on classes
    extractor = EnvSettingsLoader(env).build_top_level_extractor(
        TypedSettings,
        app_cfg_type=Mode.mode_b,
        fallback_cfg=Fallback,
        field_overrides=dict(override_me="totally_overridden"),
    )
    actual_settings = extractor.extract(env)

    assert actual_settings == TypedSettings(
        mode=Mode.mode_b, depends_on_mode=f"{Mode.mode_b.name} + {Fallback.SOME_KEY}", override_me="totally_overridden"
    )


def test_fallback_only():
    class Fallback:
        SOME_KEY = "whatever"

    @attr.s
    class Nested(SettingsBase):
        a: int = s_attrib("A", sensitive=True)

    @attr.s
    class TypedSettings(SettingsBase):
        no_env_1: Optional[str] = s_attrib(None, fallback_cfg_key="SOME_KEY", missing=None)
        via_env: Optional[str] = s_attrib("VIA_ENV", fallback_cfg_key="SOME_KEY")
        no_env_nested: Nested = s_attrib(None, fallback_factory=lambda: Nested(a=1))

    perform_loader_env_check(
        dict(VIA_ENV="mode_a"),
        TypedSettings(
            no_env_1=Fallback.SOME_KEY,
            via_env="mode_a",
            no_env_nested=Nested(a=1),
        ),
        fallback_env=Fallback(),
    )


def test_fallback_value_propagation_in_nested_settings():
    @attr.s()
    class ConnectorA(SettingsBase):
        host: str = s_attrib("HOST")
        password: str = s_attrib("PASSWORD")

    @attr.s()
    class ConnBKey(SettingsBase):
        id: Optional[str] = s_attrib("ID")
        secret: str = s_attrib("SECRET")

    @attr.s()
    class ConnectorB(SettingsBase):
        url: str = s_attrib("URL")
        key: ConnBKey = s_attrib("KEY")

    @attr.s()
    class ConnectorsSettings(SettingsBase):
        conn_a: ConnectorA = s_attrib("A")
        conn_b: ConnectorB = s_attrib("B")

    @attr.s()
    class Settings(SettingsBase):
        conns: ConnectorsSettings = s_attrib(
            "CS",
            fallback_factory=lambda: ConnectorsSettings(
                conn_a=ConnectorA(
                    host="127.0.0.1",
                    password=required(str),
                ),
                conn_b=ConnectorB(
                    url="https://127.0.0.1:8080",
                    key=ConnBKey(id=None, secret=required(str)),
                ),
            ),
        )

    # Check required only
    perform_loader_env_check(
        dict(
            CS_A_PASSWORD="pass",
            CS_B_KEY_SECRET="key",
        ),
        Settings(
            conns=ConnectorsSettings(
                conn_a=ConnectorA(
                    host="127.0.0.1",
                    password="pass",
                ),
                conn_b=ConnectorB(
                    url="https://127.0.0.1:8080",
                    key=ConnBKey(
                        id=None,
                        secret="key",
                    ),
                ),
            )
        ),
    )

    # Check that deep overrides are applied
    perform_loader_env_check(
        dict(
            CS_A_PASSWORD="pass",
            CS_B_KEY_SECRET="key_secret",
            CS_B_KEY_ID="key_id_from_env",
        ),
        Settings(
            conns=ConnectorsSettings(
                conn_a=ConnectorA(
                    host="127.0.0.1",
                    password="pass",
                ),
                conn_b=ConnectorB(
                    url="https://127.0.0.1:8080",
                    key=ConnBKey(
                        id="key_id_from_env",
                        secret="key_secret",
                    ),
                ),
            )
        ),
    )

    # Check that exception fires on missing keys
    with pytest.raises(ConfigFieldMissing) as exc:
        load_settings(
            env=dict(),
            settings_type=Settings,
        )

    assert exc.value.field_set == {"conns.conn_b.key.secret", "conns.conn_a.password"}


def test_complex_nested_with_required_dft_ffactory_no_env():
    @attr.s()
    class Nested(SettingsBase):
        val: str = s_attrib("VAL")

    @attr.s()
    class RootWithFallbackFactoryNone(SettingsBase):
        nested_with_ff: Optional[Nested] = s_attrib("NWFF", fallback_factory=lambda: None)

    perform_loader_env_check(
        dict(),
        RootWithFallbackFactoryNone(nested_with_ff=None),
    )


def test_complex_nested_with_required_dft_missing_no_env():
    @attr.s()
    class Nested(SettingsBase):
        val: str = s_attrib("VAL")

    @attr.s()
    class RootWithMissingNone(SettingsBase):
        nested_with_ff: Optional[Nested] = s_attrib("NWFF", missing=None)

    perform_loader_env_check(
        dict(),
        RootWithMissingNone(nested_with_ff=None),
    )


def test_complex_nested_with_required_dft_not_defined_no_env():
    @attr.s()
    class Nested(SettingsBase):
        val: str = s_attrib("VAL")

    @attr.s()
    class RootWithMissingNone(SettingsBase):
        nested_with_ff: Optional[Nested] = s_attrib("NWFF")

    with pytest.raises(ConfigFieldMissing) as exc:
        load_settings(
            env=dict(),
            settings_type=RootWithMissingNone,
        )

    # There is keys for any of `nested_with_ff` attributes so it's treated as missing object, not particular fields
    assert exc.value.field_set == {"nested_with_ff"}


def test_complex_double_nested_with_required_dft_not_defined_partial_env():
    @attr.s()
    class Pair(SettingsBase):
        left: str = s_attrib("L")
        right: str = s_attrib("R")
        comment: str = s_attrib("C")

    @attr.s()
    class Nested(SettingsBase):
        val: Pair = s_attrib("PAIR")

    @attr.s()
    class RootWithMissingNone(SettingsBase):
        nested_with_ff: Optional[Nested] = s_attrib("NWFF")

    with pytest.raises(ConfigFieldMissing) as exc:
        load_settings(
            env=dict(NWFF_PAIR_C="some comment"),
            settings_type=RootWithMissingNone,
        )

    assert exc.value.field_set == {"nested_with_ff.val.right", "nested_with_ff.val.left"}


def test_complex_double_nested_dft_fallback_factory_nested_none():
    @attr.s()
    class Pair(SettingsBase):
        left: str = s_attrib("L")
        right: str = s_attrib("R")
        comment: str = s_attrib("C")

    @attr.s()
    class Nested(SettingsBase):
        val: Optional[Pair] = s_attrib("PAIR")

    @attr.s()
    class RootWithMissingNone(SettingsBase):
        nested_with_ff: Optional[Nested] = s_attrib("NWFF", fallback_factory=lambda: Nested(val=None))

    # Check regular loading
    perform_loader_env_check(
        dict(
            NWFF_PAIR_L="L",
            NWFF_PAIR_R="R",
            NWFF_PAIR_C="1",
        ),
        RootWithMissingNone(
            nested_with_ff=Nested(
                val=Pair(
                    left="L",
                    right="R",
                    comment="1",
                )
            )
        ),
    )

    # Check that exception is correct in case of missing vars
    with pytest.raises(ConfigFieldMissing) as exc:
        load_settings(
            env=dict(
                NWFF_PAIR_C="1",
            ),
            settings_type=RootWithMissingNone,
        )

    assert exc.value.field_set == {"nested_with_ff.val.right", "nested_with_ff.val.left"}


def test_json_value():
    @attr.s
    class Nested(SettingsBase):
        dft_key: str = s_attrib("DFT_KEY")
        map_key_id_value: Dict[str, str] = s_attrib("MAP")

    @attr.s
    class TypedSettings(SettingsBase):
        nested: Optional[Nested] = s_attrib(
            "N",
            json_converter=lambda js: Nested(dft_key=js["dft_key"], map_key_id_value=js["keys"]),
            enabled_key_name="ENABLED",
            missing=None,
        )

    expected_dft_key = "asf"
    expected_key_map = dict(a="a", b="b")
    expected_settings = TypedSettings(
        nested=Nested(dft_key=expected_dft_key, map_key_id_value=dict(expected_key_map)),
    )

    json_encoded_nested_value = json.dumps(dict(dft_key=expected_dft_key, keys=expected_key_map))

    # Case None
    perform_loader_env_check(
        dict(),
        TypedSettings(nested=None),
    )

    # Case of JSON value
    perform_loader_env_check(
        dict(
            N_JSON_VALUE=json_encoded_nested_value,
        ),
        expected_settings,
    )

    # Case disabled
    perform_loader_env_check(
        dict(
            N_JSON_VALUE=json_encoded_nested_value,
            N_ENABLED="false",
        ),
        TypedSettings(nested=None),
    )

    # Case disabled
    perform_loader_env_check(
        dict(
            N_JSON_VALUE=json_encoded_nested_value,
            N_ENABLED="true",
        ),
        expected_settings,
    )

    # Case of plain vars
    perform_loader_env_check(
        dict(
            N_DFT_KEY="asf",
            **{f"N_MAP_{key}": value for key, value in expected_key_map.items()},
        ),
        expected_settings,
    )


@pytest.fixture(scope="function")
def temp_file_factory() -> Callable[[str], str]:
    with tempfile.TemporaryDirectory() as tmp_dir_name:

        def create_file_with_content(content: str) -> str:
            file_name = path.join(tmp_dir_name, shortuuid.uuid())
            with open(file_name, "wb") as file:
                file.write(content.encode("ascii"))
            return file_name

        yield create_file_with_content


def test_file_mapping(temp_file_factory):
    @attr.s
    class TypedSettings(SettingsBase):
        a: str = s_attrib("A")
        b: int = s_attrib("B")

    a_value = "the_a_value"
    b_value = 434

    a_file_name = temp_file_factory(a_value)
    b_file_name = temp_file_factory(str(b_value))

    perform_loader_env_check(
        dict(
            BIE_FILE_MAP_A=a_file_name,
            BIE_FILE_MAP_B=b_file_name,
        ),
        TypedSettings(
            a=a_value,
            b=b_value,
        ),
    )
