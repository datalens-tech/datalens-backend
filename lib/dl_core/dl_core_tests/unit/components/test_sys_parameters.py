from dl_api_commons.base_models import RequestContextInfo
from dl_constants.enums import UserDataType
from dl_core import exc
from dl_core.components.sys_parameters import (
    SYS_PARAMETER_PREFIX,
    get_sys_parameter_expected_type,
    is_registered_sys_name,
    is_sys_name,
    resolve_sys_value,
)


def test_sys_prefix_is_reserved():
    assert SYS_PARAMETER_PREFIX == "_sys."


def test_is_sys_name_detects_prefix():
    assert is_sys_name("_sys.user_id") is True
    assert is_sys_name("_sys.anything") is True
    assert is_sys_name("region") is False


def test_is_registered_sys_name_only_known():
    assert is_registered_sys_name("_sys.user_id") is True
    assert is_registered_sys_name("_sys.unknown") is False
    assert is_registered_sys_name("region") is False


def test_resolve_user_id_from_rci():
    rci = RequestContextInfo(user_id="u-42")
    assert resolve_sys_value("_sys.user_id", rci) == "u-42"


def test_resolve_user_id_none_when_absent():
    rci = RequestContextInfo.create_empty()
    assert resolve_sys_value("_sys.user_id", rci) is None


def test_sys_user_id_expected_type_is_string():
    assert get_sys_parameter_expected_type("_sys.user_id") == UserDataType.string


def test_system_parameter_not_settable_inherits_parameter_value_invalid():
    # Inheriting ParameterValueInvalidError gives the HTTP 400 mapping for free.
    assert issubclass(exc.ParameterValueSystemParameterNotSettableError, exc.ParameterValueInvalidError)
    assert exc.ParameterValueSystemParameterNotSettableError.err_code == (
        *exc.ParameterValueInvalidError.err_code,
        "SYSTEM_PARAMETER_NOT_SETTABLE",
    )


def test_unknown_system_parameter_error_code():
    assert issubclass(exc.UnknownSystemParameterError, exc.DataSourceConfigurationError)
    assert exc.UnknownSystemParameterError.err_code == (
        *exc.DataSourceConfigurationError.err_code,
        "UNKNOWN_SYSTEM_PARAMETER",
    )


def test_system_parameter_type_mismatch_error_code():
    assert issubclass(exc.SystemParameterTypeMismatchError, exc.DataSourceConfigurationError)
    assert exc.SystemParameterTypeMismatchError.err_code == (
        *exc.DataSourceConfigurationError.err_code,
        "SYSTEM_PARAMETER_TYPE_MISMATCH",
    )
