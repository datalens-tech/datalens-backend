from __future__ import annotations

from dl_api_lib.error_handling import (
    BIError,
    PublicAPIErrorSchema,
    RegularAPIErrorSchema,
)
from dl_constants.exc import (
    DEFAULT_ERR_CODE_API_PREFIX,
    GLOBAL_ERR_PREFIX,
)
from dl_core import exc as common_exc


def test_status_code_mapping():
    class MyCustomValueError(ValueError):
        pass

    exc_to_status = {
        Exception: 500,
        ValueError: 400,
    }

    assert 400 == BIError.get_default_error_code(MyCustomValueError(), exc_code_mapping=exc_to_status)
    assert 400 == BIError.get_default_error_code(ValueError(), exc_code_mapping=exc_to_status)
    assert 500 == BIError.get_default_error_code(KeyError(), exc_code_mapping=exc_to_status)


def test_bi_error_default_message():
    exc_to_status = {}
    e = Exception("Private exception info")

    bi_error = BIError.from_exception(e, default_message="Some default message", exc_code_mapping=exc_to_status)

    assert (
        BIError(
            message="Some default message",
            http_code=None,
            application_code_stack=(),
            debug={},
            details={},
        )
        == bi_error
    )


def test_regular_bi_error_building():
    class ExcA(common_exc.DLBaseException):
        err_code = common_exc.DLBaseException.err_code + ["EXC_A"]
        _message = "ExcA message"

    class NonDLExc(Exception):
        pass

    class NonDLExc2(Exception):
        message = "Some message"
        details = {"some": "detail"}
        debug_info = {"some": "debug_info"}

    exc_to_status = {
        ExcA: 400,
    }

    exc_a = ExcA()
    assert BIError(
        http_code=400,
        application_code_stack=tuple(ExcA.err_code),
        message=exc_a.message,
        details=exc_a.details,
        debug=exc_a.debug_info,
    ) == BIError.from_exception(exc_a, exc_code_mapping=exc_to_status)

    for exc_unk in (NonDLExc(), NonDLExc2):
        assert BIError(
            http_code=None,
            application_code_stack=(),
            message=BIError.DEFAULT_ERROR_MESSAGE,
            details={},
            debug={},
        ) == BIError.from_exception(exc_unk, exc_code_mapping=exc_to_status)


def test_regular_schema():
    bi_error = BIError(
        message="Some message",
        http_code=None,
        application_code_stack=("A", "B"),
        debug=dict(a="b"),
        details=dict(a="b"),
    )

    # Ensure all fields passed to output
    assert (
        {"code": "ERR.SOME_API.A.B", "debug": dict(a="b"), "details": dict(a="b"), "message": "Some message"}
    ) == RegularAPIErrorSchema(context=dict(api_prefix="SOME_API")).dump(bi_error)

    # Ensure default prefix
    assert (
        {
            "code": f"{GLOBAL_ERR_PREFIX}.{DEFAULT_ERR_CODE_API_PREFIX}.A.B",
            "debug": dict(a="b"),
            "details": dict(a="b"),
            "message": "Some message",
        }
    ) == RegularAPIErrorSchema().dump(bi_error)


def test_public_schema():
    bi_error = BIError(
        message="Very user-private message",
        http_code=None,
        application_code_stack=("A", "B"),
        debug=dict(stackrace=["1", "2"]),
        details=dict(private_code="Some private value"),
    )

    assert (
        {
            "code": PublicAPIErrorSchema.Meta.PUBLIC_DEFAULT_ERR_CODE,
            "debug": {},
            "details": {},
            "message": PublicAPIErrorSchema.Meta.PUBLIC_DEFAULT_MESSAGE,
        }
    ) == PublicAPIErrorSchema().dump(bi_error)

    assert (
        len(PublicAPIErrorSchema.Meta.PUBLIC_FORWARDED_ERROR_CODES) > 0
    ), "Can not perform full public schema test without public whitelisted error codes"

    for err_code_stack in PublicAPIErrorSchema.Meta.PUBLIC_FORWARDED_ERROR_CODES:
        public_whitelisted_bi_error = BIError(
            message="Non user-private message",
            http_code=None,
            application_code_stack=err_code_stack,
            debug=dict(stackrace=["1", "2"]),
            details=dict(private_code="Some private value"),
        )
        # Ensure that message and error code are passed to output for whitelisted errors
        assert (
            {
                "code": ".".join([GLOBAL_ERR_PREFIX, DEFAULT_ERR_CODE_API_PREFIX] + list(err_code_stack)),
                "debug": {},
                "details": {},
                "message": public_whitelisted_bi_error.message,
            }
        ) == PublicAPIErrorSchema().dump(public_whitelisted_bi_error)
