import enum
from typing import (
    Iterable,
    Optional,
    overload,
)


class AuthFailureError(Exception):
    internal_message: str
    user_message: Optional[str]
    response_code: Optional[int]

    def __init__(
        self, internal_message: str, *, user_message: Optional[str] = None, response_code: Optional[int] = None
    ) -> None:
        self.internal_message = internal_message
        self.user_message = user_message
        self.response_code = response_code
        super().__init__(internal_message, user_message, response_code)


class BadHeaderPrefixError(AuthFailureError):
    response_code: Optional[int] = 401


# _default_skip_auth_path_list_raw: Tuple[str, ...] = (
#     "/ping",
#     "/unistat",
#     "/metrics",
# )

# default_skip_auth_path_list: Tuple[str, ...] = tuple(
#     itertools.chain(
#         (path.rstrip("/") for path in _default_skip_auth_path_list_raw),
#         (path.rstrip("/") + "/" for path in _default_skip_auth_path_list_raw),
#     )
# )


# def match_path_prefix(prefix_list: Iterable[str], path: str) -> bool:
#     for skip_path_template in prefix_list:
#         if skip_path_template.endswith("*"):
#             return path.startswith(skip_path_template[:-1])
#         if path == skip_path_template:
#             return True
#     return False


class AuthTokenType(enum.Enum):
    bearer = "Bearer"
    oauth = "OAuth"


@overload
def get_token_from_authorization_header(secret_header_value: str, token_type: AuthTokenType) -> str:
    pass


@overload
def get_token_from_authorization_header(secret_header_value: None, token_type: AuthTokenType) -> None:
    pass


def get_token_from_authorization_header(secret_header_value, token_type):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
    if secret_header_value is None:
        return None
    assert isinstance(secret_header_value, str)
    prefix = f"{token_type.value} "

    if secret_header_value.startswith(prefix):
        return secret_header_value.removeprefix(prefix)

    raise BadHeaderPrefixError(
        user_message="Invalid format of Authorization header",
        internal_message=f"Authorization header does not start with {prefix!r}",
    )
