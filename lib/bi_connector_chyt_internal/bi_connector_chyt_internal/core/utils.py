from typing import Optional


def get_chyt_user_auth_headers(
    authorization: Optional[str],
    cookie: Optional[str],
    csrf_token: Optional[str]
) -> dict[str, str]:
    auth_headers = {}
    if authorization is not None:
        auth_headers['Authorization'] = authorization
    if cookie is not None:
        auth_headers['Cookie'] = cookie

    assert auth_headers, "need non-empty context.auth_headers for the conn_line"

    if csrf_token is not None:
        auth_headers['X-CSRF-Token'] = csrf_token

    return auth_headers
