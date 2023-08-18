from bi_constants.api_constants import DLHeaders, DLCookies


def stringify_dl_headers(headers: dict[DLHeaders, str]) -> dict[str, str]:
    return {
        name.value.lower(): val
        for name, val in headers.items()
    }


def stringify_dl_cookies(headers: dict[DLCookies, str]) -> dict[str, str]:
    return {
        name.value: val
        for name, val in headers.items()
    }
