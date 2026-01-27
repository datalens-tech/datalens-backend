import aiohttp.web

import dl_constants


def extract_user_ip(request: aiohttp.web.Request, forwarded_for_proxies_count: int) -> str | None:
    real_ip = request.headers.get(dl_constants.DLHeadersCommon.REAL_IP.value)
    if real_ip is not None:
        return real_ip

    forwarded_for = request.headers.get(dl_constants.DLHeadersCommon.FORWARDED_FOR.value)
    if forwarded_for is not None:
        # forwarded_for is list of all proxies that have forwarded the request
        # usually number of our proxies is known, so we can extract user ip from the end of the list
        # skipping proxies count from the end of the list
        # Example:
        # forwarded_for: "127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4"
        # proxies_count: 2, so last 2 are our proxies, thus user ip is the third from the end
        # user_ip: "127.0.0.2"
        ip_list = [ip.strip() for ip in forwarded_for.split(",")]
        if len(ip_list) > forwarded_for_proxies_count:
            return ip_list[-forwarded_for_proxies_count - 1]

    return request.remote
