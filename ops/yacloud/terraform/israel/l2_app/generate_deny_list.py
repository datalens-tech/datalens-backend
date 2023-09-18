#!/usr/bin/env python3

from __future__ import annotations

import json
import subprocess


def read_deny_config() -> dict[str, list]:
    return dict(
        deny_dst_host_list=[
            "us.datalens-front.yandexcloud.co.il",
            "main.back.datalens.yandexcloud.co.il",
        ],
        deny_dst_subnet_list=[
            "127.0.0.1/32",
            "::1/128",
            "169.254.0.0/16",
            "172.16.0.0/24",
            "172.17.0.0/24",
            "172.18.0.0/24",
            "2a11:f740:1:0:9000:45::/112",
            "2a11:f740:1:4000:9000:45::/112",
            "2a11:f740:1:8000:9000:45::/112",
        ],
    )


def process_host_list(host_list: list[str]) -> list[str]:
    result = []
    for host in host_list:
        result.extend(map(lambda x: f"{x}/32", run_cmd("dig", "+short", "A", host).split()))
        result.extend(map(lambda x: f"{x}/128", run_cmd("dig", "+short", "AAAA", host).split()))

    return result


def run_cmd(*command) -> str:
    result = ""
    try:
        result = str(subprocess.check_output(command), "utf-8")
    except subprocess.CalledProcessError:
        pass
    return result.rstrip("\n")


def main() -> dict[str, str]:
    config = read_deny_config()
    deny_ips = []

    deny_ips.extend(process_host_list(config["deny_dst_host_list"]))
    deny_ips.extend(config["deny_dst_subnet_list"])

    return dict(
        ipv4=",".join([ip for ip in deny_ips if ":" not in ip]),
        ipv6=",".join([ip for ip in deny_ips if ":" in ip]),
    )


if __name__ == "__main__":
    result = main()

    result_json = json.dumps(result)
    print(result_json)
