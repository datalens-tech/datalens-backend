#!/usr/bin/env python3

from __future__ import annotations

import json
import subprocess

import requests


def read_deny_config() -> dict[str, list]:
    return dict(
        allow_host_list=[
            "rc1b-zbbvgpj1d83x05qq.mdb.cloud-preprod.yandex.net:6379",  # RQE Cache
        ],
        deny_dst_host_list=[],
        deny_dst_macros_list=[
            "_YANDEXNETS_",
        ],
        deny_dst_subnet_list=[
            "127.0.0.1/32",
            "::1/128",
            "169.254.0.0/16",
        ],
    )


def get_hbf_macros(macros_name: str) -> list[str]:
    hbf_hosts = ["vla.hbf.yandex.net", "sas.hbf.yandex.net", "myt.hbf.yandex.net"]
    for host in hbf_hosts:
        try:
            return _get_hbf_macros_from_perdc_balancer(macros_name, hbf_host=host)
        except requests.exceptions.RequestException:
            pass

    raise Exception("All hbf hosts seem to be down")


def _get_hbf_macros_from_perdc_balancer(macros_name: str, hbf_host: str) -> list[str]:
    session = requests.Session()
    session.mount("https://", requests.adapters.HTTPAdapter(max_retries=10))
    r = session.get(
        "https://{hbf_host}/macros/{macros_name}".format(macros_name=macros_name, hbf_host=hbf_host), timeout=10
    )
    r.raise_for_status()
    result = r.json()
    return result


def process_macros_list(macros_list: list[str]) -> list[str]:
    result = []
    for macros in macros_list:
        result.extend(get_hbf_macros(macros))
    return result


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
    deny_ips.extend(process_macros_list(config["deny_dst_macros_list"]))
    deny_ips.extend(config["deny_dst_subnet_list"])

    return dict(
        ipv4_deny=",".join([ip for ip in deny_ips if ":" not in ip]),
        ipv6_deny=",".join([ip for ip in deny_ips if ":" in ip]),
        host_allow=",".join([host for host in config["allow_host_list"]]),
    )


if __name__ == "__main__":
    result = main()

    result_json = json.dumps(result)
    print(result_json)
