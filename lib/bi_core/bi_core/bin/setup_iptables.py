#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import sys

import requests

DEBUG = False
USER_NAME = "www-data" if len(sys.argv) == 1 else sys.argv[1]
CHAIN_NAME = "check_www_data_user"
TOOLS = ("/sbin/iptables", "/sbin/ip6tables")


def read_config():  # type: ignore  # TODO: fix
    yenv_type = os.environ.get('YENV_TYPE')

    IP4_DNS_HOSTS = tuple(
        # ipv4 dns, `{network}.2`
        # currently, 16,17,18 for prod, 26,27,28 for preprod, but that might
        # change, so erring on the larger-access side for DNS.
        (f"172.{num}.0.2", 53)  # 172.16.0.2 .. 172.31.0.2
        for num in range(16, 32)
    )

    deny_dst_from_env = [
        (subnet, None, f'Deny dst subnet from env {idx}')
        for idx, subnet in
        enumerate(os.environ.get('DL_DENY_DST_SUBNET', '').split(','))
    ]

    if yenv_type in ('production', 'staging'):
        print("Deploying to {env}. Setting up {env} access permissions".format(env=yenv_type.upper()))
        return dict(
            allow_dst_macros_list=(
                ("_CLOUD_MDB_PROD_CLIENT_NETS_", 5432, 6432, 8443, 9440, 27018, 3306),
                # ("_PGAASINTERNALNETS_", 6432, 8443, 9440),
                # ("_CLOUD_MDB_LAB_CLIENT_NETS_", 6432, 8443, 9440),  # df network
                # ("_DL_EXT_BACK_PROD_NETS_",),  # Not in puncher
            ),
            allow_dst_host_list=(
                # ("us.datalens-front.cloud.yandex.net", 443),
                # ("sqs.yandex.net", 8771),
                # ("datalens-back.private-api.ycp.cloud.yandex.net", 443),
                # ("pgaas.mail.yandex.net", 12000),
                # ("upload.datalens.yandex.ru", 443),
                ("2a02:6b8:b010:a0ff::1",),  # IPv4 tunnel
                ("ns64-cache.yandex.net", 53),  # Caching DNS cluster with NAT64
                ('rc1a-h0c0k98f4p8imglf.mdb.yandexcloud.net', 6379),
            ),
            allow_dst_host_list_udp=(
                IP4_DNS_HOSTS +
                (
                    # IPv6 DNS: {network}:2, for cloud-dl-back-prod-nets
                    # https://console.cloud.yandex.ru/folders/b1g77mbejmj4m6flq848/vpc/network/enp36l0d67d913n4gu8g
                    ('2a02:6b8:c02:900:0:f83d:a987:2', 53),
                    ('2a02:6b8:c03:500:0:f83d:a987:2', 53),
                    ('2a02:6b8:c0e:500:0:f83d:a987:2', 53),
                    # IPv6 DNS: {network}:2, for cloud-dl-rqecaches-prod-nets:
                    # https://console.cloud.yandex.ru/folders/b1g77mbejmj4m6flq848/vpc/network/enp3ull07cpis85pcdv8
                    ('2a02:6b8:c02:900:0:f8b4:f4d0:2', 53),
                    ('2a02:6b8:c03:500:0:f8b4:f4d0:2', 53),
                    ('2a02:6b8:c0e:500:0:f8b4:f4d0:2', 53),
                )
            ),
            deny_dst_macros_list=(
                ("_YANDEXNETS_",),
            ),
        )
    elif yenv_type == 'israel':
        print('Deploying to israel')
        return dict(
            deny_dst_subnet_list=deny_dst_from_env + [
                ('172.16.0.0/24', None, 'backend VPC (ipv4)'),
                ('172.17.0.0/24', None, 'backend VPC (ipv4)'),
                ('172.18.0.0/24', None, 'backend VPC (ipv4)'),
                ('2a11:f740:1:0:9000:45::/112', None, 'backend VPC (ipv6)'),
                ('2a11:f740:1:4000:9000:45::/112', None, 'backend VPC (ipv6)'),
                ('2a11:f740:1:8000:9000:45::/112', None, 'backend VPC (ipv6)'),
            ],
            deny_dst_host_list=[
                ('us.datalens-front.yandexcloud.co.il', 80),
                ('us.datalens-front.yandexcloud.co.il', 443),
                ('main.back.datalens.yandexcloud.co.il', 80),
                ('main.back.datalens.yandexcloud.co.il', 443),
            ]
        )
    elif yenv_type == 'nemax':
        print('Deploying to nemax')
        return dict(
            deny_dst_subnet_list=deny_dst_from_env + [
                ('10.0.0.0/16', None, 'backend VPC (ipv4)'),
                ('10.1.0.0/16', None, 'backend VPC (ipv4)'),
                ('10.2.0.0/16', None, 'backend VPC (ipv4)'),
                ('2a13:5947:10:0:9010:45::/112', None, 'backend VPC (ipv6)'),
                ('2a13:5947:11:0:9010:45::/112', None, 'backend VPC (ipv6)'),
                ('2a13:5947:12:0:9010:45::/112', None, 'backend VPC (ipv6)'),
            ],
            deny_dst_host_list=[
                ('us.datalens-front.nemax.nebiuscloud.net', 80),
                ('us.datalens-front.nemax.nebiuscloud.net', 443),
                ('main.back.datalens.nemax.nebiuscloud.net', 80),
                ('main.back.datalens.nemax.nebiuscloud.net', 443),
            ]

        )
    elif yenv_type in ('datacloud', 'datacloud-sec-embeds'):
        print('Deploying to datacloud')

        return dict(
            deny_dst_subnet_list=deny_dst_from_env,
            deny_dst_host_list=[],
        )

    else:
        print("Deploying to DEV OR TESTING. Setting up TESTING access permissings")
        return dict(
            allow_dst_macros_list=(
                # ("_DL_EXT_INFRA_TEST_NETS_",),
                # ("_PGAASINTERNALNETS_", 6432, 8443, 9440),
                # ("_DL_EXT_BACK_TEST_NETS_", 443),
                ("_CLOUD_MDB_LAB_CLIENT_NETS_",),
                # ("_PARTNER_NETS_", 3306),
            ),
            allow_dst_host_list=(
                # ("us.datalens-test.yandex.net", 443),
                # ("sqs.yandex.net", 8771),
                # ("back.datalens-test.yandex.net", 443),
                # ("pgaas.mail.yandex.net", 12000),
                # ("upload.datalens-test.yandex.net", 443),
                ("2a02:6b8:b010:a0ff::1",),  # IPv4 tunnel
                ("ns64-cache.yandex.net", 53,),  # Caching DNS cluster with NAT64
                ('rc1b-zbbvgpj1d83x05qq.mdb.cloud-preprod.yandex.net', 6379),
            ),
            allow_dst_host_list_udp=(
                IP4_DNS_HOSTS +
                (
                    # IPv6 DNS: {network}:2, for cloud-dl-back-preprod-nets:
                    # https://console-preprod.cloud.yandex.ru/folders/aoevv1b69su5144mlro3/vpc/network/c64an5jmjnpgiaclqg8k
                    ('2a02:6b8:c02:901:0:fc1f:1c8c:2', 53),
                    ('2a02:6b8:c03:501:0:fc1f:1c8c:2', 53),
                    ('2a02:6b8:c0e:501:0:fc1f:1c8c:2', 53),
                    # IPv6 DNS: {network}:2, for cloud-dl-rqecaches-preprod-nets:
                    # https://console-preprod.cloud.yandex.ru/folders/aoevv1b69su5144mlro3/vpc/network/c644e7mfpea388720fti
                    ('2a02:6b8:c02:901:0:fc04:78c8:2', 53),
                    ('2a02:6b8:c03:501:0:fc04:78c8:2', 53),
                    ('2a02:6b8:c0e:501:0:fc04:78c8:2', 53),
                )
            ),
            deny_dst_macros_list=(
                ("_YANDEXNETS_",),
            ),
        )


def get_hbf_macros(macros_name):  # type: ignore  # TODO: fix
    hbf_hosts = ['vla.hbf.yandex.net', 'sas.hbf.yandex.net', 'myt.hbf.yandex.net']
    for host in hbf_hosts:
        print(f'Going to request HBF api {host}')
        try:
            return _get_hbf_macros_from_perdc_balancer(macros_name, hbf_host=host)
        except requests.exceptions.RequestException as err:
            print(f'HBF request failed on host {host}: {err}')

    raise Exception('All hbf hosts seem to be down')


def _get_hbf_macros_from_perdc_balancer(macros_name, hbf_host):  # type: ignore  # TODO: fix
    session = requests.Session()
    session.mount("http://", requests.adapters.HTTPAdapter(max_retries=10))
    r = session.get(
        "http://{hbf_host}/macros/{macros_name}".format(macros_name=macros_name, hbf_host=hbf_host),
        params={
            "trypo_format": "dottedquad",
            "format": "json",
        },
        timeout=10
    )
    r.raise_for_status()
    return r.json()


def set_iptables(mask, ports, rule_result, comment):  # type: ignore  # TODO: fix
    tool = TOOLS[0]
    if '::' in mask:
        tool = TOOLS[1]

    if ports:
        for port in ports:
            run_cmd(tool,
                    '-A', CHAIN_NAME,
                    '-d', mask,
                    '-m', 'multiport',
                    '-p', 'tcp',
                    '--port', str(port),
                    '-j', rule_result,
                    '-m', 'comment',
                    '--comment', comment
                    )
    else:
        run_cmd(tool,
                '-A', CHAIN_NAME,
                '-d', mask,
                '-j', rule_result,
                '-m', 'comment',
                '--comment', comment
                )


def process_macros_list(config, result):  # type: ignore  # TODO: fix
    for macroports in config:
        if not isinstance(macroports, tuple):
            raise Exception('Wrong config type for macros')
        print("Setting up {result} for macro {macro}".format(result=result, macro=macroports[0]))
        for mask in get_hbf_macros(macroports[0]):
            set_iptables(mask, macroports[1:], result, 'Macro {}'.format(macroports[0]))


def process_hosts_list(config, rule, proto='tcp'):  # type: ignore  # TODO: fix
    for tool in TOOLS:
        for hostports in config:
            if not isinstance(hostports, tuple):
                raise Exception('Wrong allow_dst_host_list type')
            hostname = hostports[0]
            ports = hostports[1:]
            comment = 'Host {}'.format(hostname)
            if ports:  # Setting up hosts with ports
                for port in ports:
                    run_cmd(tool,
                            '-A', CHAIN_NAME,
                            '-d', hostname,
                            '-m', 'multiport',
                            '-p', proto,
                            '--port', str(port),
                            '-j', rule,
                            '-m', 'comment',
                            '--comment', comment
                            )
            else:
                run_cmd(tool,
                        '-A', CHAIN_NAME,
                        '-d', hostname,
                        '-j', rule,
                        '-m', 'comment',
                        '--comment', comment
                        )


def allow_rqe_to_respond() -> None:
    for tool in TOOLS:
        run_cmd(
            tool,
            '-A', CHAIN_NAME,
            '-m', 'state',
            '--state', 'ESTABLISHED',
            '-j', 'ACCEPT'
        )


def process_subnet_list(config, rule):  # type: ignore  # TODO: fix
    for subnet_config in config:
        mask = subnet_config[0]
        ports = subnet_config[1]
        comment = subnet_config[2]
        set_iptables(mask, ports, rule, f"setup_iptables: {comment}")


def run_cmd(*command):  # type: ignore  # TODO: fix
    print("RUN_CMD: {}".format(" ".join([str(p) for p in command])))
    result = ''
    if not DEBUG:
        try:
            result = str(subprocess.check_output(command), 'utf-8')
            result = result.rstrip('\n')
        except subprocess.CalledProcessError as e:
            print('RUN CMD FAILED, exc: {}, out: {}, err: {}, res: {}'.format(e, e.stdout, e.stderr, result))
            pass
    return result


def main():  # type: ignore  # TODO: fix
    # TODO FIX: BI-2527 setup iptables for DataCloud (restrict access to all VPC CIDRs)
    if os.environ.get('YENV_NAME') not in ("cloud", "datacloud", "datacloud-sec-embeds"):
        print("Looks like not external deployment - don't close any ports")
        exit(0)
    global DEBUG
    DEBUG = os.environ.get('DL_IPTABLES_DEBUG') == '1'

    print("I'm a {}".format(run_cmd('whoami')))
    print("Debug mode is {}".format(DEBUG))
    print("Setting up iptables")

    config = read_config()

    print("Delete existing chain {chain_name} with all rules".format(chain_name=CHAIN_NAME))
    for tool in TOOLS:
        run_cmd(tool,
                '-D', 'OUTPUT',
                '-m', 'owner',
                '--uid-owner', USER_NAME,
                '-j', CHAIN_NAME
                )
        run_cmd(tool,
                '-F', CHAIN_NAME
                )
        run_cmd(tool,
                '--delete-chain', CHAIN_NAME
                )
    print("Create chain {chain_name}".format(chain_name=CHAIN_NAME))
    print("Route requests from {user_name} to chain {chain_name}".format(user_name=USER_NAME, chain_name=CHAIN_NAME))
    for tool in TOOLS:
        run_cmd(tool,
                '--new-chain', CHAIN_NAME
                )
        run_cmd(tool,
                '-A', 'OUTPUT',
                '-m', 'owner',
                '--uid-owner', USER_NAME,
                '-j', CHAIN_NAME
                )

    allow_rqe_to_respond()

    process_hosts_list(config.get('allow_dst_host_list', ()), 'ACCEPT')
    process_hosts_list(config.get('deny_dst_host_list', ()), 'DROP')
    process_hosts_list(config.get('allow_dst_host_list_udp', ()), 'ACCEPT', proto='udp')

    process_macros_list(config.get('allow_dst_macros_list', ()), 'ACCEPT')
    process_macros_list(config.get('deny_dst_macros_list', ()), 'DROP')

    process_subnet_list(config.get('allow_dst_subnet_list', ()), 'ACCEPT')
    process_subnet_list(config.get('deny_dst_subnet_list', ()), 'DROP')

    process_hosts_list(
        (
            ('127.0.0.1', '1:32767'),
            ('::1', '1:32767')
        ),
        'DROP'
    )
    process_subnet_list(
        (
            ('169.254.0.0/16', None, 'BI-4179'),
        ),
        'DROP'
    )

    print("Result iptables")
    for tool in TOOLS:
        print(run_cmd(tool, "-L"))


if __name__ == '__main__':
    main()
