#!/usr/bin/env python3

from __future__ import annotations

import requests
import os

import ruamel.yaml

ABC_URL = 'https://abc-back.yandex-team.ru/api/v4/services/members/?service__slug={}&role__scope__slug={}'
STAFF_URL = 'https://staff-api.yandex-team.ru/v3/persons?login={}&_pretty=1&_fields=login,keys,id&_sort=login'
ABC_OAUTH = os.environ['ABC_OAUTH']
ABC_SERVICE = os.environ['ABC_SERVICE']
ABC_SCOPE = os.environ.get('ABC_SCOPE', 'devops')
YAPI_HEADERS = {'Authorization': 'OAuth {}'.format(ABC_OAUTH)}


def get_ssh_keys_string() -> str:
    resp = requests.get(ABC_URL.format(ABC_SERVICE, ABC_SCOPE), headers=YAPI_HEADERS)
    resp.raise_for_status()
    keys_by_user = {}
    all_persons = get_abc_service_members(ABC_SERVICE)

    resp = requests.get(STAFF_URL.format(','.join(all_persons)), headers=YAPI_HEADERS)
    resp.raise_for_status()
    for result in resp.json()['result']:
        login = result['login']
        if login not in keys_by_user:
            keys_by_user[login] = []
        keys = result['keys']
        for k in keys:
            key = k['key']
            keys_by_user[login].append(key)

    keys_metadata = []
    for login in sorted(keys_by_user):
        for key in sorted(keys_by_user[login]):
            keys_metadata.append("{}:{}".format(login, key))
    keys_metadata = "\n".join(keys_metadata)

    return keys_metadata


def get_abc_service_members(service_slug):
    def abc_api(url):
        resp = requests.get(url, headers=YAPI_HEADERS)
        resp.raise_for_status()
        return resp

    def members_from_response(resp):
        members = []
        for item in resp.json()['results']:
            members.append(item['person']['login'])
        return members

    service_logins = []
    init_abc_url = ABC_URL.format(service_slug, ABC_SCOPE)
    resp = abc_api(init_abc_url)
    service_logins.extend(members_from_response(resp))
    while resp.json()['next']:
        resp = abc_api(resp.json()['next'])
        service_logins.extend(members_from_response(resp))
    return sorted(set(service_logins))


def main():
    ryaml = ruamel.yaml.YAML()
    for dir_name, _, filenames in os.walk('.'):
        for filename in filenames:
            if filename != 'ig-config.yaml':
                continue
            filepath = os.path.join(dir_name, filename)
            with open(filepath) as fobj:
                data = ryaml.load(fobj)
            data['instance_template']['metadata']['ssh-keys'] = get_ssh_keys_string()
            with open(filepath, 'w') as fobj:
                ryaml.dump(data, fobj)


if __name__ == '__main__':
    main()
