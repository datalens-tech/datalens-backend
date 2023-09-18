#!/usr/bin/env python

from __future__ import annotations

import sys

import requests


def main():
    token = sys.argv[2]
    dir_name = sys.argv[1]

    try:
        uuid = requests.get(
            "https://united-storage.yandex-team.ru/v1/entriesByKey",
            params={"key": dir_name},
            headers={"Authorization": "OAuth {}".format(token)},
        ).json()["entryUUID"]

        requests.delete(
            "https://united-storage.yandex-team.ru/v1/entries/{}".format(uuid),
            headers={"Authorization": "OAuth {}".format(token)},
        ).raise_for_status()
    except Exception:
        pass

    r = requests.post(
        "https://united-storage.yandex-team.ru/v1/entries",
        json={
            "scope": "folder",
            "key": dir_name,
            "type": "",
        },
        headers={"Authorization": "OAuth {}".format(token)},
    )
    r.raise_for_status()
    dir_uuid = r.json()["entryUUID"]

    for i in range(3):
        r = requests.post(
            "https://united-storage.yandex-team.ru/v1/entries",
            json={"scope": "dataset", "key": "{}/dataset_{}".format(dir_name, i), "type": ""},
            headers={"Authorization": "OAuth {}".format(token)},
        )
        r.raise_for_status()

    r = requests.delete(
        "https://united-storage.yandex-team.ru/v1/entries/{}".format(dir_uuid),
        headers={"Authorization": "OAuth {}".format(token)},
    )
    r.raise_for_status()

    r = requests.post(
        "https://united-storage.yandex-team.ru/v1/entries",
        json={
            "scope": "folder",
            "key": dir_name,
            "type": "",
        },
        headers={"Authorization": "OAuth {}".format(token)},
    )
    r.raise_for_status()

    r = requests.get(
        "https://united-storage.yandex-team.ru/v1/navigation",
        params={"path": dir_name},
        headers={"Authorization": "OAuth {}".format(token)},
    )
    r.raise_for_status()
    print(r.json())


if __name__ == "__main__":
    main()
