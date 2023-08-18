#!/usr/bin/env python3
"""
Modify the secrets configs (normally, in `skm.yaml`) in the current folder (recursively).

Edit the variables below as needed.

See also about redeploying: `./add_env_var.py`

See also: `./skm_update`
"""

from __future__ import annotations

import os
import ruamel.yaml  # parser that can preserve comments


# # Example:
# SECRET_ID = 'sec-01dc4kwx555c9cby0rkmv50eym'
# SECRET_KEY = 'LB_IAM_KEYDATA'
# ENV_KEY = 'LB_IAM_KEYDATA'


ENV_PATH = f'/etc/secrets/{ENV_KEY}'



def process_data(filepath, data):
    if 'secrets' not in data:
        return

    new_secret = dict(
        path=ENV_PATH,
        source=dict(
            yav=dict(
                secret_id=SECRET_ID,
                key=SECRET_KEY,
            ),
        ),
    )

    secrets = data['secrets']
    for secret in secrets:
        if secret.get('path') == ENV_PATH:
            secret.clear()
            secret.update(new_secret)
            return data

    secrets.append(new_secret)
    return data


def main():
    yaml = ruamel.yaml.YAML()
    for dir_name, _, filenames in os.walk('.'):
        for filename in filenames:
            if not filename.endswith('.yaml'):
                continue
            filepath = os.path.join(dir_name, filename)
            with open(filepath) as fobj:
                data = yaml.load(fobj)
            data = process_data(filepath, data)
            if not data:
                continue
            with open(filepath, 'w') as fobj:
                yaml.dump(data, fobj)


if __name__ == '__main__':
    main()
